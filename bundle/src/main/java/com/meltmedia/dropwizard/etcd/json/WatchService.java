/**
 * Copyright (C) 2015 meltmedia (christian.trimble@meltmedia.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.meltmedia.dropwizard.etcd.json;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.responses.EtcdAuthenticationException;
import mousio.etcd4j.responses.EtcdException;
import mousio.etcd4j.responses.EtcdKeysResponse;
import mousio.etcd4j.responses.EtcdKeysResponse.EtcdNode;

/**
 * A service for watching changes on Etcd with JSON content stored in the values.
 * 
 * @author Christian Trimble
 *
 */
public class WatchService {
  public static final Logger logger = LoggerFactory.getLogger(WatchService.class);

  public static class Builder {
    private Supplier<EtcdClient> client;
    private String directory;
    private ScheduledExecutorService executor;
    private ObjectMapper mapper;
    private MetricRegistry registry;
    private long timeout = 30L;
    private TimeUnit timeoutUnit = TimeUnit.SECONDS;

    public Builder withEtcdClient(Supplier<EtcdClient> client) {
      this.client = client;
      return this;
    }

    public Builder withDirectory(String directory) {
      this.directory = directory;
      return this;
    }

    public Builder withExecutor(ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    public Builder withMapper(ObjectMapper mapper) {
      this.mapper = mapper;
      return this;
    }
    
    public Builder withMetricRegistry( MetricRegistry registry ) {
      this.registry = registry;
      return this;
    }

    public Builder withWatchTimeout( long timeout, TimeUnit timeoutUnit ) {
      this.timeout = timeout;
      this.timeoutUnit = timeoutUnit;
      return this;
    }

    public WatchService build() {
      if( registry == null ) {
        throw new IllegalStateException("metric registry is required");
      }
      if( timeout <= 0L ) {
        throw new IllegalStateException("timeout must be positive");
      }
      if( timeoutUnit == null ) {
        throw new IllegalStateException("timeout unit is required");
      }
      return new WatchService(client, directory, mapper, executor, registry, timeout, timeoutUnit);
    }
  }
  
  public static String ETCD_INDEX = "etcdIndex";
  public static String SYNC_INDEX = "syncIndex";
  public static String INDEX_DELTA = "indexDelta";
  public static String WATCH_EVENTS = "watchEvents";
  public static String RESYNC_EVENTS = "resyncEvents";

  private Supplier<EtcdClient> client;
  private ObjectMapper mapper;
  private String directory;
  private ScheduledExecutorService executor;
  private long timeout;
  private TimeUnit timeoutUnit;

  private ScheduledFuture<?> watchFuture;
  private AtomicLong syncIndex = new AtomicLong(0); // the index to which we are synchronized.
  private AtomicLong etcdIndex = new AtomicLong(0); // the last etcd index we have seen.
  private AtomicLong indexDelta = new AtomicLong(0);
  private List<Watch> watchers = Lists.newCopyOnWriteArrayList();
  private FunctionalLock lock = new FunctionalLock();
  private Meter watchEvents;
  private Meter resyncEvents;

  protected WatchService(Supplier<EtcdClient> client, String directory, ObjectMapper mapper,
    ScheduledExecutorService executor, MetricRegistry registry, long timeout, TimeUnit timeoutUnit) {
    this.client = client;
    this.directory = directory;
    this.mapper = mapper;
    this.executor = executor;
    this.timeout = timeout;
    this.timeoutUnit = timeoutUnit;
    registry.register(MetricRegistry.name(WatchService.class, ETCD_INDEX), (Gauge<Long>)()->etcdIndex.get());
    registry.register(MetricRegistry.name(WatchService.class, SYNC_INDEX), (Gauge<Long>)()->syncIndex.get());
    registry.register(MetricRegistry.name(WatchService.class, INDEX_DELTA), (Gauge<Long>)()->indexDelta.get());
    watchEvents = registry.meter(MetricRegistry.name(WatchService.class, WATCH_EVENTS));
    resyncEvents = registry.meter(MetricRegistry.name(WatchService.class, RESYNC_EVENTS));
  }

  public <T> Watch registerDirectoryWatch(String directory, TypeReference<T> type,
    EtcdEventHandler<T> handler) {
    DirectoryWatch<T> watch = new DirectoryWatch<T>(this.directory + directory, type, handler);
    addWatch(watch);
    return watch;
  }

  public <T> Watch registerValueWatch(String directory, String key, TypeReference<T> type,
    EtcdEventHandler<T> handler) {
    ValueWatch<T> watch = new ValueWatch<T>(this.directory + directory, key, type, handler);
    addWatch(watch);
    return watch;
  }

  public void start() {
    logger.debug("starting watch for {}", directory);
    ensureDirectoryExists();
    startWatchingNodes();
    logger.debug("started watch for {}", directory);
  }

  public void stop() {
    logger.debug("stopping watch for {}", directory);
    stopWatchingNodes();
    logger.debug("stopped watch for {}", directory);
  }
  
  public List<Watch> outOfSyncWatchers() {
    return lock
      .readSupplier(()->watchers.stream().filter(w->!w.inSync())
      .collect(Collectors.toList()))
      .get();
  }

  protected long getWatchIndex() {
    lock.read.lock();
    try {
      return syncIndex.get();
    } finally {
      lock.read.unlock();
    }
  }

  protected void startWatchingNodes() {
    logger.debug("starting watch thread.");
    watchFuture =
      executor.scheduleWithFixedDelay(() -> {
        logger.debug(format("watch returned for %s", syncIndex.get()));

        long currIndex = lock.readSupplier(syncIndex::get).get();
        try {
          // get the next event.
          EtcdKeysResponse response =
            client.get().getDir(directory)
              .recursive()
              .waitForChange(currIndex+1)
              .timeout(timeout, timeoutUnit)
              .send()
              .get();
          
          watchEvents.mark();

        lock.writeRunnable(() -> {
          etcdIndex.set(response.etcdIndex);
          if (syncIndex.compareAndSet(currIndex, response.node.modifiedIndex)) {
            indexDelta.set(etcdIndex.get()-syncIndex.get());
            watchers.forEach(w -> w.accept(response));
          }
        }).run();
      } catch( EtcdException etcdException ) {
        // we don't know what the state is, resync.
        resyncEvents.mark();
        ensureDirectoryExists();
        lock.writeRunnable(()->{
          etcdIndex.set(etcdException.index);
          syncIndex.set(watchers.stream()
            .mapToLong(w->w.sync().currentIndex())
            .min()
            .orElse(etcdException.index));
          indexDelta.set(etcdIndex.get()-syncIndex.get());
        }).run();
      } catch (TimeoutException e) {
        logger.debug("etcd watch timed out");
      } catch (Exception e) {
        logger.warn(format("exception thrown while watching %s", directory), e);
      }
    }, 1, 1, TimeUnit.MILLISECONDS);
  }

  protected void addWatch(Watch watch) {
    lock.writeRunnable(() -> {
      watch.sync();
      watchers.add(watch);
      syncIndex.set(watchers.stream().mapToLong(Watch::currentIndex).min().orElse(0L));
      indexDelta.set(etcdIndex.get()-syncIndex.get());
    }).run();
  }

  protected void removeWatch(Watch watch) {
    lock.writeRunnable(() -> {
      watchers.remove(watch);
    }).run();
  }

  protected void stopWatchingNodes() {
    if (watchFuture != null) {
      watchFuture.cancel(true);
    }
  }

  static <T> Optional<EtcdEvent<T>> asEvent(ObjectMapper mapper, String directory,
    EtcdKeysResponse response, TypeReference<T> type) {
    T value = readValue(mapper, response.node, type);
    T prevValue = readValue(mapper, response.prevNode, type);
    long etcdIndex = response.node.modifiedIndex;
    if (value == null && prevValue == null)
      return Optional.empty();
    switch (response.action) {
      case delete:
        return Optional.of(EtcdEvent.<T> builder().withType(EtcdEvent.Type.removed)
          .withIndex(etcdIndex)
          .withValue(value).withPrevValue(prevValue)
          .withKey(removeDirectory(directory, response.prevNode.key))
          .build());
      case expire:
        return Optional.of(EtcdEvent.<T> builder().withType(EtcdEvent.Type.removed)
          .withIndex(etcdIndex)
          .withValue(value).withPrevValue(prevValue)
          .withKey(removeDirectory(directory, response.prevNode.key))
          .build());
      case set:
        if (filterValueChange(value, prevValue)) {
          return Optional.of(EtcdEvent.<T> builder()
            .withType(prevValue == null ? EtcdEvent.Type.added : EtcdEvent.Type.updated)
            .withIndex(etcdIndex)
            .withValue(value).withPrevValue(prevValue)
            .withKey(removeDirectory(directory, response.node.key))
            .build());
        }
        break;
      case update:
        if (!value.equals(prevValue)) {
          return Optional.of(EtcdEvent.<T> builder().withType(EtcdEvent.Type.updated)
            .withIndex(etcdIndex)
            .withValue(value).withPrevValue(prevValue)
            .withKey(removeDirectory(directory, response.node.key))
            .build());
        }
        break;
      case compareAndSwap:
        if (!value.equals(prevValue)) {
          return Optional.of(EtcdEvent.<T> builder().withType(EtcdEvent.Type.updated)
            .withIndex(etcdIndex)
            .withValue(value).withPrevValue(prevValue)
            .withKey(removeDirectory(directory, response.node.key))
            .build());
        }
      default:
        break;
    }
    return Optional.empty();
  }

  public interface Watch {
    public Watch accept(EtcdKeysResponse response);

    public Watch sync();

    public Long currentIndex();

    public void stop();
    
    public boolean inSync();
  }
  
  protected static class EtcdValue<T> {
    T value;
    long modifiedIndex;
    long loadIndex;
    
    public EtcdValue( T value, long modifiedIndex, long loadIndex ) {
      this.value = value;
      this.modifiedIndex = modifiedIndex;
      this.loadIndex = loadIndex;
    }
  }

  public class DirectoryWatch<T> implements Watch {
    TypeReference<T> type;
    EtcdEventHandler<T> handler;
    String directory;
    volatile Long currentIndex = Long.valueOf(0L);
    volatile boolean inSync = false;
    Pattern keyMatcher;
    Map<String, EtcdValue<T>> state = Maps.newHashMap();

    private DirectoryWatch(String directory, TypeReference<T> type, EtcdEventHandler<T> handler) {
      this.directory = directory;
      this.type = type;
      this.handler = handler;
      this.keyMatcher = Pattern.compile("\\A" + Pattern.quote(directory) + "/[^/]+\\Z");
    }

    public String toString() {
      return String.format("Directory watch of %s", directory);
    }

    public Long currentIndex() {
      return currentIndex;
    }
    
    public boolean inSync() {
      return inSync;
    }

    public Watch sync() {
      inSync = false;
      try {
        EtcdKeysResponse dirList = client.get().getDir(directory).dir().send().get();

        currentIndex = dirList.etcdIndex;

        if (dirList.node.nodes != null) {
          dirList.node.nodes
            .stream()
            .filter(n -> !n.dir)
            .forEach(
              node -> {
                  state.compute(removeDirectory(directory, node.key), (key, entry)->{
                    T value = readValue(mapper, node, type);
                    if(entry == null && value == null) return null;
                    
                    else if(entry == null) {
                      fireEvent(handler, EtcdEvent.<T> builder()
                        .withType(EtcdEvent.Type.added)
                        .withIndex(node.modifiedIndex)
                        .withValue(value)
                        .withKey(key)
                        .build());
                      return new EtcdValue<T>(value, node.modifiedIndex, currentIndex);
                    }
                    else if(value == null) {
                      fireEvent(handler, EtcdEvent.<T> builder()
                        .withType(EtcdEvent.Type.removed)
                        .withIndex(node.modifiedIndex)
                        .withPrevValue(entry.value)
                        .withKey(key)
                        .build());
                      return null;                     
                    }
                    else if( entry.modifiedIndex == node.modifiedIndex || entry.equals(value) ) {
                      entry.loadIndex = currentIndex;
                      return entry;
                    }
                    else {
                      fireEvent(handler, EtcdEvent.<T> builder()
                        .withType(EtcdEvent.Type.updated)
                        .withIndex(node.modifiedIndex)
                        .withValue(value)
                        .withPrevValue(entry.value)
                        .withKey(key)
                        .build());
                      return new EtcdValue<T>(value, node.modifiedIndex, currentIndex);
                    }
                  });
              });
          }
          state.replaceAll((key, entry)->{
            if( entry.loadIndex == currentIndex ) return entry;
            
            fireEvent(handler, EtcdEvent.<T> builder()
              .withType(EtcdEvent.Type.removed)
              .withIndex(currentIndex)
              .withPrevValue(entry.value)
              .withKey(key)
              .build());
            return null;             
          });
          inSync = true;
      } catch (EtcdException ee) {
        currentIndex = Long.valueOf((long) ee.index.intValue());
        if( ee.errorCode == 100 ) {
          state.replaceAll((key, entry)->{
            fireEvent(handler, EtcdEvent.<T> builder()
              .withType(EtcdEvent.Type.removed)
              .withIndex(currentIndex)
              .withPrevValue(entry.value)
              .withKey(key)
              .build());
            return null;             
          });
          inSync = true;
        }
        else {
          logger.debug("exception during sync", ee);
        }
      } catch (IOException | TimeoutException | EtcdAuthenticationException e) {
        logger.error(format("failed to start watch for directory %s", directory), e);
      }
      return this;
    }

    public Watch accept(EtcdKeysResponse response) {
      if (!keyMatcher.matcher(key(response)).matches()) {
        return this;
      }

      Long responseIndex = indexOf(response);
      if (currentIndex < responseIndex) {
        asEvent(mapper, directory, response, type).ifPresent(event->{
          state.compute(event.getKey(), (key, entry)->{
            switch( event.getType() ) {
              case added:
                fireEvent(handler, event);
                return new EtcdValue<T>(event.getValue(), response.node.modifiedIndex, response.etcdIndex);
              case updated:
                fireEvent(handler, event);
                return new EtcdValue<T>(event.getValue(), response.node.modifiedIndex, response.etcdIndex);
              case removed:
                fireEvent(handler, event);
                return null;
              default:
                throw new IllegalStateException("unknown event type "+event.getType().name());
            }
          });
        });
        currentIndex = responseIndex;
      } else {
        logger.debug("filtering event for {}", key(response));
        logger.debug("response index {} current index {}", responseIndex, currentIndex);
      }
      return this;
    }

    public void stop() {
      removeWatch(this);
    }
  }

  public class ValueWatch<T> implements Watch {
    TypeReference<T> type;
    EtcdEventHandler<T> handler;
    String directory;
    String key;
    volatile boolean inSync = false;
    volatile Long currentIndex = Long.valueOf(0L);
    volatile EtcdValue<T> currentValue = null;

    protected ValueWatch(String directory, String key, TypeReference<T> type,
      EtcdEventHandler<T> handler) {
      this.directory = directory;
      this.key = key;
      this.type = type;
      this.handler = handler;
    }

    @Override
    public Watch accept(EtcdKeysResponse response) {
      Long responseIndex = indexOf(response);
      if (currentIndex < responseIndex && key(response).equals(directory + "/" + key)) {
        asEvent(mapper, directory, response, type).ifPresent(handler::handle);
        currentIndex = responseIndex;
      }
      return this;
    }
    
    public boolean inSync() {
      return inSync;
    }

    @Override
    public Watch sync() {
      inSync = false;
      try {
        EtcdKeysResponse response = client.get().get(directory + "/" + key).send().get();

        currentIndex = response.etcdIndex;

        if (!response.node.dir) {
          T value = readValue(mapper, response.node, type);

          if (value != null && currentValue == null) {
            fireEvent(handler,
              EtcdEvent.<T> builder().withType(EtcdEvent.Type.added).withValue(value).withKey(key).withIndex(response.node.modifiedIndex)
                .build());
            currentValue = new EtcdValue<T>(value, response.node.modifiedIndex, currentIndex);
          }
          else if( value == null && currentValue != null) {
            fireEvent(handler, EtcdEvent.<T>builder()
              .withType(EtcdEvent.Type.removed)
              .withPrevValue(currentValue.value)
              .withIndex(response.node.modifiedIndex)
              .withKey(key)
              .build());
             currentValue = null;
          }
          else if( value == null && currentValue == null ) {}
          else if( response.node.modifiedIndex == currentValue.modifiedIndex || currentValue.value.equals(value) ) {
            currentValue = new EtcdValue<T>(currentValue.value, currentValue.modifiedIndex, currentIndex);
          }
          inSync = true;
        }
      } catch (EtcdException ee) {
        currentIndex = ee.index;
      } catch (IOException | TimeoutException | EtcdAuthenticationException e) {
        logger.error(format("faled to start watch for directory %s", directory), e);
      }
      return this;
    }

    @Override
    public Long currentIndex() {
      return currentIndex;
    }

    public void stop() {
      removeWatch(this);
    }
  }

  protected void ensureDirectoryExists() {
    try {
      logger.debug("attempting to put directory {}", directory);
      EtcdKeysResponse response = client.get().putDir(directory).prevExist(false).send().get();
      logger.debug("put directory {} successful", directory);

      lock.writeRunnable(()->{
        syncIndex.set(response.node.modifiedIndex);
        etcdIndex.set(response.node.modifiedIndex);
      });
      logger.debug("directory {} index recorded", directory);
    } catch (EtcdException ee) {
      logger.debug("put directory {} failed, it exists", directory);
      lock.writeRunnable(()->{
        syncIndex.set(ee.index);
        etcdIndex.set(ee.index);
      });
      logger.debug("directory {} index recorded", directory);
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("could not create directory %s", directory), e);
    }
  }

  static String key(EtcdKeysResponse response) {
    String key =
      response.node != null ? response.node.key : response.prevNode != null ? response.prevNode.key
        : null;

    if (key == null)
      throw new IllegalStateException("no key defined.");

    return key;
  }

  static <T> T readValue(ObjectMapper mapper, EtcdNode value, TypeReference<T> type) {
    try {
      return value == null || value.value == null || value.dir ? null : mapper.readValue(
        value.value, type);
    } catch (Exception e) {
      logger.warn(format("could not read value at %s as type %s", value.key, type), e);
      return null;
    }
  }

  static <T> void fireEvent(EtcdEventHandler<T> handler, EtcdEvent<T> event) {
    try {
      handler.handle(event);
    } catch (Throwable t) {
      logger.error(format("exception thrown handling event for %s", event.getKey()), t);
    }
  }

  static String removeDirectory(String directory, String key) {
    return key.substring(directory.length() + 1, key.length());
  }

  static <T> boolean filterValueChange(T value, T prevValue) {
    return value == null || prevValue == null || !value.equals(prevValue);
  }

  static Long indexOf(EtcdKeysResponse response) {
    return response.node != null ? response.node.modifiedIndex : response.prevNode != null
      ? response.prevNode.modifiedIndex : response.etcdIndex;
  }

  public static Builder builder() {
    return new Builder();
  }
}
