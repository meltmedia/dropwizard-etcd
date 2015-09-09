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

import static java.lang.Math.min;
import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.responses.EtcdException;
import mousio.etcd4j.responses.EtcdKeysResponse;
import mousio.etcd4j.responses.EtcdKeysResponse.EtcdNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

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

    public WatchService build() {
      return new WatchService(client, directory, mapper, executor);
    }
  }

  private Supplier<EtcdClient> client;
  private ObjectMapper mapper;
  private String directory;
  private ScheduledExecutorService executor;

  private ScheduledFuture<?> watchFuture;
  private AtomicLong watchIndex = new AtomicLong();
  private List<Watch> watchers = Lists.newCopyOnWriteArrayList();

  protected WatchService(Supplier<EtcdClient> client, String directory, ObjectMapper mapper,
    ScheduledExecutorService executor) {
    this.client = client;
    this.directory = directory;
    this.mapper = mapper;
    this.executor = executor;
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

  ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  Lock read = stateLock.readLock();
  Lock write = stateLock.writeLock();

  protected <T> T read(Callable<T> callable) throws Exception {
    read.lock();
    try {
      return callable.call();
    } finally {
      read.unlock();
    }
  }

  protected <E extends Exception> void read(RunnableWithException<E> r, Class<E> e) throws E {
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
  }

  protected void read(Runnable r) {
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
  }

  protected <E extends Exception> void write(RunnableWithException<E> r, Class<E> e) throws E {
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
  }

  protected void write(Runnable r) {
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
  }

  protected <T> T write(Callable<T> callable) throws Exception {
    read.lock();
    try {
      return callable.call();
    } finally {
      read.unlock();
    }
  }

  public interface RunnableWithException<E extends Exception> {
    public void run() throws E;
  }

  protected long getWatchIndex() {
    read.lock();
    try {
      return watchIndex.get();
    } finally {
      read.unlock();
    }
  }

  protected void startWatchingNodes() {
    logger.debug("starting watch thread.");
    watchFuture =
      executor.scheduleWithFixedDelay(() -> {
        logger.debug(format("watch returned for %s", watchIndex.get()));

        try {

          long nextIndex = write((Callable<Long>) () -> watchIndex.incrementAndGet());
          // get the next event.
        EtcdKeysResponse response =
          client.get().getDir(directory).recursive()
            .waitForChange(read((Callable<Long>) () -> watchIndex.get()))
            .timeout(30, TimeUnit.SECONDS).send().get();

        logger.debug("received update for {} at {}", key(response), response.etcdIndex);

        read(() -> {
          if (nextIndex == watchIndex.get()) {
            watchers.forEach(w -> w.accept(response));
          }
        });
      } catch (InterruptedException ie) {
        logger.info("etcd watch interrupted");
      } catch (Exception e) {
        logger.warn(format("exception thrown while watching %s", directory), e);
      }
    }, 1, 1, TimeUnit.MILLISECONDS);
  }

  protected void addWatch(Watch watch) {
    write(() -> {
      logger.debug("Adding watch.");
      watch.init();
      watchers.add(watch);
      watchIndex.set(min(watchIndex.get(), watch.currentIndex()));
    });
  }

  protected void removeWatch(Watch watch) {
    write(() -> {
      watchers.remove(watch);
    });
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
    if (value == null && prevValue == null)
      return Optional.empty();
    switch (response.action) {
      case delete:
        return Optional.of(EtcdEvent.<T> builder().withType(EtcdEvent.Type.removed)
          .withValue(value).withPrevValue(prevValue)
          .withKey(removeDirectory(directory, response.prevNode.key)).build());
      case expire:
        return Optional.of(EtcdEvent.<T> builder().withType(EtcdEvent.Type.removed)
          .withValue(value).withPrevValue(prevValue)
          .withKey(removeDirectory(directory, response.prevNode.key)).build());
      case set:
        if (filterValueChange(value, prevValue)) {
          return Optional.of(EtcdEvent.<T> builder()
            .withType(prevValue == null ? EtcdEvent.Type.added : EtcdEvent.Type.updated)
            .withValue(value).withPrevValue(prevValue)
            .withKey(removeDirectory(directory, response.node.key)).build());
        }
        break;
      case update:
        if (!value.equals(prevValue)) {
          return Optional.of(EtcdEvent.<T> builder().withType(EtcdEvent.Type.updated)
            .withValue(value).withPrevValue(prevValue)
            .withKey(removeDirectory(directory, response.node.key)).build());
        }
        break;
      case compareAndSwap:
        if (!value.equals(prevValue)) {
          return Optional.of(EtcdEvent.<T> builder().withType(EtcdEvent.Type.updated)
            .withValue(value).withPrevValue(prevValue)
            .withKey(removeDirectory(directory, response.node.key)).build());
        }
      default:
        break;
    }
    return Optional.empty();
  }

  public interface Watch {
    public void accept(EtcdKeysResponse response);

    public void init();

    public Long currentIndex();

    public void stop();
  }

  public class DirectoryWatch<T> implements Watch {
    TypeReference<T> type;
    EtcdEventHandler<T> handler;
    String directory;
    volatile Long currentIndex;
    Pattern keyMatcher;

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

    public void init() {
      try {
        EtcdKeysResponse dirList = client.get().getDir(directory).dir().send().get();

        currentIndex = dirList.etcdIndex;

        if (dirList.node.nodes != null) {
          dirList.node.nodes
            .stream()
            .filter(n -> !n.dir)
            .forEach(
              node -> {
                logger.debug("reading value for " + node.key);
                T value = readValue(mapper, node, type);

                if (value != null) {
                  fireEvent(handler, EtcdEvent.<T> builder().withType(EtcdEvent.Type.added)
                    .withValue(value).withKey(removeDirectory(directory, node.key)).build());
                } else {
                  logger.debug("No value to read for {}!", node.key);
                }
              });
        }
      } catch (EtcdException ee) {
        logger.debug("exception during sync", ee);
        currentIndex = Long.valueOf((long) ee.index.intValue());
      } catch (IOException | TimeoutException e) {
        logger.error(format("faled to start watch for directory %s", directory), e);
      }
    }

    public void accept(EtcdKeysResponse response) {
      if (!keyMatcher.matcher(key(response)).matches()) {
        return;
      }

      Long responseIndex = indexOf(response);
      if (currentIndex < responseIndex) {
        asEvent(mapper, directory, response, type).ifPresent(handler::handle);
        currentIndex = responseIndex;
      } else {
        logger.debug("filtering event for {}", key(response));
        logger.debug("reponses index {} current index {}", responseIndex, currentIndex);
      }
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
    volatile Long currentIndex;

    protected ValueWatch(String directory, String key, TypeReference<T> type,
      EtcdEventHandler<T> handler) {
      this.directory = directory;
      this.key = key;
      this.type = type;
      this.handler = handler;
    }

    @Override
    public void accept(EtcdKeysResponse response) {
      Long responseIndex = indexOf(response);
      if (currentIndex < responseIndex && key(response).equals(directory + "/" + key)) {
        asEvent(mapper, directory, response, type).ifPresent(handler::handle);
        currentIndex = responseIndex;
      }
    }

    @Override
    public void init() {
      try {
        EtcdKeysResponse response = client.get().get(directory + "/" + key).send().get();

        currentIndex = response.etcdIndex;

        if (!response.node.dir) {
          T value = readValue(mapper, response.node, type);

          if (value != null) {
            fireEvent(handler,
              EtcdEvent.<T> builder().withType(EtcdEvent.Type.added).withValue(value).withKey(key)
                .build());
          }
        }
      } catch (EtcdException ee) {
        currentIndex = Long.valueOf((long) ee.index.intValue());
      } catch (IOException | TimeoutException e) {
        logger.error(format("faled to start watch for directory %s", directory), e);
      }
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
      EtcdKeysResponse response = client.get().putDir(directory).isDir().send().get();

      watchIndex.set(response.etcdIndex);
    } catch (EtcdException ee) {
      watchIndex.set((long) ee.index);
      logger.debug(format("failed to create directory %s", directory), ee);
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
