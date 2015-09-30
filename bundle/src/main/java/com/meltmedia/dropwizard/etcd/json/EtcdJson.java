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

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mousio.etcd4j.EtcdClient;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A factory for mapped interfaces to Etcd.
 * 
 * @author Christian Trimble
 */
public class EtcdJson {
  private static Logger logger = LoggerFactory.getLogger(EtcdJson.class);

  public static class Builder {
    private ObjectMapper mapper;
    private Supplier<EtcdClient> clientSupplier;
    private String baseDirectory = "";
    private ScheduledExecutorService executor;
    private MetricRegistry registry;

    public Builder withMapper(ObjectMapper mapper) {
      this.mapper = mapper;
      return this;
    }

    public Builder withClient(Supplier<EtcdClient> clientSupplier) {
      this.clientSupplier = clientSupplier;
      return this;
    }

    public Builder withBaseDirectory(String baseDirectory) {
      this.baseDirectory = baseDirectory;
      return this;
    }

    public Builder withExecutor(ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }
    
    public Builder withMetricRegistry( MetricRegistry registry ) {
      this.registry = registry;
      return this;
    }

    public EtcdJson build() {
      return new EtcdJson(clientSupplier, executor, mapper, baseDirectory, registry);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private Supplier<EtcdClient> clientSupplier;
  private ObjectMapper mapper;
  private String baseDirectory;
  private WatchService watchService;
  private ScheduledExecutorService executor;

  public EtcdJson(Supplier<EtcdClient> clientSupplier, ScheduledExecutorService executor,
    ObjectMapper mapper, String baseDirectory, MetricRegistry registry) {
    this.clientSupplier = clientSupplier;
    this.executor = executor;
    this.mapper = mapper;
    this.baseDirectory = baseDirectory;
    this.watchService =
      WatchService.builder().withEtcdClient(clientSupplier).withExecutor(executor)
        .withMapper(mapper).withDirectory(baseDirectory).withMetricRegistry(registry).build();
  }

  public void start() {
    logger.info("starting EtcdFactory.");
    this.watchService.start();
    logger.info("started EtcdFactory.");
  }

  public void stop() {
    logger.info("stopping EtcdFactory");
    this.watchService.stop();
    logger.info("Stopped EtcdFactory");
  }

  public WatchService getWatchService() {
    return watchService;
  }

  public <T> Heartbeat<T> newHeartbeat(String key, T value, int ttl) {
    return Heartbeat.<T> builder().withClient(clientSupplier).withExecutor(executor)
      .withKey(baseDirectory + key).withMapper(mapper).withValue(value).withTtl(ttl).build();
  }

  public <T> MappedEtcdDirectory<T> newDirectory(String directory, TypeReference<T> type) {
    return new MappedEtcdDirectory<T>(directory, type);
  }

  public class MappedEtcdDirectory<T> {

    private String directory;
    private TypeReference<T> type;

    protected MappedEtcdDirectory(String directory, TypeReference<T> type) {
      this.directory = directory;
      this.type = type;
    }

    public WatchService.Watch registerWatch(EtcdEventHandler<T> handler) {
      return watchService.registerDirectoryWatch(directory, type, handler);
    }

    public WatchService.Watch registerValueWatch(String key, EtcdEventHandler<T> handler) {
      return watchService.registerValueWatch(directory, key, type, handler);
    }

    public Heartbeat<T> newHeartbeat(String key, T value, int ttl) {
      return Heartbeat.<T> builder().withClient(clientSupplier).withExecutor(executor)
        .withKey(baseDirectory + directory + key).withMapper(mapper).withValue(value).withTtl(ttl)
        .build();
    }

    public <U> MappedEtcdDirectory<U> newDirectory(String directory, TypeReference<U> type) {
      return new MappedEtcdDirectory<U>(this.directory + directory, type);
    }

    public EtcdDirectoryDao<T> newDao() {
      return new EtcdDirectoryDao<T>(clientSupplier, baseDirectory + directory, mapper, type);
    }
  }

  public ObjectMapper getMapper() {
    return mapper;
  }
}
