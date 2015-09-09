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
package com.meltmedia.dropwizard.etcd.cluster;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.WatchService;

/**
 * Clusters processors using an Etcd directory to store process configuration.
 * 
 * @author Christian Trimble
 */
public class ClusterProcessor<C> {
  private static Logger logger = LoggerFactory.getLogger(ClusterProcessor.class);

  public static class Builder<C> {
    private String nodeId;
    private ObjectMapper mapper;
    private MappedEtcdDirectory<ClusterProcess> directory;
    private Function<C, ClusterProcessLifecycle> lifecycleFactory;
    private TypeReference<C> type;

    public ClusterProcessor.Builder<C> withNodeId(String nodeId) {
      this.nodeId = nodeId;
      return this;
    }

    public ClusterProcessor.Builder<C> withMapper(ObjectMapper mapper) {
      this.mapper = mapper;
      return this;
    }

    public ClusterProcessor.Builder<C> withDirectory(MappedEtcdDirectory<ClusterProcess> directory) {
      this.directory = directory;
      return this;
    }

    public ClusterProcessor.Builder<C> withLifecycleFactory(
      Function<C, ClusterProcessLifecycle> lifecycleFactory) {
      this.lifecycleFactory = lifecycleFactory;
      return this;
    }

    public ClusterProcessor.Builder<C> withType(TypeReference<C> type) {
      this.type = type;
      return this;
    }

    public ClusterProcessor<C> build() {
      ClusterProcessor<C> service =
        new ClusterProcessor<C>(directory, mapper, nodeId, type, lifecycleFactory);

      return service;
    }
  }

  public static <C> ClusterProcessor.Builder<C> builder() {
    return new ClusterProcessor.Builder<C>();
  }

  private ConcurrentMap<String, ClusterProcessLifecycle> lifecycles = Maps.newConcurrentMap();
  private MappedEtcdDirectory<ClusterProcess> directory;
  private String nodeId;
  private Function<C, ClusterProcessLifecycle> lifecycleFactory;
  private ObjectMapper mapper;
  private TypeReference<C> type;
  private WatchService.Watch watch;

  public ClusterProcessor(MappedEtcdDirectory<ClusterProcess> directory, ObjectMapper mapper,
    String nodeId, TypeReference<C> type, Function<C, ClusterProcessLifecycle> lifecycleFactory) {
    this.directory = directory;
    this.mapper = mapper;
    this.nodeId = nodeId;
    this.type = type;
    this.lifecycleFactory = lifecycleFactory;
  }

  public void start() {
    watch = directory.registerWatch(this::handle);
  }

  public void stop() {
    logger.debug("stopping watch");
    watch.stop();
    logger.debug("stopped watch");

    lifecycles.values().stream().forEach(lifecycle -> {
      try {
        logger.debug("stopping lifecycle");
        lifecycle.stop();
        logger.debug("stopped lifecycle");
      } catch (Exception e) {
        logger.warn("execption encountered while stopping processor", e);
      }
    });
    lifecycles.clear();
  }

  public void handle(EtcdEvent<ClusterProcess> event) {
    switch (event.getType()) {
      case added:
        if (nodeId.equals(event.getValue().getAssignedTo())) {
          startProcess(event.getKey(), event.getValue());
        }
        break;
      case updated:
        if (nodeId.equals(event.getPrevValue().getAssignedTo())) {
          stopProcess(event.getKey(), event.getPrevValue());
        }
        if (nodeId.equals(event.getValue().getAssignedTo())) {
          startProcess(event.getKey(), event.getValue());
        }
        break;
      case removed:
        if (nodeId.equals(event.getPrevValue().getAssignedTo())) {
          stopProcess(event.getKey(), event.getPrevValue());
        }
        break;
    }
  }

  protected void startProcess(String key, ClusterProcess node) {
    C configuration = convertConfiguration(node);
    ClusterProcessLifecycle process = lifecycleFactory.apply(configuration);
    ClusterProcessLifecycle existing = lifecycles.put(key, process);
    if (existing != null)
      safe(key, existing::stop);
    safe(key, process::start);
  }

  protected void stopProcess(String key, ClusterProcess node) {
    ClusterProcessLifecycle existing = lifecycles.remove(key);
    if (existing != null)
      safe(key, existing::stop);
  }

  protected static void safe(String key, Runnable runnable) {
    try {
      runnable.run();
    } catch (Throwable t) {
      logger.warn(String.format("message thrown while calling process %s", key), t);
    }
  }

  protected C convertConfiguration(ClusterProcess node) {
    try {
      return mapper.convertValue(node.getConfiguration(), type);
    } catch (IllegalArgumentException e) {
      logger.error("could not convert process configuration", e);
      throw e;
    }
  }

}