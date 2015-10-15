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

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meltmedia.dropwizard.etcd.json.EtcdJson;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.EtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.Heartbeat;

public class ClusterService {
  public static class Builder {
    private MappedEtcdDirectory<ClusterNode> nodesDirectory;
    private ScheduledExecutorService executor;
    private ClusterNode thisNode;
    private EtcdJson factory;
    private MetricRegistry registry;

    public Builder withNodesDirectory(MappedEtcdDirectory<ClusterNode> nodesDirectory) {
      this.nodesDirectory = nodesDirectory;
      return this;
    }

    public Builder withEtcdFactory(EtcdJson factory) {
      this.factory = factory;
      return this;
    }

    public Builder withThisNode(ClusterNode thisNode) {
      this.thisNode = thisNode;
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

    public ClusterService build() {
      return new ClusterService(executor, factory, thisNode, nodesDirectory, registry);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private EtcdJson factory;
  private ClusterNode thisNode;
  private ClusterStateTracker stateTracker;
  private ScheduledExecutorService executor;
  private Heartbeat<ClusterNode> heartbeats;
  private MetricRegistry registry;

  public ClusterService(ScheduledExecutorService executor, EtcdJson factory, ClusterNode thisNode,
    MappedEtcdDirectory<ClusterNode> nodesDirectory, MetricRegistry registry) {
    this.executor = executor;
    this.factory = factory;
    this.thisNode = thisNode;
    this.heartbeats = nodesDirectory.newHeartbeat("/" + thisNode.getId(), thisNode, 60);
    this.stateTracker =
      ClusterStateTracker.builder()
        .withDirectory(nodesDirectory)
        .withThisNode(thisNode)
        .build();
    this.registry = registry;
  }

  public void start() {
    heartbeats.start();
    stateTracker.start();
  }

  public void stop() {
    stateTracker.stop();
    heartbeats.stop();
  }

  public <C> ProcessService<C> newProcessService(String directory,
    Function<C, ClusterProcessLifecycle> lifecycleFactory, TypeReference<C> configType) {
    return newProcessService(factory.newDirectory(directory), lifecycleFactory, configType);
  }

  public <C> ProcessService<C> newProcessService(EtcdDirectory directory,
    Function<C, ClusterProcessLifecycle> lifecycleFactory, TypeReference<C> type) {
    return ProcessService.<C> builder()
      .withDirectory(directory)
      .withLifecycleFactory(lifecycleFactory)
      .withStateTracker(stateTracker)
      .withExecutor(executor)
      .withThisNode(thisNode)
      .withType(type)
      .withMapper(factory.getMapper())
      .withMetricRegistry(registry)
      .build();
  }

  public static class ProcessService<C> {
    public static class Builder<C> {
      private EtcdDirectory directory;
      private ClusterNode thisNode;
      private Function<C, ClusterProcessLifecycle> lifecycleFactory;
      private ClusterStateTracker stateTracker;
      private ScheduledExecutorService executor;
      private TypeReference<C> type;
      private ObjectMapper mapper;
      private MetricRegistry registry;
      
      public Builder<C> withDirectory(EtcdDirectory directory) {
        this.directory = directory;
        return this;
      }

      public Builder<C> withExecutor(ScheduledExecutorService executor) {
        this.executor = executor;
        return this;
      }

      public Builder<C> withStateTracker(ClusterStateTracker stateTracker) {
        this.stateTracker = stateTracker;
        return this;
      }

      public Builder<C> withThisNode(ClusterNode thisNode) {
        this.thisNode = thisNode;
        return this;
      }

      public Builder<C> withLifecycleFactory(Function<C, ClusterProcessLifecycle> lifecycleFactory) {
        this.lifecycleFactory = lifecycleFactory;
        return this;
      }

      public Builder<C> withType(TypeReference<C> type) {
        this.type = type;
        return this;
      }

      public Builder<C> withMapper(ObjectMapper mapper) {
        this.mapper = mapper;
        return this;
      }
      
      public Builder<C> withMetricRegistry( MetricRegistry registry ) {
        this.registry = registry;
        return this;
      }

      public ProcessService<C> build() {
        if( registry == null ) throw new IllegalStateException("metric registry is required");
        return new ProcessService<C>(thisNode, stateTracker, executor, directory, lifecycleFactory,
          type, mapper, registry);
      }
    }

    public static <C> Builder<C> builder() {
      return new Builder<C>();
    }

    private ClusterAssignmentService assignments;
    private ClusterAssignmentTracker tracker;
    private ProcessorStateTracker processorStateTracker;
    private ClusterProcessor<C> processor;
    private MappedEtcdDirectory<ClusterProcess> processDirectory;
    private MappedEtcdDirectory<ProcessorNode> processorDirectory;
    private ClusterNode thisNode;

    public ProcessService(ClusterNode thisNode, ClusterStateTracker stateTracker, ScheduledExecutorService executor, EtcdDirectory directory,
      Function<C, ClusterProcessLifecycle> lifecycleFactory, TypeReference<C> type,
      ObjectMapper mapper, MetricRegistry registry) {
      this.thisNode = thisNode;
      String dirName = directory.getName().replace('.', '_');
      Function<String, String> metricName = name->MetricRegistry.name(ClusterAssignmentService.class, dirName, name);
      this.processDirectory = directory.newDirectory("/processes", new TypeReference<ClusterProcess>(){});
      this.processorDirectory = directory.newDirectory("/processors", new TypeReference<ProcessorNode>(){});
      this.tracker = new ClusterAssignmentTracker(thisNode, processDirectory, registry, metricName);
      this.processorStateTracker = ProcessorStateTracker.builder().withDirectory(processorDirectory).withThisNode(thisNode).build();
      this.assignments =
        ClusterAssignmentService.builder()
          .withExecutor(executor)
          .withProcessDir(processDirectory)
          .withProcessorState(processorStateTracker)
          .withAssignmentState(tracker::getState)
          .withThisNode(thisNode)
          .withClusterState(stateTracker)
          .withMetricRegistry(registry)
          .withMetricName(metricName)
          .build();
      this.processor =
        ClusterProcessor.<C> builder()
          .withLifecycleFactory(lifecycleFactory)
          .withDirectory(processDirectory)
          .withType(type)
          .withNodeId(thisNode.getId())
          .withMapper(mapper)
          .build();
    }

    public void start() {
      processorStateTracker.start(()->{
        processor.start();
        tracker.start();
        assignments.start();
      }).run();
    }

    public void stop() {
      processorStateTracker.stop(()->{
        assignments.stop();
        tracker.stop();
        processor.stop();
      }).run();
    }

    public MappedEtcdDirectory<ClusterProcess> getDirectory() {
      return processDirectory;
    }

    public String getId() {
      return thisNode.getId();
    }
  }

  public ClusterNode getThisNode() {
    return thisNode;
  }

  public ClusterStateTracker getStateTracker() {
    return stateTracker;
  }
}
