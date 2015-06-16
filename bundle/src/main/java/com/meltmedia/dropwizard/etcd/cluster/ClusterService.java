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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meltmedia.dropwizard.etcd.json.EtcdJson;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.Heartbeat;

public class ClusterService {
  public static class Builder {
    private MappedEtcdDirectory<ClusterNode> nodesDirectory;
    private ScheduledExecutorService executor;
    private ClusterNode thisNode;
    private EtcdJson factory;
    
    public Builder withNodesDirectory( MappedEtcdDirectory<ClusterNode> nodesDirectory ) {
      this.nodesDirectory = nodesDirectory;
      return this;
    }
    
    public Builder withEtcdFactory( EtcdJson factory ) {
      this.factory = factory;
      return this;
    }
    
    public Builder withThisNode( ClusterNode thisNode ) {
      this.thisNode = thisNode;
      return this;
    }
    
    public Builder withExecutor( ScheduledExecutorService executor ) {
      this.executor = executor;
      return this;
    }
    
    public ClusterService build() {
      return new ClusterService(executor, factory, thisNode, nodesDirectory);
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

  public ClusterService( ScheduledExecutorService executor, EtcdJson factory, ClusterNode thisNode, MappedEtcdDirectory<ClusterNode> nodesDirectory ) {
    this.executor = executor;
    this.factory = factory;
    this.thisNode = thisNode;
    this.heartbeats = nodesDirectory.newHeartbeat("/"+thisNode.getId(), thisNode, 60);
    this.stateTracker = ClusterStateTracker.builder()
      .withDirectory(nodesDirectory)
      .withThisNode(thisNode)
      .build();
  }
  
  public void start() {
    heartbeats.start();
    stateTracker.start();
  }
  
  public void stop() {
    stateTracker.stop();
    heartbeats.stop();
  }
  
  public <C> ProcessService<C> newProcessService( String directory, Function<C, ClusterProcessLifecycle> lifecycleFactory, TypeReference<C> configType ) {
    return newProcessService(factory.newDirectory(directory, new TypeReference<ClusterProcess>(){}), lifecycleFactory, configType);
  }
  
  public <C> ProcessService<C> newProcessService(MappedEtcdDirectory<ClusterProcess> directory, Function<C, ClusterProcessLifecycle> lifecycleFactory, TypeReference<C> type) {
    return ProcessService.<C>builder()
      .withDirectory(directory)
      .withLifecycleFactory(lifecycleFactory)
      .withStateTracker(stateTracker)
      .withExecutor(executor)
      .withThisNode(thisNode)
      .withType(type)
      .withMapper(factory.getMapper())
      .build();
  }
  
  public static class ProcessService<C> {
    public static class Builder<C> {
      private MappedEtcdDirectory<ClusterProcess> directory;
      private ClusterNode thisNode;
      private Function<C, ClusterProcessLifecycle> lifecycleFactory;
      private ClusterStateTracker stateTracker;
      private ScheduledExecutorService executor;
      private TypeReference<C> type;
      private ObjectMapper mapper;
      
      public Builder<C> withDirectory( MappedEtcdDirectory<ClusterProcess> directory ) {
        this.directory = directory;
        return this;
      }
      
      public Builder<C> withExecutor( ScheduledExecutorService executor ) {
        this.executor = executor;
        return this;
      }

      public Builder<C> withStateTracker( ClusterStateTracker stateTracker ) {
        this.stateTracker = stateTracker;
        return this;
      }

      public Builder<C> withThisNode( ClusterNode thisNode ) {
        this.thisNode = thisNode;
        return this;
      }
      
      public Builder<C> withLifecycleFactory(Function<C, ClusterProcessLifecycle> lifecycleFactory) {
        this.lifecycleFactory = lifecycleFactory;
        return this;
      }
      
      public Builder<C> withType( TypeReference<C> type ) {
        this.type = type;
        return this;
      }
      
      public Builder<C> withMapper( ObjectMapper mapper ) {
        this.mapper = mapper;
        return this;
      }
      
      public ProcessService<C> build() {
        return new ProcessService<C>(thisNode, stateTracker, executor, directory, lifecycleFactory, type, mapper);
      }
    }

    public static <C> Builder<C> builder() {
      return new Builder<C>();
    }

    private ClusterAssignmentService assignments;
    private ClusterProcessor<C> processor;

    public ProcessService( ClusterNode thisNode , ClusterStateTracker stateTracker , ScheduledExecutorService executor , MappedEtcdDirectory<ClusterProcess> directory, Function<C, ClusterProcessLifecycle> lifecycleFactory, TypeReference<C> type, ObjectMapper mapper ) {
      this.assignments = ClusterAssignmentService.builder()
        .withExecutor(executor)
        .withProcessDir(directory)
        .withThisNode(thisNode)
        .withClusterState(stateTracker)
        .build();
      this.processor = ClusterProcessor.<C>builder()
        .withLifecycleFactory(lifecycleFactory)
        .withDirectory(directory)
        .withType(type)
        .withNodeId(thisNode.getId())
        .withMapper(mapper)
        .build();
    }
    
    public void start() {
      processor.start();
      assignments.start();
    }
    
    public void stop() {
      assignments.stop();
      processor.stop();  
    }

  }

  public ClusterNode getThisNode() {
    return thisNode;
  }

  public ClusterStateTracker getStateTracker() {
    return stateTracker;
  }
}
