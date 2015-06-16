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

import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.meltmedia.dropwizard.etcd.cluster.ClusterStateTracker.State;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryDao;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryException;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.WatchService;

public class ClusterAssignmentService {
  private static Logger logger = LoggerFactory.getLogger(ClusterAssignmentService.class);
  public static class Builder {
    
    private ScheduledExecutorService executor;
    private ClusterNode thisNode;
    private MappedEtcdDirectory<ClusterProcess> processDir;
    private ClusterStateTracker stateTracker;
    private Optional<FixedDelay> crashCleanupDelay = Optional.empty();

    public Builder withExecutor( ScheduledExecutorService executor ) {
      this.executor = executor;
      return this;
    }
    
    public Builder withProcessDir( MappedEtcdDirectory<ClusterProcess> processDir ) {
      this.processDir = processDir;
      return this;
    }
    
    public Builder withThisNode( ClusterNode thisNode ) {
      this.thisNode = thisNode;
      return this;
    }
    
    public Builder withClusterState( ClusterStateTracker stateTracker ) {
      this.stateTracker = stateTracker;
      return this;
    }
    
    public Builder withCrashCleanupDelay( FixedDelay crashCleanupDelay ) {
      this.crashCleanupDelay = Optional.ofNullable(crashCleanupDelay);
      return this;
    }

    public ClusterAssignmentService build() {
      return new ClusterAssignmentService(
        executor,
        thisNode,
        processDir,
        stateTracker,
        crashCleanupDelay
          .orElse(FixedDelay.builder()
            .withDelay(10)
            .withInitialDelay(10)
            .withTimeUnit(TimeUnit.SECONDS)
            .build()));
    }
  }
  
  public static Builder builder() {
    return new Builder();
  }

  public String getId() {
    return thisNode.getId();
  }

  public ClusterAssignmentService( ScheduledExecutorService executor, ClusterNode thisNode, MappedEtcdDirectory<ClusterProcess> processDir, ClusterStateTracker stateTracker, FixedDelay crashCleanupDelay  ) {
    this.executor = executor;
    this.thisNode = thisNode;
    this.processDir = processDir;
    this.stateTracker = stateTracker;
    this.processDao = processDir.newDao();
    this.crashCleanupDelay = crashCleanupDelay;
  }

  // extenral dependencies
  ScheduledExecutorService executor;
  ClusterNode thisNode;
  EtcdDirectoryDao<ClusterProcess> processDao;
  ClusterStateTracker stateTracker;
  MappedEtcdDirectory<ClusterProcess> processDir;
  FixedDelay crashCleanupDelay;

  // used by the service.
  ScheduledFuture<?> assignmentFuture;
  WatchService.Watch jobsWatch;
  AtomicInteger totalProcessCount = new AtomicInteger();
  AtomicInteger thisProcessCount = new AtomicInteger();
  Map<String, Set<String>> processes = Maps.newConcurrentMap();
  List<String> unassigned = Lists.newCopyOnWriteArrayList();
  private ScheduledFuture<?> cleanupFuture;
  
  public void start() {
    logger.debug("starting assignments for {}", thisNode.getId());
    jobsWatch = processDir.registerWatch(this::handleProcess);
    startNodeAssignmentTask();
    startFailureCleanupTask();
  }
  
  public void stop() {
    logger.debug("stopping assignments for {}", thisNode.getId());
    stopFailureCleanupTask();
    stopNodeAssignmentTask();
    unassignJobs();
    jobsWatch.stop();
    totalProcessCount.set(0);
    thisProcessCount.set(0);
    processes.clear();
    unassigned.clear();
  }

  public void startNodeAssignmentTask() {
    logger.info("starting assignment task for {}", thisNode.getId());
    assignmentFuture = executor.scheduleWithFixedDelay(()->{
      logger.debug("assignment loop for {}", thisNode.getId());
      try {
      int processCount = thisProcessCount.get();
      int currentNodeCount = stateTracker.getState().memberCount();
      int total = totalProcessCount.get();
      
      int maxProcessCount = currentNodeCount == 0 ? 0 : IntMath.divide(total, currentNodeCount, RoundingMode.CEILING);
      
      //System.out.printf("Node %s processCount %d minProcessCount %d maxProcessCount %d%n", thisNode.getId(), processCount, minProcessCount, maxProcessCount);
      logger.debug("Empty nodes on "+thisNode.getId()+" "+unassigned);

      if( processCount < maxProcessCount && !unassigned.isEmpty() ) {
        for( String toAssign : unassigned ) {
        try {
          processDao.update(toAssign, p->p.getAssignedTo() == null, p->p.withAssignedTo(thisNode.getId()));
          return;
        }
        catch( IndexOutOfBoundsException | EtcdDirectoryException e ) {
          logger.debug("could not assign process {}", e.getMessage());
          ignoreException(()->TimeUnit.SECONDS.sleep(1));
        }
        }
      } else if( processCount > maxProcessCount ) {
        for( String toUnassign : processes.get(thisNode.getId()) ) {
        try {
          processDao.update(toUnassign, p->thisNode.getId().equals(p.getAssignedTo()), p->p.withAssignedTo(null));
          return;
        }
        catch( IndexOutOfBoundsException | EtcdDirectoryException e ) {
          logger.warn("could not unassign process {}", e.getMessage());
        }
        }
      }
        ignoreException(()->TimeUnit.SECONDS.sleep(1));
      } catch( Exception e ) {
        logger.error("exception thrown in assignment process.", e);
      }
    }, 100L, 100L, TimeUnit.MILLISECONDS);
  }
  
  public void startFailureCleanupTask() {
    logger.info("starting failure clean up task for {}", thisNode.getId());
    cleanupFuture = executor.scheduleWithFixedDelay(()->{
      logger.debug("clean up loop for {}", thisNode.getId());

      // iterate over the process map and make sure we have an entry in the state nodes.
      State state = stateTracker.getState();
      if( state.isLeader(thisNode) ) {
        processes.entrySet().stream()
          .filter(processEntry->!stateTracker.getState().hasMember(processEntry.getKey()))
          .forEach((processEntry)->{
            logger.info("cleaning up assignments for node {}", processEntry.getKey());
            processEntry.getValue().stream().forEach(processId->processDao.update(
              processId,
              (process)->processEntry.getKey().equals(process.getAssignedTo()),
              (process)->process.withAssignedTo(null)));
          });
      }
    }, crashCleanupDelay.getInitialDelay(), crashCleanupDelay.getDelay(), crashCleanupDelay.getTimeUnit());
  }
  
  public void stopFailureCleanupTask() {
    try {
      cleanupFuture.cancel(true);
    }
    catch( Exception e ) {
      logger.info("error thrown while stoping cleanup task");
    }
    cleanupFuture = null;
  }
  
  static void ignoreException(ClusterAssignmentService.RunnableWithException r) {
    try {
      r.run();
    }
    catch( Exception e ) {
      // do nothing.
    }
  }
  
  static interface RunnableWithException {
    public void run() throws Exception;
  }
  
  public void stopNodeAssignmentTask() {
    try {
      assignmentFuture.cancel(true);
    }
    catch( Exception e ) {
      logger.info("error thrown while stoping assignment task");
    }
    assignmentFuture = null;
  }
  
  public void unassignJobs() {
    processes.getOrDefault(thisNode.getId(), Collections.emptySet())
      .stream()
      .forEach(processKey->{
        try {
          processDao.update(processKey, process->thisNode.getId().equals(process.getAssignedTo()), process->process.withAssignedTo(null));
        }
        catch( Exception e ) {
          logger.warn("Could not unassign process {}", processKey);
        }
      });
  }
  
  public void handleProcess( EtcdEvent<ClusterProcess> event ) {
    switch( event.getType() ) {
      case added:
        addProcess(event.getKey(), event.getValue());
        break;
      case updated:
        removeProcess(event.getKey(), event.getPrevValue());
        addProcess(event.getKey(), event.getValue());
        break;
      case removed:
        removeProcess(event.getKey(), event.getPrevValue());
        break;
    }
  }
  
  void addProcess( String key, ClusterProcess process ) {
    Optional<String> assignedTo = assignedToKey(process);
    if( assignedTo.isPresent() ) {
      processes.computeIfAbsent(assignedTo.get(), k->Sets.newConcurrentHashSet()).add(key);
    }
    else {
      if( unassigned.contains(key)) {
        logger.warn("Key {} already unassigned");
      }
      unassigned.add(key);
    }
    assignedTo
      .filter(thisNode.getId()::equals)
      .ifPresent((id)->thisProcessCount.incrementAndGet());
    totalProcessCount.incrementAndGet();
  }
  
  void removeProcess( String key, ClusterProcess process ) {
    Optional<String> assignedTo = assignedToKey(process);
    totalProcessCount.decrementAndGet();
    assignedTo
    .filter(thisNode.getId()::equals)
    .ifPresent((id)->thisProcessCount.decrementAndGet());
    
    if( assignedTo.isPresent() ) {
      processes.computeIfPresent(assignedTo.get(), (k, v)->{
        v.remove(key);
        return v.isEmpty() ? null : v;
      });
    }
    else {
      logger.debug("removing key {}", key);
      if( !unassigned.remove(key) ) {
        logger.warn("key removed but not present"); 
      }
    }
  }
  
  static Optional<String> assignedToKey(ClusterProcess process) {
    return Optional.ofNullable(process.getAssignedTo());
  }
  
  public static class FixedDelay {
     public static class Builder {
      private long initialDelay;
      private long delay;
      private TimeUnit timeUnit;

      public Builder withInitialDelay( long initialDelay ) {
        this.initialDelay = initialDelay;
        return this;
      }
      
      public Builder withDelay( long delay ) {
        this.delay = delay;
        return this;
      }
      
      public Builder withTimeUnit( TimeUnit timeUnit ) {
        this.timeUnit = timeUnit;
        return this;
      }
      
      public FixedDelay build() {
        return new FixedDelay(initialDelay, delay, timeUnit);
      }
    }
     
     public static Builder builder() {
       return new Builder();
     }
     
     private long initialDelay;
     private long delay;
     private TimeUnit timeUnit;

     public FixedDelay( long initialDelay, long delay, TimeUnit timeUnit ) {
       this.initialDelay = initialDelay;
       this.delay = delay;
       this.timeUnit = timeUnit;
     }

    public long getInitialDelay() {
      return initialDelay;
    }

    public long getDelay() {
      return delay;
    }

    public TimeUnit getTimeUnit() {
      return timeUnit;
    }
  }
}