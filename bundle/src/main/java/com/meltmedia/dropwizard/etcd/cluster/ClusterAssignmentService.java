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
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import com.meltmedia.dropwizard.etcd.cluster.ClusterAssignmentTracker.AssignmentState;
import com.meltmedia.dropwizard.etcd.cluster.ClusterStateTracker.State;
import com.meltmedia.dropwizard.etcd.cluster.ProcessorStateTracker.ProcessorState;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryDao;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryException;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.RunnableWithException;

public class ClusterAssignmentService {
  private static Logger logger = LoggerFactory.getLogger(ClusterAssignmentService.class);

  public static class Builder {

    private ScheduledExecutorService executor;
    private ClusterNode thisNode;
    private MappedEtcdDirectory<ClusterProcess> processDir;
    private ClusterStateTracker stateTracker;
    private Optional<FixedDelay> crashCleanupDelay = Optional.empty();
    private MetricRegistry registry;
    private Supplier<AssignmentState> assignmentState;
    Function<String, String> metricName;
    private ProcessorStateTracker processorState;

    public Builder withExecutor(ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    public Builder withProcessDir(MappedEtcdDirectory<ClusterProcess> processDir) {
      this.processDir = processDir;
      return this;
    }

    public Builder withThisNode(ClusterNode thisNode) {
      this.thisNode = thisNode;
      return this;
    }

    public Builder withClusterState(ClusterStateTracker stateTracker) {
      this.stateTracker = stateTracker;
      return this;
    }
    
    public Builder withProcessorState(ProcessorStateTracker processorState ) {
      this.processorState = processorState;
      return this;
    }

    public Builder withCrashCleanupDelay(FixedDelay crashCleanupDelay) {
      this.crashCleanupDelay = Optional.ofNullable(crashCleanupDelay);
      return this;
    }
    
    public Builder withMetricRegistry( MetricRegistry registry ) {
      this.registry = registry;
      return this;
    }
    
    public Builder withMetricName( Function<String, String> metricName ) {
      this.metricName = metricName;
      return this;
    }
    
    public Builder withAssignmentState( Supplier<AssignmentState> assignmentState ) {
      this.assignmentState = assignmentState;
      return this;
    }    
    
    private static FixedDelay DEFAULT_DELAY = FixedDelay.builder()
      .withDelay(10)
      .withInitialDelay(10)
      .withTimeUnit(TimeUnit.SECONDS)
      .build();

    public ClusterAssignmentService build() {
      if( metricName == null ) {
        String dirName = processDir.getName().replace('.', '_');
        metricName = name->MetricRegistry.name(ClusterAssignmentService.class, dirName, name);
      }
      if( registry == null ) throw new IllegalStateException("metric registry is required");
      if( assignmentState == null ) throw new IllegalStateException("assignment state is required");
      if( processorState == null ) throw new IllegalStateException("processor state is required");
      return new ClusterAssignmentService(executor, thisNode, processDir, stateTracker, processorState, assignmentState,
        crashCleanupDelay.orElse(DEFAULT_DELAY),
        registry, metricName);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getId() {
    return thisNode.getId();
  }

  public ClusterAssignmentService(ScheduledExecutorService executor, ClusterNode thisNode,
    MappedEtcdDirectory<ClusterProcess> processDir, ClusterStateTracker stateTracker, ProcessorStateTracker processorState,
    Supplier<AssignmentState> assignmentState, FixedDelay crashCleanupDelay, MetricRegistry registry, Function<String, String> metricName) {
    this.executor = executor;
    this.thisNode = thisNode;
    this.processDir = processDir;
    this.stateTracker = stateTracker;
    this.processorState = processorState;
    this.assignmentState = assignmentState;
    this.processDao = processDir.newDao();
    this.crashCleanupDelay = crashCleanupDelay;
    this.registry = registry;
    this.metricName = metricName;
  }
  
  //
  // Metric Names
  //
  public static final String EXCEPTIONS = "exceptions";
  public static final String UNASSIGNMENT_FAILURES = "unassignmentFailures";
  public static final String ASSIGNMENT_FAILURES = "assignmentFailures";
  public static final String CLEAN_UP_TASK = "cleanUpTask";
  public static final String ASSIGNMENT_TASK = "assignmentTask";
  public static final List<String> ALL_METRICS = Lists.newArrayList(EXCEPTIONS, UNASSIGNMENT_FAILURES, ASSIGNMENT_FAILURES, CLEAN_UP_TASK, ASSIGNMENT_TASK);
  
  // extenral dependencies
  ScheduledExecutorService executor;
  ClusterNode thisNode;
  EtcdDirectoryDao<ClusterProcess> processDao;
  ClusterStateTracker stateTracker;
  ProcessorStateTracker processorState;
  MappedEtcdDirectory<ClusterProcess> processDir;
  FixedDelay crashCleanupDelay;
  MetricRegistry registry;
  Function<String, String> metricName;
  private Supplier<AssignmentState> assignmentState;

  // used by the service.
  ScheduledFuture<?> assignmentFuture;
  volatile long lastAssignmentIndex = 0L;
  private ScheduledFuture<?> cleanupFuture;
  final CleanShutdownCondition shutdown = new CleanShutdownCondition();
  private Meter assignmentTask;
  private Meter cleanUpTask;
  private Meter assignmentFailures;
  private Meter unassignmentFailures;
  private Meter exceptions;

  public void start() {
    logger.debug("starting assignments for {}", thisNode.getId());
    assignmentTask = registry.meter(metricName.apply(ASSIGNMENT_TASK));
    cleanUpTask = registry.meter(metricName.apply(CLEAN_UP_TASK));
    assignmentFailures = registry.meter(metricName.apply(ASSIGNMENT_FAILURES));
    unassignmentFailures = registry.meter(metricName.apply(UNASSIGNMENT_FAILURES));
    exceptions = registry.meter(metricName.apply(EXCEPTIONS));
    lastAssignmentIndex = 0L;
    startNodeAssignmentTask();
    startFailureCleanupTask();
  }

  public void stop() {
    logger.debug("stopping assignments for {}", thisNode.getId());
    shutdown.await(10, TimeUnit.MINUTES);      
    stopFailureCleanupTask();
    stopNodeAssignmentTask();
    unassignJobs();
    ALL_METRICS.forEach(name->registry.remove(metricName.apply(name)));
  }

  public void startNodeAssignmentTask() {
    logger.info("starting assignment task for {}", thisNode.getId());
    assignmentFuture =
      executor.scheduleWithFixedDelay(
        () -> {
          assignmentTask.mark();
          try {
            ProcessorState processorState = this.processorState.getState();
            AssignmentState assignmentState = this.assignmentState.get();
            State clusterState = stateTracker.getState();
            long lastSeenIndex = Math.max(assignmentState.etcdIndex, clusterState.lastModifiedIndex());
            
            if( lastAssignmentIndex > lastSeenIndex ) return;
            boolean active = clusterState.hasMember(thisNode.getId()) && processorState.hasProcessor(this.getId());
            int localProcesses = assignmentState.nodeProcessCount();
            int unassigned = assignmentState.unassignedProcessCount();
            int processorNodes = processorState.processorCount();
            int totalProcesses = assignmentState.totalProcessCount();

            int maxProcessCount =
              !active || totalProcesses == 0 || processorNodes == 0
                ? 0
                : IntMath.divide(assignmentState.totalProcessCount(), processorNodes, RoundingMode.CEILING);
            
            boolean giveProcess = localProcesses > maxProcessCount  && unassigned == 0;
            boolean takeProcess = active && localProcesses < maxProcessCount && unassigned > 0;
            boolean abandonProcess =  processorNodes == 0 && localProcesses > 0;
            boolean terminate = !active && localProcesses == 0;

            if( terminate ) {
              shutdown.signalAll();
              return;
            }
            else if (takeProcess) {
              for (String toAssign : assignmentState.unassigned) {
                try {
                  lastAssignmentIndex = processDao.update(toAssign, p -> p.getAssignedTo() == null,
                    p -> p.withAssignedTo(thisNode.getId()));
                  return;
                } catch (IndexOutOfBoundsException | EtcdDirectoryException e) {
                  assignmentFailures.mark();
                  logger.debug("could not assign process {}", e.getMessage());
                }
              }
            } else if (giveProcess || abandonProcess) {
              for (String toUnassign : assignmentState.processes.get(thisNode.getId())) {
                try {
                  lastAssignmentIndex = processDao.update(toUnassign, p -> thisNode.getId().equals(p.getAssignedTo()),
                    p -> p.withAssignedTo(null));
                  return;
                } catch (IndexOutOfBoundsException | EtcdDirectoryException e) {
                  unassignmentFailures.mark();
                  logger.warn("could not unassign process {}", e.getMessage());
                }
              }
            }
          }
          catch (Exception e) {
            exceptions.mark();
            logger.error("exception thrown in assignment process", e);
          }
        }, 100L, 100L, TimeUnit.MILLISECONDS);
  }

  public void startFailureCleanupTask() {
    logger.info("starting failure clean up task for {}", thisNode.getId());
    cleanupFuture =
      executor
        .scheduleWithFixedDelay(
          () -> {
            cleanUpTask.mark();

            // iterate over the process map and make sure we have an entry in the state nodes.
            State state = stateTracker.getState();
            if (state.isLeader(thisNode)) {
              processorState.getState().getProcessors().stream()
                .filter(p->!stateTracker.getState().hasMember(p.getId()))
                .forEach(p->processorState.removeCrashedProcessor(p));
              assignmentState.get().processes
                .entrySet()
                .stream()
                .filter(processEntry -> !stateTracker.getState().hasMember(processEntry.getKey()))
                .forEach(
                  (processEntry) -> {
                    logger.info("cleaning up assignments for node {}", processEntry.getKey());
                    processEntry
                      .getValue()
                      .stream()
                      .forEach(processId ->{
                        try {
                         processDao.update(processId, (process) -> processEntry
                          .getKey().equals(process.getAssignedTo()), (process) -> process
                          .withAssignedTo(null));
                        } catch( Exception e ) {
                          unassignmentFailures.mark();
                          logger.debug("could not unassign process after crash", e);
                        }});
                  });
            }
          }, crashCleanupDelay.getInitialDelay(), crashCleanupDelay.getDelay(), crashCleanupDelay
            .getTimeUnit());
  }

  public void stopFailureCleanupTask() {
    try {
      cleanupFuture.cancel(true);
    } catch (Exception e) {
      exceptions.mark();
      logger.warn("error thrown while stoping cleanup task");
    }
    cleanupFuture = null;
  }

  static void ignoreException(RunnableWithException<?> r) {
    try {
      r.run();
    } catch (Exception e) {
      // do nothing.
    }
  }

  public void stopNodeAssignmentTask() {
    try {
      assignmentFuture.cancel(true);
    } catch (Exception e) {
      exceptions.mark();
      logger.warn("error thrown while stoping assignment task");
    }
    assignmentFuture = null;
  }

  public void unassignJobs() {
    assignmentState.get().processes
      .getOrDefault(thisNode.getId(), Collections.emptySet())
      .stream()
      .forEach(
        processKey -> {
          try {
            processDao.update(processKey,
              process -> thisNode.getId().equals(process.getAssignedTo()),
              process -> process.withAssignedTo(null));
          } catch (Exception e) {
            unassignmentFailures.mark();
            logger.warn("could not unassign process {}", processKey);
          }
        });
  }

  public static class FixedDelay {
    public static class Builder {
      private long initialDelay;
      private long delay;
      private TimeUnit timeUnit;

      public Builder withInitialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
        return this;
      }

      public Builder withDelay(long delay) {
        this.delay = delay;
        return this;
      }

      public Builder withTimeUnit(TimeUnit timeUnit) {
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

    public FixedDelay(long initialDelay, long delay, TimeUnit timeUnit) {
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
  
  public static class CleanShutdownCondition {
    final Lock emptyLock = new ReentrantLock();
    final Condition empty  = emptyLock.newCondition();

    public boolean await( long time, TimeUnit unit ) {
      try {
        if( emptyLock.tryLock(10, TimeUnit.SECONDS) ) {
          try {
            if( !empty.await(time, unit) ) {
              logger.warn("forcing shutdown of processes");
              return false;
            }
            return true;
          }
          finally {
            emptyLock.unlock();
          }
        }
        else {
          return false;
        }
      } catch( InterruptedException e ) {
        logger.warn("interrupted while shutting down processes");
        Thread.currentThread().interrupt();
        return false;
      }
    }

    public void signalAll() {
      try {
        if( emptyLock.tryLock(10, TimeUnit.SECONDS) ) {
          try {
            empty.signalAll();
          }
          finally {
            emptyLock.unlock();
          }
        }
      }
      catch( InterruptedException ie ) {
        logger.warn("interrupted while signaling shutdown");
        Thread.currentThread().interrupt();
      }
    }
  }
}