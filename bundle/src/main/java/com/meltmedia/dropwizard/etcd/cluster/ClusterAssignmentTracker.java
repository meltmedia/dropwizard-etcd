package com.meltmedia.dropwizard.etcd.cluster;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.WatchService;

/**
 * Tracks the state of assignments in the system.
 * 
 * @author Christian Trimble
 */
public class ClusterAssignmentTracker {
  public static final String ASSIGNED = "assigned";
  public static final String UNASSIGNED = "unassigned";
  public static final String TOTAL = "total";

  public static class AssignmentState {
    final int totalProcessCount;
    final int nodeProcessCount;
    final int unassignedProcessCount;
    final long etcdIndex;
    final ClusterNode node;
    final Map<String, Set<String>> processes;
    final Set<String> unassigned;
    
    AssignmentState( ClusterNode node, long etcdIndex, Map<String, Set<String>> processes, Set<String> unassigned, int totalProcessCount, int nodeProcessCount, int unassignedProcessCount ) {
      this.node = node;
      this.etcdIndex = etcdIndex;
      this.processes = Collections.unmodifiableMap(processes);
      this.unassigned = Collections.unmodifiableSet(unassigned);
      this.totalProcessCount = totalProcessCount;
      this.nodeProcessCount = nodeProcessCount;
      this.unassignedProcessCount = unassignedProcessCount;
    }
    
    public AssignmentState addProcess(long etcdIndex, String key, ClusterProcess process) {
      Optional<String> assignedTo = assignedToKey(process);
      Map<String, Set<String>> processes = Maps.newHashMap(this.processes);
      Set<String> unassigned = Sets.newHashSet(this.unassigned);
      if( assignedTo.isPresent() ) {
        processes.computeIfPresent(assignedTo.get(), (k, v) -> {
          Set<String> ids = Sets.newHashSet(v);
          ids.add(key);
          return ids.isEmpty() ? null : Collections.unmodifiableSet(ids);
        });
        processes.computeIfAbsent(assignedTo.get(), k->Sets.newHashSet(key));
      }
      else {
        unassigned.add(key);
      }
      int totalProcessCount = 
        unassigned.size() +
        processes.entrySet().stream()
          .mapToInt(e->e.getValue().size())
          .sum();
      int nodeProcessCount = 
        processes.getOrDefault(node.getId(), Collections.EMPTY_SET)
        .size();
      int unassignedProcessCount = unassigned.size();
      return new AssignmentState(node, etcdIndex, processes, unassigned, totalProcessCount, nodeProcessCount, unassignedProcessCount );
    }
    
    public AssignmentState removeProcess( long etcdIndex, String key, ClusterProcess process ) {
      Optional<String> assignedTo = assignedToKey(process);
      Map<String, Set<String>> processes = Maps.newHashMap(this.processes);
      Set<String> unassigned = Sets.newHashSet(this.unassigned);
      if( assignedTo.isPresent() ) {
        processes.computeIfPresent(assignedTo.get(), (k, v) -> {
          Set<String> ids = Sets.newHashSet(v);
          ids.remove(key);
          return ids.isEmpty() ? null : Collections.unmodifiableSet(ids);
        });
      }
      else {
        unassigned.remove(key);
      }
      int totalProcessCount = unassigned.size() + processes.entrySet().stream().mapToInt(e->e.getValue().size()).sum();
      int nodeProcessCount = 
        processes.getOrDefault(node.getId(), Collections.EMPTY_SET)
        .size();
      int unassignedProcessCount = unassigned.size();
      return new AssignmentState(node, etcdIndex, processes, unassigned, totalProcessCount, nodeProcessCount, unassignedProcessCount );
    }

    public int nodeProcessCount() {
      return nodeProcessCount;
    }

    public int totalProcessCount() {
      return totalProcessCount;
    }

    public int unassignedProcessCount() {
      return unassignedProcessCount;
    }
  }

  static Optional<String> assignedToKey(ClusterProcess process) {
    return Optional.ofNullable(process.getAssignedTo());
  }
  
  private volatile AssignmentState state;
  MappedEtcdDirectory<ClusterProcess> processDir;
  WatchService.Watch jobsWatch;
  MetricRegistry registry;
  Function<String, String> metricName;
  
  public ClusterAssignmentTracker( ClusterNode node, MappedEtcdDirectory<ClusterProcess> processDir, MetricRegistry registry, Function<String, String> metricName ) {
    state = new AssignmentState(node, 0, Collections.emptyMap(), Collections.emptySet(), 0, 0, 0);
    this.registry = registry;
    this.metricName = metricName;
    this.processDir = processDir;
  }
  
  public AssignmentState getState() {
    return state;
  }
  
  public void handleProcess(EtcdEvent<ClusterProcess> event) {
    switch (event.getType()) {
      case added:
        state = state.addProcess(event.getIndex(), event.getKey(), event.getValue());
        break;
      case updated:
        state = state
          .removeProcess(event.getIndex(), event.getKey(), event.getPrevValue())
          .addProcess(event.getIndex(), event.getKey(), event.getValue());
        break;
      case removed:
        state = state.removeProcess(event.getIndex(), event.getKey(), event.getPrevValue());
        break;
    }
  }

  public void start() {
    registry.register(metricName.apply(TOTAL), (Gauge<Integer>)()->state.totalProcessCount);
    registry.register(metricName.apply(ASSIGNED), (Gauge<Integer>)()->state.nodeProcessCount);
    registry.register(metricName.apply(UNASSIGNED), (Gauge<Integer>)()->state.unassignedProcessCount);
    jobsWatch = processDir.registerWatch(this::handleProcess);
  }

  public void stop() {
    jobsWatch.stop();
    registry.remove(metricName.apply(UNASSIGNED));
    registry.remove(metricName.apply(ASSIGNED));
    registry.remove(metricName.apply(TOTAL));
  }
}
