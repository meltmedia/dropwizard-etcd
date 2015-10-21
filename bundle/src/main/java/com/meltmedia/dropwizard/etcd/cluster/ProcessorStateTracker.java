package com.meltmedia.dropwizard.etcd.cluster;

import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.BiFunction;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.WatchService.Watch;

public class ProcessorStateTracker {
  private static Logger logger = LoggerFactory.getLogger(ProcessorStateTracker.class);
  public static class Builder {
    private MappedEtcdDirectory<ProcessorNode> directory;
    private ClusterNode thisNode;

    public Builder withDirectory(MappedEtcdDirectory<ProcessorNode> directory) {
      this.directory = directory;
      return this;
    }

    public Builder withThisNode(ClusterNode thisNode) {
      this.thisNode = thisNode;
      return this;
    }

    public ProcessorStateTracker build() {
      return new ProcessorStateTracker(directory, thisNode);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private MappedEtcdDirectory<ProcessorNode> directory;
  private Watch watch;
  private volatile ProcessorState state;
  private ClusterNode thisNode;
  
  private ProcessorStateTracker(MappedEtcdDirectory<ProcessorNode> directory, ClusterNode thisNode) {
    this.directory = directory;
    this.state = ProcessorState.empty();
    this.thisNode = thisNode;
  }
  
  public Runnable start( Runnable startWithTracker ) {
    return ()->{
      watch = directory.registerWatch(this::handle);
      try {
        startWithTracker.run();
      } finally {
        publishNode();
      }
    };
  }

  public Runnable stop(Runnable stopWithTracker) {
    return ()->{
      unpublishNode();
      try {
        stopWithTracker.run();
      } finally {
        watch.stop();
      }
    };
  }
  
  public ProcessorState getState() {
    return state;
  }

  public void handle(EtcdEvent<ProcessorNode> event) {
    switch (event.getType()) {
      case added:
        state = state.addProcessor(event.getIndex(), event.getValue());
        break;
      case removed:
        state = state.removeProcessor(event.getIndex(), event.getPrevValue());
        break;
      case updated:
        state = state.removeProcessor(event.getIndex(), event.getPrevValue()).addProcessor(event.getIndex(), event.getValue());
        break;
    }
  }
  
  public static class ProcessorState {
    private static Comparator<ProcessorNode> COMPARATOR = Ordering.<ProcessorNode> from(
      (n1, n2) -> Objects.compare(n1.getStartedAt(), n2.getStartedAt(), Ordering.natural()))
      .compound((n1, n2) -> Objects.compare(n1.getId(), n2.getId(), Ordering.natural()));

    public static ProcessorState empty() {
      return new ProcessorState(0, Collections.emptySortedSet());
    }
    
    public static ProcessorState empty( long lastModifiedIndex ) {
      return new ProcessorState(lastModifiedIndex, Collections.emptySortedSet());
    }
    
    private SortedSet<ProcessorNode> processors;
    private final long lastModifiedIndex;

    private ProcessorState(long lastModifiedIndex, SortedSet<ProcessorNode> processors) {
      this.lastModifiedIndex = lastModifiedIndex;
      this.processors = processors;
    }

    public ProcessorState addProcessor(long lastModifiedIndex, ProcessorNode newProcessor) {
      return new ProcessorState(lastModifiedIndex, newProcessors(newProcessor, SortedSet::add));
    }

    public ProcessorState removeProcessor(long lastModfiedIndex, ProcessorNode oldProcessor) {
      return new ProcessorState(lastModifiedIndex, newProcessors(oldProcessor, SortedSet::remove));
    }

    private SortedSet<ProcessorNode> newProcessors(ProcessorNode changingMember,
      BiFunction<SortedSet<ProcessorNode>, ProcessorNode, Boolean> action) {
      SortedSet<ProcessorNode> newProcessors = Sets.newTreeSet(COMPARATOR);
      newProcessors.addAll(processors);
      action.apply(newProcessors, changingMember);
      return newProcessors;
    }

    public int processorCount() {
      return processors.size();
    }
    
    public long lastModifiedIndex() {
      return lastModifiedIndex;
    }

    public SortedSet<ProcessorNode> getProcessors() {
      return Collections.unmodifiableSortedSet(processors);
    }

    public boolean hasProcessor(String nodeId) {
      return processors.stream().anyMatch(m -> Objects.equals(m.getId(), nodeId) );
    }
  }

  void publishNode() {
    directory.newDao().put(thisNode.getId(), new ProcessorNode().withId(thisNode.getId()).withStartedAt(new DateTime()));
  }
  
  void unpublishNode() {
    try {
      directory.newDao().remove(thisNode.getId());
    } catch( RuntimeException re ) {
      logger.warn("could not unpublish process node");
    }
  }

  void removeCrashedProcessor( ProcessorNode p ) {
    try {
      directory.newDao().remove(p.getId());
    } catch( Exception e ) {
      logger.debug("could not remove crashed processor node");
    }
  }
}
