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

import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.BiFunction;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.WatchService.Watch;

public class ClusterStateTracker {
  public static class Builder {
    private MappedEtcdDirectory<ClusterNode> directory;
    private ClusterNode thisNode;
    
    public Builder withDirectory( MappedEtcdDirectory<ClusterNode> directory ) {
      this.directory = directory;
      return this;
    }
    
    public Builder withThisNode( ClusterNode thisNode ) {
      this.thisNode = thisNode;
      return this;
    }
    
    public ClusterStateTracker build() {
      return new ClusterStateTracker(directory, thisNode);
    }
  }
  
  public static Builder builder() {
    return new Builder();
  }

  private MappedEtcdDirectory<ClusterNode> directory;
  private Watch watch;
  private volatile State state;
  
  private ClusterStateTracker(MappedEtcdDirectory<ClusterNode> directory, ClusterNode thisNode ) {
    this.directory = directory;
    this.state = State.empty();
  }
  
  public void start() {
    watch = directory.registerWatch(this::handle);
  }
  
  public void stop() {
    watch.stop();
  }
  
  public State getState() {
    return state;
  }
  
  public void handle( EtcdEvent<ClusterNode> event ) {
    switch( event.getType() ) {
      case added:
        state = state.addMember(event.getValue());
        break;
      case removed:
        state = state.removeMember(event.getPrevValue());
       break;
      case updated:
        state = state.removeMember(event.getPrevValue()).addMember(event.getValue());
        break;
    }
  }
  
  public static class State {
    private static Comparator<ClusterNode> COMPARATOR = Ordering
      .<ClusterNode>from((n1, n2)->Objects.compare(n1.getStartedAt(), n2.getStartedAt(), Ordering.natural()))
      .compound((n1, n2)->Objects.compare(n1.getId(), n2.getId(), Ordering.natural()));
    
    public static State empty() {
      return new State(Sets.newTreeSet(COMPARATOR));
    }

    private SortedSet<ClusterNode> members;
    
    private State(SortedSet<ClusterNode> members) {
      this.members = members;
    }
    
    public ClusterNode master() {
      return members.isEmpty() ? null : members.first();
    }
    
    public State addMember( ClusterNode newMember ) {
      return new State(newMembers(newMember, SortedSet::add));
    }
    
    public State removeMember( ClusterNode oldMember ) {
      return new State(newMembers(oldMember, SortedSet::remove));
    }
    
    private SortedSet<ClusterNode> newMembers( ClusterNode changingMember, BiFunction<SortedSet<ClusterNode>, ClusterNode, Boolean> action) {
      SortedSet<ClusterNode> newMembers = Sets.newTreeSet(COMPARATOR);
      newMembers.addAll(members);
      action.apply(newMembers, changingMember);
      return newMembers;
    }

    public int memberCount() {
      return members.size();
    }
    
    public SortedSet<ClusterNode> getMembers() {
      return Collections.unmodifiableSortedSet(members);
    }

    public boolean hasMember( String nodeId ) {
      return members.stream().anyMatch(m->m.getId().equals(nodeId));
    }

    public boolean isLeader( ClusterNode thisNode ) {
      return members.last().getId().equals(thisNode.getId());
    }
  }
}
