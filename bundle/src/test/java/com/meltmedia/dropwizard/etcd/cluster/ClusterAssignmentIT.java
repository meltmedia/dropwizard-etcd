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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.meltmedia.dropwizard.etcd.EtcdClientRule;
import com.meltmedia.dropwizard.etcd.cluster.ClusterAssignmentService;
import com.meltmedia.dropwizard.etcd.cluster.ClusterAssignmentService.FixedDelay;
import com.meltmedia.dropwizard.etcd.cluster.ClusterService;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryDao;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdJsonRule;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.WatchService;

/**
 * The first version of this application just needs to keep all of the twitter streams
 * open as best as possible.  There are 2 concerns here:
 * 
 * 1. Getting the correct set of stream states published to etcd.
 * 2. Updating stream state as nodes come up and down.
 * 
 * To accompilsh the first task, it would help to have a leader that updated the state.
 * 
 * @author Christian Trimble
 *
 */
public class ClusterAssignmentIT {
  @ClassRule
  public static EtcdClientRule clientRule = new EtcdClientRule("http://127.0.0.1:2379");
  
  @Rule
  public EtcdJsonRule factoryRule = new EtcdJsonRule(clientRule::getClient, "/cluster-test");
  
  ClusterNode node1;
  ClusterNode node2;
  ClusterNode node3;
  ClusterAssignmentService service1;
  ClusterAssignmentService service2;
  ClusterAssignmentService service3;
  ScheduledExecutorService executor;
  EtcdDirectoryDao<ClusterProcess> dao;
  AssignmentLatchFactory latchFactory;
  MappedEtcdDirectory<ClusterNode> nodeDir;
  MappedEtcdDirectory<ClusterProcess> processDir;

  private ClusterService clusterService1;
  private ClusterService clusterService2;
  private ClusterService clusterService3;
  
  @Before
  public void setUp() throws Exception {
    executor = Executors.newScheduledThreadPool(10);
    
    nodeDir = factoryRule.getFactory().newDirectory("/app/nodes", new TypeReference<ClusterNode>(){});
    processDir = factoryRule.getFactory().newDirectory("/app/streams", new TypeReference<ClusterProcess>(){});
    
    node1 = new ClusterNode().withId("node1").withStartedAt(new DateTime());
    
    clusterService1 = ClusterService.builder()
      .withEtcdFactory(factoryRule.getFactory())
      .withExecutor(executor)
      .withNodesDirectory(nodeDir)
      .withThisNode(node1)
      .build();
    
    service1 = ClusterAssignmentService.builder()
      .withExecutor(executor)
      .withClusterState(clusterService1.getStateTracker())
      .withProcessDir(processDir)
      .withThisNode(node1)
      .build();
    
    node2 = new ClusterNode().withId("node2").withStartedAt(new DateTime());
    
    clusterService2 = ClusterService.builder()
      .withEtcdFactory(factoryRule.getFactory())
      .withExecutor(executor)
      .withNodesDirectory(nodeDir)
      .withThisNode(node2)
      .build();
    
   service2 = ClusterAssignmentService.builder()
      .withExecutor(executor)
      .withClusterState(clusterService2.getStateTracker())
      .withProcessDir(processDir)
      .withThisNode(node2)
      .build();

   node3 = new ClusterNode().withId("node3").withStartedAt(new DateTime());
   
   clusterService3 = ClusterService.builder()
     .withEtcdFactory(factoryRule.getFactory())
     .withExecutor(executor)
     .withNodesDirectory(nodeDir)
     .withThisNode(node3)
     .build();

   service3 = ClusterAssignmentService.builder()
      .withExecutor(executor)
      .withClusterState(clusterService3.getStateTracker())
      .withProcessDir(processDir)
      .withThisNode(node3)
      .build();
    
    dao = factoryRule.getFactory().newDirectory("/app/streams", new TypeReference<ClusterProcess>(){}).newDao();
    
    latchFactory = new AssignmentLatchFactory(factoryRule.getFactory().getWatchService(), "/app/streams");
  }

  @After
  public void tearDown() throws Exception {
    //clusterService3.stop();
    //clusterService2.stop();
    //clusterService1.stop();
    executor.shutdown();
  }
  
  @Test
  public void shouldAssignJob() throws InterruptedException {
    clusterService1.start();
    service1.start();
    
    dao.put("id", processNode(null, "name"));
    
    assertState("job assigned", s->s.assignments("node1")==1);
    
    service1.stop();
    clusterService1.stop();
  }
  
  @Test
  public void shouldAllowRestart() throws InterruptedException {
    dao.put("id", processNode(null, "name"));
    for( int i = 0; i < 25; i++ ) {
      clusterService1.start();
      service1.start();
    
      assertState("job assigned", s->s.assignments("node1")==1);
    
      service1.stop();
      clusterService1.stop();
      
      assertState("job unnassigned", s->s.unassigned()==1);
      
      clusterService2.start();
      service2.start();
    
      assertState("job assigned", s->s.assignments("node2")==1);
    
      service2.stop();
      clusterService2.stop();
    }
  }
  
  @Test
  public void shouldAssignJobsEvenly() throws InterruptedException {
    clusterService1.start();
    service1.start();
    clusterService2.start();
    service2.start();
    
    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));

    assertState("jobs evenly assigned", s->s.assignments("node1")==1 && s.assignments("node2")==1);
    
    service1.stop();
    clusterService1.stop();
    service2.stop();
    clusterService2.stop();
  }
  
  @Test
  public void shouldReassignWhenServiceAdded() throws InterruptedException {
    clusterService1.start();
    service1.start();
    
    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));
    
    assertState("initial state reached", s->s.assignments("node1")==2 && s.assignments("node2")==0);

    clusterService2.start();
    service2.start();
    
    assertState("reassigned after start", s->s.assignments("node1")==1 && s.assignments("node2")==1);
    
    service1.stop();
    clusterService1.stop();
    service2.stop();
    clusterService2.stop();
  }
  
  private void assertState(String message, Predicate<AssignmentState> test ) throws InterruptedException {
    assertThat(message, latchFactory
      .newLatch(message, test)
      .await(10, TimeUnit.SECONDS), equalTo(true));
  }
  
  @Test
  public void shouldReassignWhenServiceLost() throws InterruptedException {
    clusterService1.start();
    service1.start();
    clusterService2.start();
    service2.start();
    
    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));
    
    assertState("the initial state was reached", s->s.assignments("node1")==1 && s.assignments("node2")==1);

    service2.stop();
    clusterService2.stop();
    
    assertState("the jobs were reassigned", s->s.assignments("node1")==2 && s.assignments("node2")==0);
    
    service1.stop();
    clusterService1.stop();
  }
  
  @Test
  public void shouldUnassignWhenAllStopped() throws InterruptedException {
    clusterService1.start();
    service1.start();
    clusterService2.start();
    service2.start();
    
    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));
    
    assertState("the initial state was reached", s->s.unassigned()==0);

    service1.stop();
    clusterService1.stop();
    service2.stop();
    clusterService2.stop();
    
    assertState("all jobs were unassigned", s->s.unassigned()==2);
  }
  
  @Ignore
  @Test
  /**
   * There is a known bug with reusing a cluster assignment service.  Same code works with
   * new instances.
   * @throws InterruptedException
   */
  public void shouldBlueGreenDeploy() throws InterruptedException {
    List<ClusterAssignmentService> services = Lists.newArrayList(service1, service2, service3);
    
    Function<Integer, ClusterAssignmentService> serviceLookup = i->{
      return services.get(i%services.size());
    };
    
    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));
    dao.put("id3", processNode(null, "name3"));
    dao.put("id4", processNode(null, "name4"));
    dao.put("id5", processNode(null, "name5"));
    
    ClusterAssignmentService currentService = serviceLookup.apply(0);
    currentService.start();

    assertState("only green running", s->s.assignments("node1")==5);
    
    for( int i = 1; i < 10; i++ ) {
      ClusterAssignmentService nextService = serviceLookup.apply(i);
      nextService.start();
      
      assertState("blue and green running", s->s.maxAssignments() == 3 && s.minAssignments() == 2 && s.unassigned()==0);
      
      currentService.stop();
      
      assertState("blue now green", s->s.assignments(nextService.getId())==5);
      
      currentService = nextService;
    }
    
    currentService.stop();
    
    assertState("all stopped", s->s.unassigned()==5);
  }

  @Test
  public void shouldBlueGreenDeployWithNewServices() throws InterruptedException {
    int processCount = 50;
    for( int i = 0; i < processCount; i++ ) {
      dao.put("id"+i, processNode(null, "name"+i));
    }
    
    int halfFloorCount = IntMath.divide(processCount, 2, RoundingMode.FLOOR);
    int halfCeilCount = IntMath.divide(processCount, 2, RoundingMode.CEILING);
    
     ClusterNode node0 = new ClusterNode().withId("node0").withStartedAt(new DateTime());
    
     ClusterService currentClusterService = ClusterService.builder()
       .withEtcdFactory(factoryRule.getFactory())
       .withExecutor(executor)
       .withNodesDirectory(nodeDir)
       .withThisNode(node0)
       .build();
     
     currentClusterService.start();
    
    ClusterAssignmentService currentService = ClusterAssignmentService.builder()
      .withExecutor(executor)
      .withClusterState(currentClusterService.getStateTracker())
      .withProcessDir(processDir)
      .withThisNode(node0)
       .build();
    
    currentService.start();

    assertState("only green running", s->s.assignments("node0")==processCount);
    
    for( int i = 1; i < 10; i++ ) {
      ClusterNode nextNode = new ClusterNode().withId("node"+i).withStartedAt(new DateTime());
      
      ClusterService nextClusterService = ClusterService.builder()
        .withEtcdFactory(factoryRule.getFactory())
        .withExecutor(executor)
        .withNodesDirectory(nodeDir)
        .withThisNode(nextNode)
        .build();
      
      nextClusterService.start();

      ClusterAssignmentService nextService = ClusterAssignmentService.builder()
        .withExecutor(executor)
      .withClusterState(nextClusterService.getStateTracker())
        .withProcessDir(processDir)
        .withThisNode(nextNode)
         .build();
      nextService.start();
      
      assertState("blue and green running", s->s.maxAssignments() == halfCeilCount && s.minAssignments() == halfFloorCount && s.unassigned()==0);
      
      currentService.stop();
      currentClusterService.stop();
      
      assertState("blue now green", s->s.assignments(nextService.getId())==processCount);
      
      currentService = nextService;
      currentClusterService = nextClusterService;
    }
    
    currentService.stop();
    currentClusterService.stop();
    
    assertState("all stopped", s->s.unassigned()==processCount);
  }

  @Test
  public void shouldReassignWhenProcessStopped() throws InterruptedException {
    clusterService1.start();
    service1.start();
    clusterService2.start();
    service2.start();
    
    dao.put("id1", processNode(null, "name1"));
    
    assertState("the initial state was reached", s->s.unassigned()==0 && s.totalAssignments()==1);
    
    dao.put("id1", processNode(null, "name1"));
    
    assertState("the node was reassigned", s->s.assignments(service1.getId(), service2.getId())==1 && s.unassigned()==0);

    service1.stop();
    clusterService1.stop();
    service2.stop();
    clusterService2.stop();
    
    assertState("all jobs were unassigned", s->s.unassigned()==1 && s.totalAssignments()==0);
  }
  
  @Test
  public void shouldRecoverCrashedProcesses() throws InterruptedException {
    clusterService1.start();

    dao.put("id1", processNode(node2.getId(), "name1"));
    
    assertState("the initial state was reached", s->s.unassigned()==0 && s.assignments(node2.getId())==1);
    
    service1 = ClusterAssignmentService.builder()
      .withExecutor(executor)
      .withClusterState(clusterService1.getStateTracker())
      .withProcessDir(processDir)
      .withThisNode(node1)
      .withCrashCleanupDelay(FixedDelay.builder().withDelay(10).withInitialDelay(10).withTimeUnit(TimeUnit.MILLISECONDS).build())
      .build();
    service1.start();
    
    assertState("the stranded process was recovered", s->s.unassigned()==0 && s.assignments(node1.getId())==1);
    
    service1.stop();
    clusterService1.stop();
    
    assertState("all jobs were unassigned", s->s.unassigned()==1);

  }

  
  public static ClusterProcess processNode( String assignedTo, String name ) {
    return new ClusterProcess().withAssignedTo(assignedTo).withConfiguration(nodeData(name));
  }
  
  public static ObjectNode nodeData( String name ) {
    return JsonNodeFactory.instance.objectNode().put("name", name);
  }

  public static class NodeData {
    protected String name;

    public String getName() {
      return name;
    }

    public void setName( String name ) {
      this.name = name;
    }
    
    public NodeData withName( String name ) {
      this.name = name;
      return this;
    }
    
    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }
    
    public boolean equals( Object o ) {
      return EqualsBuilder.reflectionEquals(this, o);
    }
  }

  public static class AssignmentLatchFactory {
    private WatchService service;
    private String directory;

    public AssignmentLatchFactory( WatchService service, String directory ) {
      this.service = service;
      this.directory = directory;
    }
    
    public AssignmentLatch newLatch( String name, Predicate<AssignmentState> test ) {
      return new AssignmentLatch( service, directory, test, name );
    }
  }
  
  public static class AssignmentState {
    public static String UNASSIGNED = "unassigned";
    Map<String, Integer> assignedCount = Maps.newConcurrentMap();
    
    public void assign( String key ) {
      assignedCount.compute(Optional.ofNullable(key).orElse(UNASSIGNED), (k, count)->{
        return count == null ? 1 : count+1;
      });    
    }
    
    public int minAssignments() {
      return assignedCount.entrySet().stream()
        .filter(e->!UNASSIGNED.equals(e.getKey()))
        .mapToInt(Map.Entry::getValue)
        .min()
        .orElse(0);
    }

    public int maxAssignments() {
      return assignedCount.entrySet().stream()
        .filter(e->!UNASSIGNED.equals(e.getKey()))
        .mapToInt(Map.Entry::getValue)
        .max()
        .orElse(0);
    }

    public void unassign( String key ) {
      assignedCount.compute(Optional.ofNullable(key).orElse(UNASSIGNED), (k, count)->{
        if( count == null ) {
          throw new IllegalStateException("node unassigned when not assigned.");
        }
        if( count < 1 ) {
          throw new IllegalStateException("node count less than zero.");
        }
        return count == 1 ? null : count-1;
      });      
    }
    
    public int totalAssignments() {
      return assignedCount.entrySet().stream()
        .filter(e->!UNASSIGNED.equals(e.getKey()))
        .mapToInt(Map.Entry::getValue)
        .sum();
    }
    
    public int unassigned() {
      return assignedCount.getOrDefault(UNASSIGNED, 0);
    }
    
    public int assignments( String... keys ) {
      Set<String> keySet = Sets.newHashSet(keys);
      return assignedCount.entrySet().stream()
      .filter(e->keySet.contains(e.getKey()))
      .mapToInt(Map.Entry::getValue)
      .sum();
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");
      assignedCount.entrySet().stream().forEach(e->{
        sb.append(String.format(" %s:%s", e.getKey(), e.getValue()));
      });
      sb.append(" }");
      return sb.toString();
    }
  }
  
  public static class AssignmentLatch {
    private static Logger logger = LoggerFactory.getLogger(AssignmentLatch.class);
    AssignmentState state = new AssignmentState();
    WatchService service;
    String directory;
    Predicate<AssignmentState> test;
    CountDownLatch latch;
    private String name;
    
    public AssignmentLatch( WatchService service, String directory, Predicate<AssignmentState> test, String name ) {
      this.service = service;
      this.directory = directory;
      this.test = test;
      this.name = name;
      this.latch = new CountDownLatch(1);
    }
    
    public void handle( EtcdEvent<ClusterProcess> event ) {
      if( latch.getCount() != 0 ) {
      switch( event.getType() ) {
        case added:
          state.assign(event.getValue().getAssignedTo());
          break;
        case updated:
          state.unassign(event.getPrevValue().getAssignedTo());
          state.assign(event.getValue().getAssignedTo());
          break;
        case removed:
          state.unassign(event.getPrevValue().getAssignedTo());
          break;
      }

      try {
      if( test.test(state) ) {
        logger.debug("{} did match {}", name, state);
        latch.countDown();
      }
      else {
        logger.debug("{} did not match {}", name, state);
      }
      }
      catch( Exception e ) {
        logger.warn("bad latch predicate "+name, e);
      }
      }
    }
    
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
      WatchService.Watch watch = service.registerDirectoryWatch(directory, new TypeReference<ClusterProcess>(){}, this::handle);
      try {
        return latch.await(timeout, unit);
      }
      finally {
        watch.stop();
      }
    }
  }
}
