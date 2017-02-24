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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
import com.meltmedia.dropwizard.etcd.cluster.ClusterService.ProcessService;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryDao;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.EtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.WatchService;
import com.meltmedia.dropwizard.etcd.junit.EtcdClientRule;
import com.meltmedia.dropwizard.etcd.junit.EtcdJsonRule;

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
  public static EtcdClientRule clientRule = new EtcdClientRule("http://127.0.0.1:2379").withMaxFrameSize(1014*1000);

  @Rule
  public EtcdJsonRule factoryRule = new EtcdJsonRule(clientRule::getClient, "/cluster-test");

  ClusterNode node1;
  ClusterNode node2;
  ClusterNode node3;
  ClusterNode node4;
  ClusterNode node5;
  ClusterNode node6;
  ScheduledExecutorService executor;
  EtcdDirectoryDao<ClusterProcess> dao;
  EtcdDirectoryDao<ProcessorNode> processorDao;
  AssignmentLatchFactory latchFactory;
  MappedEtcdDirectory<ClusterNode> nodeDir;
  EtcdDirectory processDir;

  private ClusterService clusterService1;
  private ClusterService clusterService2;
  private ClusterService clusterService3;
  private ClusterService clusterService4;
  private ClusterService clusterService5;
  private ClusterService clusterService6;
  
  private ProcessService<ObjectNode> processService1;
  private ProcessService<ObjectNode> processService2;
  private ProcessService<ObjectNode> processService3;
  private ProcessService<ObjectNode> processService4;
  private ProcessService<ObjectNode> processService5;
  private ProcessService<ObjectNode> processService6;
  
  private MetricRegistryListener listener1;

  @Before
  public void setUp() throws Exception {
    executor = Executors.newScheduledThreadPool(10);

    nodeDir = factoryRule.getFactory().newDirectory("/app/nodes", new TypeReference<ClusterNode>() {
    });
    processDir =
      factoryRule.getFactory().newDirectory("/app/streams");
    
    MappedEtcdDirectory<ClusterProcess> processNodeDir = processDir.newDirectory("/processes", new TypeReference<ClusterProcess>(){});
    MappedEtcdDirectory<ProcessorNode> processorNodeDir = processDir.newDirectory("/processors", new TypeReference<ProcessorNode>(){});

    node1 = new ClusterNode().withId("node1").withStartedAt(new DateTime());
    
    MetricRegistry registry1 = new MetricRegistry();
    registry1.addListener(listener1 = mock(MetricRegistryListener.class));

    clusterService1 =
      ClusterService.builder()
        .withEtcdFactory(factoryRule.getFactory())
        .withExecutor(executor)
        .withNodesDirectory(nodeDir)
        .withThisNode(node1)
        .withMetricRegistry(registry1)
        .build();
    
    processService1 = clusterService1.newProcessService(processDir, ClusterAssignmentIT::toLifecycle, new TypeReference<ObjectNode>(){});

    node2 = new ClusterNode().withId("node2").withStartedAt(new DateTime());

    clusterService2 =
      ClusterService.builder()
        .withEtcdFactory(factoryRule.getFactory())
        .withExecutor(executor)
        .withNodesDirectory(nodeDir)
        .withThisNode(node2)
        .withMetricRegistry(new MetricRegistry())
        .build();

    processService2 = clusterService2.newProcessService(processDir, ClusterAssignmentIT::toLifecycle, new TypeReference<ObjectNode>(){});

    node3 = new ClusterNode().withId("node3").withStartedAt(new DateTime());

    clusterService3 =
      ClusterService.builder()
        .withEtcdFactory(factoryRule.getFactory())
        .withExecutor(executor)
        .withNodesDirectory(nodeDir)
        .withThisNode(node3)
        .withMetricRegistry(new MetricRegistry())
        .build();

    processService3 = clusterService3.newProcessService(processDir, ClusterAssignmentIT::toLifecycle, new TypeReference<ObjectNode>(){});

    node4 = new ClusterNode().withId("node4").withStartedAt(new DateTime());

    clusterService4 =
      ClusterService.builder()
        .withEtcdFactory(factoryRule.getFactory())
        .withExecutor(executor)
        .withNodesDirectory(nodeDir)
        .withThisNode(node4)
        .withMetricRegistry(new MetricRegistry())
        .build();

    processService4 = clusterService4.newProcessService(processDir, ClusterAssignmentIT::toLifecycle, new TypeReference<ObjectNode>(){});

    node5 = new ClusterNode().withId("node5").withStartedAt(new DateTime());

    clusterService5 =
      ClusterService.builder()
        .withEtcdFactory(factoryRule.getFactory())
        .withExecutor(executor)
        .withNodesDirectory(nodeDir)
        .withThisNode(node5)
        .withMetricRegistry(new MetricRegistry())
        .build();

    processService5 = clusterService5.newProcessService(processDir, ClusterAssignmentIT::toLifecycle, new TypeReference<ObjectNode>(){});

    node6 = new ClusterNode().withId("node6").withStartedAt(new DateTime());

    clusterService6 =
      ClusterService.builder()
        .withEtcdFactory(factoryRule.getFactory())
        .withExecutor(executor)
        .withNodesDirectory(nodeDir)
        .withThisNode(node6)
        .withMetricRegistry(new MetricRegistry())
        .build();

    processService6 = clusterService6.newProcessService(processDir, ClusterAssignmentIT::toLifecycle, new TypeReference<ObjectNode>(){});

    dao =
      processNodeDir.newDao();
    processorDao = processorNodeDir.newDao();

    latchFactory =
      new AssignmentLatchFactory(factoryRule.getFactory().getWatchService(), processNodeDir.getPath());
  }
  
  public static ClusterProcessLifecycle toLifecycle( ObjectNode process ) {
    return new ClusterProcessLifecycle() {
      @Override public void start() {}
      @Override public void stop() {}
    };
  }

  @After
  public void tearDown() throws Exception {
    // clusterService3.stop();
    // clusterService2.stop();
    // clusterService1.stop();
    executor.shutdown();
  }

  @Test
  public void shouldAssignJob() throws InterruptedException {
    
    clusterService1.start();
    processService1.start();

    dao.put("id", processNode(null, "name"));

    assertState("job assigned", s -> s.assignments("node1") == 1);

    processService1.stop();
    clusterService1.stop();
  }

  @Test
  public void shouldAssignMultipleWithOneNode() throws InterruptedException {
    clusterService1.start();
    processService1.start();

    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));

    assertState("job assigned", s -> s.assignments("node1") == 2);

    processService1.stop();
    clusterService1.stop();
  }

  @Test
  public void shouldAssignExistingBrokenNodes() throws InterruptedException {
    dao.put("id1", processNode(null, "name1").withAssignedTo("junk"));
    dao.put("id2", processNode(null, "name2").withAssignedTo("moreJunk"));

    clusterService1.start();
    processService1.start();

    assertState("job assigned", s -> s.assignments("node1") == 2, 20, TimeUnit.SECONDS);

    processService1.stop();
    clusterService1.stop();
  }

  @Test
  public void shouldAllowRestart() throws InterruptedException {
    dao.put("id", processNode(null, "name"));
    for (int i = 0; i < 25; i++) {
      clusterService1.start();
      processService1.start();

      assertState("job assigned", s -> s.assignments("node1") == 1);

      processService1.stop();
      clusterService1.stop();

      assertState("job unnassigned", s -> s.unassigned() == 1);

      clusterService2.start();
      processService2.start();

      assertState("job assigned", s -> s.assignments("node2") == 1);

      processService2.stop();
      clusterService2.stop();
    }
  }

  @Test
  public void shouldAssignJobsEvenly() throws InterruptedException {
    clusterService1.start();
    processService1.start();
    clusterService2.start();
    processService2.start();

    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));

    assertState("jobs evenly assigned", s -> s.assignments("node1") == 1
      && s.assignments("node2") == 1);

    processService1.stop();
    clusterService1.stop();
    processService2.stop();
    clusterService2.stop();
  }

  @Test
  public void shouldReassignWhenServiceAdded() throws InterruptedException {
    clusterService1.start();
    processService1.start();

    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));

    assertState("initial state reached", s -> s.assignments("node1") == 2
      && s.assignments("node2") == 0);

    clusterService2.start();
    processService2.start();

    assertState("reassigned after start",
      s -> s.assignments("node1") == 1 && s.assignments("node2") == 1);

    processService1.stop();
    clusterService1.stop();
    processService2.stop();
    clusterService2.stop();
  }

  private void assertState(String message, Predicate<AssignmentState> test)
    throws InterruptedException {
    assertThat(message, latchFactory.newLatch(message, test).await(30, TimeUnit.SECONDS),
      equalTo(true));
  }

  private void assertState(String message, Predicate<AssignmentState> test, Predicate<AssignmentState> illegalStateTest)
    throws InterruptedException {
    assertThat(message, latchFactory.newLatch(message, test, illegalStateTest).await(100, TimeUnit.SECONDS),
      equalTo(true));
  }

  private void assertState(String message, Predicate<AssignmentState> test, long duration,
    TimeUnit unit) throws InterruptedException {
    assertThat(message, latchFactory.newLatch(message, test).await(duration, unit), equalTo(true));
  }
  
  @SuppressWarnings("unused")
  private void assertState(String message, Predicate<AssignmentState> test, Predicate<AssignmentState> illegalStateTest, long duration,
    TimeUnit unit) throws InterruptedException {
    assertThat(message, latchFactory.newLatch(message, test, illegalStateTest).await(duration, unit), equalTo(true));
  }

  @Test
  public void shouldReassignWhenServiceLost() throws InterruptedException {
    clusterService1.start();
    processService1.start();
    clusterService2.start();
    processService2.start();

    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));

    assertState("the initial state was reached",
      s -> s.assignments("node1") == 1 && s.assignments("node2") == 1);

    processService2.stop();
    clusterService2.stop();

    assertState("the jobs were reassigned",
      s -> s.assignments("node1") == 2 && s.assignments("node2") == 0);

    processService1.stop();
    clusterService1.stop();
  }

  @Test
  public void shouldUnassignWhenAllStopped() throws InterruptedException {
    clusterService1.start();
    processService1.start();
    clusterService2.start();
    processService2.start();

    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));

    assertState("the initial state was reached", s -> s.unassigned() == 0);

    processService1.stop();
    clusterService1.stop();
    processService2.stop();
    clusterService2.stop();

    assertState("all jobs were unassigned", s -> s.unassigned() == 2);
  }

  @Ignore
  @Test
  /**
   * There is a known bug with reusing a cluster assignment service.  Same code works with
   * new instances.
   * 
   * @throws InterruptedException
   */
  public void shouldBlueGreenDeploy() throws InterruptedException {
    @SuppressWarnings("unchecked")
    List<ProcessService<ObjectNode>> services = Lists.newArrayList(processService1, processService2, processService3);

    Function<Integer, ProcessService<ObjectNode>> serviceLookup = i -> {
      return services.get(i % services.size());
    };

    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));
    dao.put("id3", processNode(null, "name3"));
    dao.put("id4", processNode(null, "name4"));
    dao.put("id5", processNode(null, "name5"));

    ProcessService<ObjectNode> currentService = serviceLookup.apply(0);
    currentService.start();

    assertState("only green running", s -> s.assignments("node1") == 5);

    for (int i = 1; i < 10; i++) {
      ProcessService<ObjectNode> nextService = serviceLookup.apply(i);
      nextService.start();

      assertState("blue and green running", s -> s.maxAssignments() == 3 && s.minAssignments() == 2
        && s.unassigned() == 0);

      currentService.stop();

      assertState("blue now green", s -> s.assignments(nextService.getId()) == 5);

      currentService = nextService;
    }

    currentService.stop();

    assertState("all stopped", s -> s.unassigned() == 5);
  }

  @Test
  public void shouldBlueGreenDeployWithNewServices() throws InterruptedException {
    int processCount = 20;
    for (int i = 0; i < processCount; i++) {
      dao.put("id" + i, processNode(null, "name" + i));
    }

    int halfFloorCount = IntMath.divide(processCount, 2, RoundingMode.FLOOR);
    int halfCeilCount = IntMath.divide(processCount, 2, RoundingMode.CEILING);

    ClusterNode node0 = new ClusterNode().withId("node0").withStartedAt(new DateTime());
    
    MetricRegistry registry = new MetricRegistry();

    ClusterService currentClusterService =
      ClusterService.builder().withEtcdFactory(factoryRule.getFactory()).withExecutor(executor)
        .withNodesDirectory(nodeDir).withThisNode(node0).withMetricRegistry(registry).build();

    currentClusterService.start();
    
    ProcessService<ObjectNode> currentProcessService = currentClusterService.newProcessService(processDir, ClusterAssignmentIT::toLifecycle, new TypeReference<ObjectNode>(){});

    currentProcessService.start();

    assertState("only green running", s -> s.assignments("node0") == processCount);

    for (int i = 1; i < 4; i++) {
      ClusterNode nextNode = new ClusterNode().withId("node" + i).withStartedAt(new DateTime());
      
      MetricRegistry nextRegistry = new MetricRegistry();

      ClusterService nextClusterService =
        ClusterService.builder()
          .withEtcdFactory(factoryRule.getFactory())
          .withExecutor(executor)
          .withNodesDirectory(nodeDir)
          .withThisNode(nextNode)
          .withMetricRegistry(nextRegistry)
          .build();

      nextClusterService.start();

      ProcessService<ObjectNode> nextProcessService = nextClusterService.newProcessService(processDir, ClusterAssignmentIT::toLifecycle, new TypeReference<ObjectNode>(){});

      nextProcessService.start();

      assertState(
        "blue and green running",
        s -> s.maxAssignments() == halfCeilCount && s.minAssignments() == halfFloorCount
          && s.unassigned() == 0, s->s.unassigned() > 1);
      
      final Runnable stopProcess = currentProcessService::stop;
      final Runnable stopClusterService = currentClusterService::stop;

      executor.schedule(()->{
        stopProcess.run();
        stopClusterService.run();
      }, 1, TimeUnit.MILLISECONDS);

      assertState("blue now green",
        s -> s.assignments(nextProcessService.getId()) == processCount,
        s->s.unassigned() > 1);

      currentProcessService = nextProcessService;
      currentClusterService = nextClusterService;
    }

    currentProcessService.stop();
    currentClusterService.stop();

    assertState("all stopped", s -> s.unassigned() == processCount);
  }

  @Test
  public void shouldReassignWhenProcessStopped() throws InterruptedException {
    clusterService1.start();
    processService1.start();
    clusterService2.start();
    processService2.start();

    dao.put("id1", processNode(null, "name1"));

    assertState("the initial state was reached", s -> s.unassigned() == 0
      && s.totalAssignments() == 1);

    dao.put("id1", processNode(null, "name1"));

    assertState("the node was reassigned",
      s -> s.assignments(processService1.getId(), processService2.getId()) == 1 && s.unassigned() == 0);

    processService1.stop();
    clusterService1.stop();
    processService2.stop();
    clusterService2.stop();

    assertState("all jobs were unassigned", s -> s.unassigned() == 1 && s.totalAssignments() == 0);
  }

  @Test
  public void shouldRecoverCrashedProcesses() throws InterruptedException {
    clusterService1.start();

    dao.put("id1", processNode(node2.getId(), "name1"));

    assertState("the initial state was reached",
      s -> s.unassigned() == 0 && s.assignments(node2.getId()) == 1);

    processService1.start();

    assertState("the stranded process was recovered",
      s -> s.unassigned() == 0 && s.assignments(node1.getId()) == 1);

    processService1.stop();
    clusterService1.stop();

    assertState("all jobs were unassigned", s -> s.unassigned() == 1);

  }

  @Test
  public void shouldRecoverCrashedProcessor() throws InterruptedException {
    clusterService1.start();

    processorDao.put(node2.getId(), processorNode(node2.getId()));
    dao.put("id1", processNode(null, "name1"));
    dao.put("id2", processNode(null, "name2"));

    assertState("the initial state was reached",
      s -> s.unassigned() == 2);

    processService1.start();

    assertState("the stranded process was recovered",
      s -> s.unassigned() == 0 && s.assignments(node1.getId()) == 2);

    processService1.stop();
    clusterService1.stop();

    assertState("all jobs were unassigned", s -> s.unassigned() == 2);

  }
  
  public static Callable<Void> startCalls(ClusterService clusterService, ProcessService<?> processService, EtcdDirectoryDao<ClusterProcess> dao ) {
    return ()->{
      clusterService.start();
      processService.start();
      dao.put("id", processNode(null, "name"));
      return null;
    };
  }
  
  public static Callable<Void> stopCalls(ClusterService clusterService, ProcessService<?> processService) {
    return ()->{
      processService.stop();
      clusterService.stop();
      return null;
    };
  }
  
  public Callable<Void> assertAllAssigned( int count, String... nodeIds ) {
    return ()->{
      assertState("the process was assigned",
        s -> s.unassigned() == 0 && s.assignments(nodeIds) == count);
      return null;
    };
  }
  
  @Test public void shouldMigrateProcessBetweenNodes() throws Exception {
    Callable<Void> startC1 = startCalls(clusterService1, processService1, dao);
    Callable<Void> startC2 = startCalls(clusterService2, processService2, dao);
    Callable<Void> stopC1 = stopCalls(clusterService1, processService1);
    Callable<Void> stopC2 = stopCalls(clusterService2, processService2);
    Callable<Void> assertOn1 = assertAllAssigned(1, node1.getId());
    Callable<Void> assertOn2 = assertAllAssigned(1, node2.getId());
    Callable<Void> assertOnEither = assertAllAssigned(1, node1.getId(), node2.getId());
    
    startC1.call();
    assertOn1.call();
    
    for( int i = 0; i < 4; i++ ) {
      startC2.call();
      assertOnEither.call();
      stopC1.call();
      assertOn2.call();
      startC1.call();
      assertOnEither.call();
      stopC2.call();
      assertOn1.call();
    }
    stopC1.call();
 }
  
  @Test
  public void shouldMigrateProcessInThreeNodeCluster() throws Exception {
    Callable<Void> startC1 = ()->{
      startCalls(clusterService1, processService1, dao).call();
      startCalls(clusterService2, processService2, dao).call();
      startCalls(clusterService3, processService3, dao).call();
      return null;
    };
    Callable<Void> startC2 = ()->{
      startCalls(clusterService4, processService4, dao).call();
      startCalls(clusterService5, processService5, dao).call();
      startCalls(clusterService6, processService6, dao).call();
      return null;
    };
    Callable<Void> stopC1 = ()->{
      stopCalls(clusterService3, processService3).call();
      stopCalls(clusterService2, processService2).call();
      stopCalls(clusterService1, processService1).call();
      return null;
    };
    Callable<Void> stopC2 = ()->{
      stopCalls(clusterService6, processService6).call();
      stopCalls(clusterService5, processService5).call();
      stopCalls(clusterService4, processService4).call();
      return null;
    };
    Callable<Void> assertOn1 = assertAllAssigned(1, node1.getId(), node2.getId(), node3.getId());
    Callable<Void> assertOn2 = assertAllAssigned(1, node4.getId(), node5.getId(), node6.getId());
    Callable<Void> assertOnEither = assertAllAssigned(1, node1.getId(), node2.getId(), node3.getId(), node4.getId(), node5.getId(), node6.getId());

    startC1.call();
    assertOn1.call();
    
    for( int i = 0; i < 4; i++ ) {
      startC2.call();
      assertOnEither.call();
      stopC1.call();
      assertOn2.call();
      startC1.call();
      assertOnEither.call();
      stopC2.call();
      assertOn1.call();
    }
    stopC1.call();
  }

  @Test
  public void registerMetrics() {
    processService1.start();

    verify(listener1).onGaugeAdded(eq(MetricRegistry.name(ClusterAssignmentService.class, "streams", ClusterAssignmentTracker.TOTAL)), any());
    verify(listener1).onGaugeAdded(eq(MetricRegistry.name(ClusterAssignmentService.class, "streams", ClusterAssignmentTracker.ASSIGNED)), any());
    verify(listener1).onGaugeAdded(eq(MetricRegistry.name(ClusterAssignmentService.class, "streams", ClusterAssignmentTracker.UNASSIGNED)), any());
    verify(listener1).onMeterAdded(eq(MetricRegistry.name(ClusterAssignmentService.class, "streams", ClusterAssignmentService.ASSIGNMENT_FAILURES)), any());
    verify(listener1).onMeterAdded(eq(MetricRegistry.name(ClusterAssignmentService.class, "streams", ClusterAssignmentService.UNASSIGNMENT_FAILURES)), any());
    verify(listener1).onMeterAdded(eq(MetricRegistry.name(ClusterAssignmentService.class, "streams", ClusterAssignmentService.EXCEPTIONS)), any());
    verify(listener1).onMeterAdded(eq(MetricRegistry.name(ClusterAssignmentService.class, "streams", ClusterAssignmentService.ASSIGNMENT_TASK)), any());
    verify(listener1).onMeterAdded(eq(MetricRegistry.name(ClusterAssignmentService.class, "streams", ClusterAssignmentService.CLEAN_UP_TASK)), any());  
  }

  public static ClusterProcess processNode(String assignedTo, String name) {
    return new ClusterProcess().withAssignedTo(assignedTo).withConfiguration(nodeData(name));
  }

  private static ProcessorNode processorNode( String id ) {
    return new ProcessorNode().withId(id).withStartedAt(new DateTime());
  }

  public static ObjectNode nodeData(String name) {
    return JsonNodeFactory.instance.objectNode().put("name", name);
  }

  public static class NodeData {
    protected String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public NodeData withName(String name) {
      this.name = name;
      return this;
    }

    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }

    public boolean equals(Object o) {
      return EqualsBuilder.reflectionEquals(this, o);
    }
  }

  public static class AssignmentLatchFactory {
    private WatchService service;
    private String directory;

    public AssignmentLatchFactory(WatchService service, String directory) {
      this.service = service;
      this.directory = directory;
    }

    public AssignmentLatch newLatch(String name, Predicate<AssignmentState> test) {
      return new AssignmentLatch(service, directory, test, s->false, name);
    }

    public AssignmentLatch newLatch(String name, Predicate<AssignmentState> test, Predicate<AssignmentState> illegalStateTest) {
      return new AssignmentLatch(service, directory, test, illegalStateTest, name);
    }
 }

  public static class AssignmentState {
    public static String UNASSIGNED = "unassigned";
    Map<String, Integer> assignedCount = Maps.newConcurrentMap();

    public void assign(String key) {
      assignedCount.compute(Optional.ofNullable(key).orElse(UNASSIGNED), (k, count) -> {
        return count == null ? 1 : count + 1;
      });
    }

    public int minAssignments() {
      return assignedCount.entrySet().stream().filter(e -> !UNASSIGNED.equals(e.getKey()))
        .mapToInt(Map.Entry::getValue).min().orElse(0);
    }

    public int maxAssignments() {
      return assignedCount.entrySet().stream().filter(e -> !UNASSIGNED.equals(e.getKey()))
        .mapToInt(Map.Entry::getValue).max().orElse(0);
    }

    public void unassign(String key) {
      assignedCount.compute(Optional.ofNullable(key).orElse(UNASSIGNED), (k, count) -> {
        if (count == null) {
          throw new IllegalStateException("node unassigned when not assigned.");
        }
        if (count < 1) {
          throw new IllegalStateException("node count less than zero.");
        }
        return count == 1 ? null : count - 1;
      });
    }

    public int totalAssignments() {
      return assignedCount.entrySet().stream().filter(e -> !UNASSIGNED.equals(e.getKey()))
        .mapToInt(Map.Entry::getValue).sum();
    }

    public int unassigned() {
      return assignedCount.getOrDefault(UNASSIGNED, 0);
    }

    public int assignments(String... keys) {
      Set<String> keySet = Sets.newHashSet(keys);
      return assignedCount.entrySet().stream().filter(e -> keySet.contains(e.getKey()))
        .mapToInt(Map.Entry::getValue).sum();
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");
      assignedCount.entrySet().stream().forEach(e -> {
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
    Predicate<AssignmentState> illegalStateTest;
    CountDownLatch latch;
    private String name;
    volatile boolean illegalState = false;

    public AssignmentLatch(WatchService service, String directory, Predicate<AssignmentState> test, Predicate<AssignmentState> illegalStateTest,
      String name) {
      this.service = service;
      this.directory = directory;
      this.test = test;
      this.illegalStateTest = illegalStateTest;
      this.name = name;
      this.latch = new CountDownLatch(1);
    }

    public void handle(EtcdEvent<ClusterProcess> event) {
      if (latch.getCount() != 0) {
        switch (event.getType()) {
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
          if( illegalStateTest.test(state)) {
            logger.debug("{} in illegalState {}", name, state);
            illegalState = true;
            latch.countDown();
          }
          if (test.test(state)) {
            logger.debug("{} did match {}", name, state);
            latch.countDown();
          } else {
            logger.debug("{} did not match {}", name, state);
          }
        } catch (Exception e) {
          logger.warn("bad latch predicate " + name, e);
        }
      }
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
      WatchService.Watch watch =
        service.registerDirectoryWatch(directory, new TypeReference<ClusterProcess>() {
        }, this::handle);
      try {
        boolean reached = latch.await(timeout, unit);
        if( !reached ) return false;
        if( reached && illegalState ) throw new IllegalStateException(name+" reached an illegal state.");
        return reached;
      } finally {
        watch.stop();
      }
    }
  }
}
