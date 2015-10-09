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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meltmedia.dropwizard.etcd.cluster.ClusterProcessLifecycle;
import com.meltmedia.dropwizard.etcd.cluster.ClusterProcessor;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryDao;
import com.meltmedia.dropwizard.etcd.junit.EtcdClientRule;
import com.meltmedia.dropwizard.etcd.junit.EtcdJsonRule;

public class ClusterProcessorIT {
  @ClassRule
  public static EtcdClientRule etcdClientSupplier = new EtcdClientRule("http://127.0.0.1:2379");

  @Rule
  public EtcdJsonRule factoryRule = new EtcdJsonRule(etcdClientSupplier::getClient, "/test");

  ClusterProcessor<NodeData> service;
  ScheduledExecutorService executor;
  EtcdDirectoryDao<ClusterProcess> dao;
  ClusterProcessLifecycle lifecycle;

  @Before
  public void setUp() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    executor = Executors.newScheduledThreadPool(4, Executors.defaultThreadFactory());
    lifecycle = mock(ClusterProcessLifecycle.class);

    service =
      ClusterProcessor
        .<NodeData> builder()
        .withMapper(mapper)
        .withDirectory(
          factoryRule.getFactory().newDirectory("/jobs", new TypeReference<ClusterProcess>() {
          })).withLifecycleFactory(d -> lifecycle).withNodeId("1")
        .withType(new TypeReference<NodeData>() {
        }).build();

    dao =
      new EtcdDirectoryDao<ClusterProcess>(etcdClientSupplier::getClient, "/test/jobs", mapper,
        new TypeReference<ClusterProcess>() {
        });

    service.start();
  }

  @After
  public void tearDown() throws Exception {
    service.stop();
  }

  @Test
  public void shouldRunProcessLifecycle() {
    dao.put("id", processNode("1", "name"));

    verify(lifecycle, timeout(1000).times(1)).start();

    dao.remove("id");

    verify(lifecycle, timeout(1000).times(1)).stop();
  }

  @Test
  public void shouldStopWhenProcessReassignedToAnotherNode() {
    dao.put("id", processNode("1", "name"));

    verify(lifecycle, timeout(1000).times(1)).start();

    dao.put("id", processNode("2", "name"));

    verify(lifecycle, timeout(1000).times(1)).stop();
  }

  @Test
  public void shouldStartWhenReassignedToThisNode() {
    dao.put("id", processNode("2", "name"));

    verify(lifecycle, never()).start();

    dao.put("id", processNode("1", "name"));

    verify(lifecycle, timeout(1000).times(1)).start();
  }

  @Test
  public void shouldStopProcessesWhenServiceStops() {
    dao.put("id", processNode("1", "name"));

    verify(lifecycle, timeout(1000).times(1)).start();

    service.stop();

    verify(lifecycle, timeout(1000).times(1)).stop();
  }

  public static ClusterProcess processNode(String assignedTo, String name) {
    return new ClusterProcess().withAssignedTo(assignedTo).withConfiguration(nodeData(name));
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

}
