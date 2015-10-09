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
package com.meltmedia.dropwizard.etcd.json;

import static com.meltmedia.dropwizard.etcd.json.EtcdMatchers.anyEtcdEventOfType;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdEventHandler;
import com.meltmedia.dropwizard.etcd.json.EtcdJson;
import com.meltmedia.dropwizard.etcd.json.Heartbeat;
import com.meltmedia.dropwizard.etcd.json.WatchService;
import com.meltmedia.dropwizard.etcd.junit.EtcdClientRule;
import com.meltmedia.dropwizard.etcd.junit.EtcdJsonRule;

public class KeyHeartbeatsIT {
  @ClassRule
  public static EtcdClientRule etcdClientSupplier = new EtcdClientRule("http://127.0.0.1:2379");

  @Rule
  public EtcdJsonRule factoryRule = new EtcdJsonRule(etcdClientSupplier::getClient, "/test");

  Heartbeat<NodeData> heartbeats;
  EtcdEventHandler<NodeData> handler;
  WatchService.Watch watch;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    handler = mock(EtcdEventHandler.class);

    EtcdJson.MappedEtcdDirectory<NodeData> directory =
      factoryRule.getFactory().newDirectory("/nodes", new TypeReference<NodeData>() {
      });

    heartbeats = directory.newHeartbeat("/id", new NodeData().withName("id"), 1);

    watch = directory.registerWatch(handler);
  }

  @After
  public void tearDown() throws Exception {
    watch.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldKeepRecordActive() throws InterruptedException {
    verify(handler, never()).handle(any(EtcdEvent.class));

    heartbeats.start();

    verify(handler, timeout(1000).times(1)).handle(anyEtcdEventOfType(EtcdEvent.Type.added));
    verify(handler, never()).handle(anyEtcdEventOfType(EtcdEvent.Type.removed));

    heartbeats.stop();
  }

  @Test
  public void shouldRemoveRecordAfterStop() {
    verify(handler, never()).handle(anyEtcdEventOfType(EtcdEvent.Type.added));

    heartbeats.start();

    verify(handler, timeout(2000).times(1)).handle(anyEtcdEventOfType(EtcdEvent.Type.added));

    heartbeats.stop();

    verify(handler, timeout(1000).times(1)).handle(anyEtcdEventOfType(EtcdEvent.Type.removed));
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
