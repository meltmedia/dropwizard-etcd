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
package com.meltmedia.dropwizard.etcd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeoutException;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.responses.EtcdException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EtcdIT {
  EtcdClient client;
  @Before
  public void setUp() throws Exception {
    client = new EtcdClient(new URI[] {URI.create("http://127.0.0.1:2379")});
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testRoundTripKey() throws IOException, EtcdException, TimeoutException {
    client.put("/collectors/1", "{id: 1}").send();
    assertThat(client.get("/collectors/1").send().get().node.value, equalTo("{id: 1}"));
  }

}
