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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.meltmedia.dropwizard.etcd.EtcdClientRule;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryDao;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;
import com.meltmedia.dropwizard.etcd.json.KeyNotFound;

public class EtcdDirectoryIT {
  @ClassRule
  public static EtcdClientRule clientRule = new EtcdClientRule("http://127.0.0.1:2379");

  @Rule
  public EtcdJsonRule factoryRule = new EtcdJsonRule(clientRule::getClient, "/cluster-test");

  MappedEtcdDirectory<NodeData> directory;
  private EtcdDirectoryDao<NodeData> dao;

  @Before
  public void setUp() {
    directory = factoryRule.getFactory().newDirectory("/test/data", new TypeReference<NodeData>() {
    });

    dao = directory.newDao();
  }

  public void tearDown() {
    dao.resetDirectory();
  }

  @Test(expected = KeyNotFound.class)
  public void shouldThrowKeyNotFound() {
    dao.get("undefined");
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
