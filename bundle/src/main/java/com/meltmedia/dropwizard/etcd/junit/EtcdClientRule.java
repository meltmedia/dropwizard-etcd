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
package com.meltmedia.dropwizard.etcd.junit;

import java.io.IOException;
import java.net.URI;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.transport.EtcdNettyClient;
import mousio.etcd4j.transport.EtcdNettyConfig;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class EtcdClientRule implements TestRule {

  String uri;
  EtcdClient client;

  public EtcdClientRule(String uri) {
    this.uri = uri;
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        URI serverUri = URI.create(uri);
        EtcdNettyConfig config = new EtcdNettyConfig().setHostName(serverUri.getHost());

        client = new EtcdClient(new EtcdNettyClient(config, null, new URI[] { serverUri }));

        try {
          base.evaluate();
        } finally {
          if (client != null) {
            try {
              client.close();
            } catch (IOException ioe) {
              ioe.printStackTrace(System.err);
            }
          }
          client = null;
        }
      }
    };
  }

  public EtcdClient getClient() {
    return client;
  }
}
