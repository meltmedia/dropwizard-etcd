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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import mousio.etcd4j.EtcdClient;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.meltmedia.dropwizard.etcd.json.EtcdJson;

public class EtcdJsonRule implements TestRule {

  private Supplier<EtcdClient> clientSupplier;
  private String directory;
  private EtcdJson factory;

  public EtcdJsonRule(Supplier<EtcdClient> clientSupplier, String directory) {
    this.clientSupplier = clientSupplier;
    this.directory = directory;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
        ObjectMapper mapper = new ObjectMapper().registerModule(new JodaModule());

        try {
          try {
            clientSupplier.get().deleteDir(directory).recursive().send().get();
          } catch (Exception e) {
            System.out.printf("could not delete %s from service rule", directory);
            e.printStackTrace();
          }

          factory =
            EtcdJson.builder().withClient(clientSupplier).withBaseDirectory(directory)
              .withExecutor(executor).withMapper(mapper).withMetricRegistry(new MetricRegistry()).build();

          factory.start();

          try {

            base.evaluate();
          } finally {
            try {
              factory.stop();
            } catch (Throwable ioe) {
              ioe.printStackTrace(System.err);
            }
            factory = null;
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        } finally {
          executor.shutdown();
        }

      }
    };
  }

  public EtcdJson getFactory() {
    return factory;
  }

}
