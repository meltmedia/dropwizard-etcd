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
import com.meltmedia.dropwizard.etcd.json.WatchService;

public class EtcdWatchServiceRule implements TestRule {

  private Supplier<EtcdClient> clientSupplier;
  private String directory;
  private WatchService service;

  public EtcdWatchServiceRule(Supplier<EtcdClient> clientSupplier, String directory) {
    this.clientSupplier = clientSupplier;
    this.directory = directory;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ObjectMapper mapper = new ObjectMapper();

        try {
          try {
            clientSupplier.get().deleteDir(directory).recursive().send().get();
          } catch (Exception e) {
            System.out.printf("could not delete %s from service rule", directory);
            e.printStackTrace();
          }

          service =
            WatchService.builder().withEtcdClient(clientSupplier).withDirectory(directory)
              .withExecutor(executor).withMapper(mapper).withMetricRegistry(new MetricRegistry()).build();

          service.start();

          try {

            base.evaluate();
          } finally {
            try {
              service.stop();
            } catch (Throwable ioe) {
              ioe.printStackTrace(System.err);
            }
            service = null;
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

  public WatchService getService() {
    return service;
  }

}
