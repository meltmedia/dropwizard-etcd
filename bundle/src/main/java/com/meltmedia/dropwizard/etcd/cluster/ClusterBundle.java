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

import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.joda.time.DateTime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.meltmedia.dropwizard.etcd.json.EtcdJson;

public class ClusterBundle<C extends Configuration> implements ConfiguredBundle<C> {
  public static class Builder<C extends Configuration> {
    Supplier<EtcdJson> factorySupplier;
    Supplier<ScheduledExecutorService> executorSupplier;

    public Builder<C> withFactorySupplier(Supplier<EtcdJson> factorySupplier) {
      this.factorySupplier = factorySupplier;
      return this;
    }

    public Builder<C> withExecutorSupplier(Supplier<ScheduledExecutorService> executorSupplier) {
      this.executorSupplier = executorSupplier;
      return this;
    }

    public ClusterBundle<C> build() {
      return new ClusterBundle<C>(factorySupplier, executorSupplier);
    }
  }

  public static <C extends Configuration> Builder<C> builder() {
    return new Builder<C>();
  }

  Supplier<EtcdJson> factorySupplier;
  ClusterService service;
  Supplier<ScheduledExecutorService> executorSupplier;

  public ClusterBundle(Supplier<EtcdJson> factorySupplier,
    Supplier<ScheduledExecutorService> executorSupplier) {
    this.factorySupplier = factorySupplier;
    this.executorSupplier = executorSupplier;
  }

  @Override
  public void initialize(Bootstrap<?> bootstrap) {

  }

  @Override
  public void run(C configuration, Environment environment) {
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        service =
          ClusterService
            .builder()
            .withExecutor(executorSupplier.get())
            .withEtcdFactory(factorySupplier.get())
            .withNodesDirectory(
              factorySupplier.get().newDirectory("/nodes", new TypeReference<ClusterNode>() {
              }))
            .withThisNode(
              new ClusterNode().withId(UUID.randomUUID().toString()).withStartedAt(new DateTime()))
            .build();
        service.start();
      }

      @Override
      public void stop() throws Exception {
        service.stop();
        service = null;
      }
    });
  }

  public ClusterService getService() {
    return service;
  }

}
