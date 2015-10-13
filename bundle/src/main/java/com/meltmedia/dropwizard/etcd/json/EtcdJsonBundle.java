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

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;

import mousio.etcd4j.EtcdClient;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class EtcdJsonBundle<C extends Configuration> implements ConfiguredBundle<C> {
  public static class Builder<C extends Configuration> {
    private Supplier<EtcdClient> client;
    private Supplier<ScheduledExecutorService> executor;
    private Function<C, String> directoryAccessor;

    public Builder<C> withClient(Supplier<EtcdClient> client) {
      this.client = client;
      return this;
    }

    public Builder<C> withExecutor(Supplier<ScheduledExecutorService> executor) {
      this.executor = executor;
      return this;
    }

    public Builder<C> withDirectory(Function<C, String> directoryAccessor) {
      this.directoryAccessor = directoryAccessor;
      return this;
    }

    public EtcdJsonBundle<C> build() {
      return new EtcdJsonBundle<C>(client, executor, directoryAccessor);
    }
  }

  public static <C extends Configuration> Builder<C> builder() {
    return new Builder<C>();
  }

  Supplier<EtcdClient> clientSupplier;
  EtcdJson factory;
  private Supplier<ScheduledExecutorService> executor;
  private Function<C, String> directoryAccessor;

  public EtcdJsonBundle(Supplier<EtcdClient> client, Supplier<ScheduledExecutorService> executor,
    Function<C, String> directoryAccessor) {
    this.clientSupplier = client;
    this.executor = executor;
    this.directoryAccessor = directoryAccessor;
  }

  @Override
  public void initialize(Bootstrap<?> bootstrap) {
  }

  @Override
  public void run(C configuration, Environment environment) throws Exception {
    factory =
      EtcdJson.builder().withClient(clientSupplier).withExecutor(executor.get())
        .withBaseDirectory(directoryAccessor.apply(configuration))
        .withMapper(environment.getObjectMapper())
        .withMetricRegistry(environment.metrics()).build();
    environment.lifecycle().manage(new EtcdJsonManager(factory));
    environment.healthChecks().register("etcd-watch", new WatchServiceHealthCheck(factory.getWatchService()));
  }

  public EtcdJson getFactory() {
    return this.factory;
  }
}
