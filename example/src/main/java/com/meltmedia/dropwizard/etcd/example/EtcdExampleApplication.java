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
package com.meltmedia.dropwizard.etcd.example;

import io.dropwizard.Application;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.hubspot.dropwizard.guice.GuiceBundle;
import com.meltmedia.dropwizard.etcd.EtcdBundle;
import com.meltmedia.dropwizard.etcd.EtcdModule;
import com.meltmedia.dropwizard.etcd.cluster.ClusterBundle;
import com.meltmedia.dropwizard.etcd.cluster.ClusterModule;
import com.meltmedia.dropwizard.etcd.example.resource.ClusterResource;
import com.meltmedia.dropwizard.etcd.json.EtcdJsonBundle;
import com.meltmedia.dropwizard.etcd.json.EtcdJsonModule;

/**
 * etcd-example entry point
 */
public class EtcdExampleApplication extends Application<EtcdExampleConfiguration> {

  public static final String COMMAND_NAME = "etcd-example";

  public static void main(String[] args) throws Exception {
    new EtcdExampleApplication().run(args);
  }

  protected EtcdBundle<EtcdExampleConfiguration> etcdBundle;
  protected EtcdJsonBundle<EtcdExampleConfiguration> etcdJsonBundle;
  protected ClusterBundle<EtcdExampleConfiguration> etcdClusterBundle;
  protected GuiceBundle<EtcdExampleConfiguration> guiceBundle;
  protected ScheduledExecutorService executor;

  public EtcdExampleApplication() {
  }

  @Override
  public String getName() {
    return COMMAND_NAME;
  }

  @Override
  public void initialize(Bootstrap<EtcdExampleConfiguration> bootstrap) {
    executor = Executors.newScheduledThreadPool(10);

    // provides a client to the application.
    bootstrap.addBundle(etcdBundle =
        EtcdBundle.<EtcdExampleConfiguration> builder()
            .withConfiguration(EtcdExampleConfiguration::getEtcd).build());

    // provides access to Etcd as a JSON store using Jackson.
    // requires the client bundle to operate.
    bootstrap.addBundle(etcdJsonBundle =
        EtcdJsonBundle.<EtcdExampleConfiguration> builder().withClient(etcdBundle::getClient)
            .withDirectory(EtcdExampleConfiguration::getEtcdDirectory).withExecutor(() -> executor)
            .build());

    // provides services for clustering jobs with Etcd.
    // requires the JSON bundle to operate.
    bootstrap.addBundle(etcdClusterBundle =
        ClusterBundle.<EtcdExampleConfiguration> builder().withExecutorSupplier(() -> executor)
            .withFactorySupplier(etcdJsonBundle::getFactory).build());

    GuiceBundle.Builder<EtcdExampleConfiguration> builder =
        GuiceBundle.<EtcdExampleConfiguration> newBuilder()
            .setConfigClass(EtcdExampleConfiguration.class)
            .enableAutoConfig(getClass().getPackage().getName());

    // these Guice modules provide injections for the etcd bundles.
    builder.addModule(new EtcdModule(etcdBundle));
    builder.addModule(new EtcdJsonModule(etcdJsonBundle));
    builder.addModule(new ClusterModule(etcdClusterBundle));

    builder.addModule(new EtcdExampleModule());

    bootstrap.addBundle(guiceBundle = builder.build(Stage.DEVELOPMENT));

    bootstrap.addCommand(new Commands.AddCommand());
    bootstrap.addCommand(new Commands.RemoveCommand());
    bootstrap.addCommand(new Commands.ListCommand());
  }

  @Override
  public void run(EtcdExampleConfiguration configuration, Environment environment) throws Exception {
    configureMapper(environment.getObjectMapper());
    environment.lifecycle().manage(
        new ExecutorServiceManager(executor, Duration.milliseconds(100), "etcd-threads"));
    Injector injector = guiceBundle.getInjector();
    environment.jersey().register(injector.getInstance(ClusterResource.class));
  }

  public void configureMapper(ObjectMapper mapper) {
    mapper.registerModule(new JodaModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(SerializationFeature.INDENT_OUTPUT, true);

  }
}
