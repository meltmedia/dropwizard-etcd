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

import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.net.URI;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.transport.EtcdNettyClient;
import mousio.etcd4j.transport.EtcdNettyConfig;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.health.HealthCheck;

public class EtcdBundle<C extends Configuration> implements ConfiguredBundle<C> {
  
  public static Logger log = LoggerFactory.getLogger(EtcdBundle.class);
  
  public static interface ConfigurationAccessor<C extends Configuration> {
    public EtcdConfiguration configuration(C configuration);
  }
  
  public static class Builder<C extends Configuration> {
    ConfigurationAccessor<C> configurationAccessor;
    
    public Builder<C> withConfiguration( ConfigurationAccessor<C> configurationAccessor ) {
      this.configurationAccessor = configurationAccessor;
      return this;
    }
    
    public EtcdBundle<C> build() {
      if( configurationAccessor == null ) {
        throw new IllegalArgumentException("The configuration accessor is required.");
      }
      return new EtcdBundle<C>(configurationAccessor);
    }
  }
  
  public static <C extends Configuration> Builder<C> builder() {
    return new Builder<C>();
  }

  ConfigurationAccessor<C> configurationAccessor;
  EtcdConfiguration bundleConfig;
  EtcdClient client;

  EtcdBundle( ConfigurationAccessor<C> configurationAccessor ) {
    this.configurationAccessor = configurationAccessor;
  }

  @Override
  public void run( C configuration, Environment environment ) throws Exception {
    bundleConfig = configurationAccessor.configuration(configuration);
    if( bundleConfig.getUrls() == null ) bundleConfig.setUrls(Lists.newArrayList());
    if( bundleConfig.getUrls().isEmpty() ) bundleConfig.getUrls().add(URI.create("http://127.0.0.1:2379"));
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        EtcdNettyConfig config = new EtcdNettyConfig()
          .setHostName(bundleConfig.getHostName());
        client =  new EtcdClient(new EtcdNettyClient(config, null, bundleConfig.getUrls().toArray(new URI[]{})));
        log.info("connected to etcd, version {}", client.getVersion());
      }

      @Override
      public void stop() throws Exception {
        client.close();
      }
    });
    environment.healthChecks().register("etcd", new HealthCheck() {
      @Override
      protected Result check() throws Exception {
        try {
          return Result.healthy("Connected to Etcd version %s", client.getVersion());
        }
        catch( Exception e ) {
          return Result.unhealthy(e);
        }
      }
    });
  }

  @Override
  public void initialize( Bootstrap<?> bootstrap ) {
    // TODO Auto-generated method stuff 
  }

  public EtcdClient getClient() {
    return client;
  }
  
  public EtcdConfiguration getConfiguration() {
    return bundleConfig;
  }
}
