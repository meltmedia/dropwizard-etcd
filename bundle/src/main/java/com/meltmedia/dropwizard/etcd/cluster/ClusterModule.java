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

import java.util.function.Supplier;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ClusterModule extends AbstractModule {

  private ClusterBundle<? extends Configuration> bundle;

  public ClusterModule(ClusterBundle<? extends Configuration> bundle) {
    this.bundle = bundle;
  }

  @Override
  protected void configure() {
  }

  @Singleton
  @Provides
  public Supplier<ClusterService> provideClusterServiceSupplier() {
    return bundle::getService;
  }

}
