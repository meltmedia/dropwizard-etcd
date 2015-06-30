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

import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.meltmedia.dropwizard.etcd.EtcdConfiguration;

public class EtcdExampleConfiguration extends Configuration {
  @NotNull
  EtcdConfiguration etcd = new EtcdConfiguration();
  
  @NotEmpty
  String etcdDirectory;

  public EtcdConfiguration getEtcd() {
    return etcd;
  }

  public void setEtcd( EtcdConfiguration etcd ) {
    this.etcd = etcd;
  }

  public String getEtcdDirectory() {
    return etcdDirectory;
  }

  public void setEtcdDirectory( String etcdDirectory ) {
    this.etcdDirectory = etcdDirectory;
  }
}