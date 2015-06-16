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

import io.dropwizard.lifecycle.Managed;

public class EtcdJsonManager implements Managed {

  EtcdJson factory;

  public EtcdJsonManager( EtcdJson factory ) {
    this.factory = factory;
  }

  @Override
  public void start() throws Exception {
    factory.start();
  }

  @Override
  public void stop() throws Exception {
    factory.stop();
  }

}
