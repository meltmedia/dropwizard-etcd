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

import io.dropwizard.lifecycle.Managed;

import java.util.function.Supplier;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.meltmedia.dropwizard.etcd.cluster.ClusterProcessLifecycle;
import com.meltmedia.dropwizard.etcd.cluster.ClusterService;

/**
 * A manager for individual hello processes.  This class is responsible for creating
 * processors for each configuration that has been assigned to this node.
 * 
 * @author Christian Trimble
 */
@Singleton
public class HelloProcessor implements Managed {
  private static Logger logger = LoggerFactory.getLogger(HelloProcessor.class);
  public static final String HELLO_DIRECTORY = "/hello";

  @Inject
  Supplier<ClusterService> service;

  volatile ClusterService.ProcessService<HelloProcessConfig> processService;

  @Override
  public void start() throws Exception {
    // This line wires up the processing and creates a service for managing processes.
    processService =
        service.get().newProcessService(HELLO_DIRECTORY, HelloProcess::new,
            new TypeReference<HelloProcessConfig>() {
            });

    processService.start();
  }

  @Override
  public void stop() throws Exception {
    processService.stop();
  }

  public ClusterService.ProcessService<HelloProcessConfig> getProcessService() {
    return processService;
  }

  /**
   * An implementation of a hello process.  Says hello and goodbye to people as they are
   * assigned and unassigned from this node.
   * 
   * @author Christian Trimble
   */
  public class HelloProcess implements ClusterProcessLifecycle {
    HelloProcessConfig config;

    public HelloProcess(HelloProcessConfig config) {
      this.config = config;
    }

    @Override
    public void start() {
      logger.info("Hello {}!", config.getName());
    }

    @Override
    public void stop() {
      logger.info("Goodbye {}!", config.getName());
    }
  }
}
