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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import mousio.etcd4j.EtcdClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Heartbeat<T> {
  private static final Logger logger = LoggerFactory.getLogger(Heartbeat.class);
  
  public static class Builder<T> {
    private String key;
    private Supplier<EtcdClient> client;
    private ScheduledExecutorService executor;
    private ObjectMapper mapper;
    private T value;
    private Integer ttl;
    
    public Builder<T> withKey( String key ) {
      this.key = key;
      return this;
    }
    
    public Builder<T> withClient( Supplier<EtcdClient> client ) {
      this.client = client;
      return this;
    }
    
    public Builder<T> withMapper( ObjectMapper mapper ) {
      this.mapper = mapper;
      return this;
    }
    
    public Builder<T> withExecutor( ScheduledExecutorService executor ) {
      this.executor = executor;
      return this;
    }
    
    public Builder<T> withValue( T value ) {
      this.value = value;
      return this;
    }
    
    public Builder<T> withTtl( Integer ttl ) {
      this.ttl = ttl;
      return this;
    }
    
    public Heartbeat<T> build() {
      return new Heartbeat<T>(executor, client, mapper, key, value, ttl);
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<T>();
  }

  private String key;
  private Supplier<EtcdClient> client;
  private ObjectMapper mapper;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> heartbeatFuture;
  private T value;
  private Integer ttl;
  
  private Heartbeat( ScheduledExecutorService executor, Supplier<EtcdClient> client, ObjectMapper mapper, String key, T value, Integer ttl ) {
    this.executor = executor;
    this.client = client;
    this.mapper = mapper;
    this.key = key;
    this.value = value;
    this.ttl = ttl;
  }

  public void start() {
    logger.info("starting heartbeats for {}", key);
    heartbeatFuture = executor.scheduleAtFixedRate(()->{
      try {
        logger.debug("sending heartbeat for {}", key);
        client.get().put(key, mapper.writeValueAsString(value))
          .ttl(ttl)
          .send()
          .get();
      } catch( Exception e ) {
        logger.error(String.format("could not heartbeat node %s in etcd", key), e);
      }
    }, 1, ttl*500, TimeUnit.MILLISECONDS);
  }
  
  public void stop() {
    heartbeatFuture.cancel(true);
    try {
      client.get().delete(key).send().get();
    } catch( Exception e ) {
      logger.error(String.format("could not delete node %s from etcd", key), e);
    }
  }
}
