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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryDao;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;
import com.meltmedia.dropwizard.etcd.json.EtcdEventHandler;
import com.meltmedia.dropwizard.etcd.json.WatchService;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent.Type;
import com.meltmedia.dropwizard.etcd.json.WatchService.Watch;
import com.meltmedia.dropwizard.etcd.junit.EtcdClientRule;

import static org.mockito.Mockito.*;
import static com.meltmedia.dropwizard.etcd.json.EtcdMatchers.*;

public class EtcdWatchServiceIT {
  public static final String BASE_PATH = "/talu-twitter-streams/it";
  public static final String EXTERNAL_NOISE_BASE_PATH = "/talu-twitter-streams/noise";
  @ClassRule
  public static EtcdClientRule clientRule = new EtcdClientRule("http://127.0.0.1:2379");
  @Rule
  public EtcdWatchServiceRule serviceRule = new EtcdWatchServiceRule(clientRule::getClient,
    BASE_PATH);
  public static TypeReference<NodeData> NODE_DATA_TYPE = new TypeReference<NodeData>() {
  };
  public static TypeReference<NoiseDocument> NOISE_DATA_TYPE = new TypeReference<NoiseDocument>() {};

  public EtcdDirectoryDao<NodeData> jobsDao;
  public EtcdDirectoryDao<NoiseDocument> externalNoiseDao;
  public ObjectMapper mapper;

  @Before
  public void setUp() {
    mapper = new ObjectMapper();

    jobsDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/jobs", mapper,
        NODE_DATA_TYPE);
    
    externalNoiseDao = 
      new EtcdDirectoryDao<NoiseDocument>(clientRule::getClient, EXTERNAL_NOISE_BASE_PATH, mapper,
        NOISE_DATA_TYPE);

  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldJoinExistingWatch() {
    // add a directory watch.
    WatchService service = serviceRule.getService();

    EtcdEventHandler<NodeData> handler = mock(EtcdEventHandler.class);

    service.registerDirectoryWatch("/jobs", new TypeReference<NodeData>() {
    }, handler);

    // add a document to the directory.
    jobsDao.put("id", new NodeData().withName("id"));

    // verify that we got an event.
    verify(handler, timeout(1000).times(1)).handle(any(EtcdEvent.class));
  }

  @Test
  public void shouldJoinExistingWatchWithLotsOfEvents() throws InterruptedException {
    int eventsCount = 1000;
    // add a directory watch.
    WatchService service = serviceRule.getService();

    @SuppressWarnings("unchecked")
    EtcdEventHandler<NodeData> handler = mock(EtcdEventHandler.class);

    Thread events = startNodeDataThread(jobsDao, eventsCount);

    try {
      service.registerDirectoryWatch("/jobs", new TypeReference<NodeData>() {
      }, handler);

      verifySequentialNodeData(handler, eventsCount);
    } finally {
      events.join();
    }
  }

  @Test
  public void shouldWatchMultipleDirectories() throws InterruptedException {
    int dir1Count = 1000;
    int dir2Count = 500;
    int dir3Count = 750;

    // add a directory watch.
    WatchService service = serviceRule.getService();

    EtcdDirectoryDao<NodeData> dir1Dao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir1", mapper,
        NODE_DATA_TYPE);
    EtcdDirectoryDao<NodeData> dir2Dao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir2", mapper,
        NODE_DATA_TYPE);
    EtcdDirectoryDao<NodeData> dir3Dao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir3", mapper,
        NODE_DATA_TYPE);

    @SuppressWarnings("unchecked")
    EtcdEventHandler<NodeData> handler1 = mock(EtcdEventHandler.class);
    @SuppressWarnings("unchecked")
    EtcdEventHandler<NodeData> handler2 = mock(EtcdEventHandler.class);

    Thread dir1Events = startNodeDataThread(dir1Dao, dir1Count);
    Thread dir2Events = startNodeDataThread(dir2Dao, dir2Count);
    Thread dir3Events = startNodeDataThread(dir3Dao, dir3Count);

    try {
      service.registerDirectoryWatch("/dir1", new TypeReference<NodeData>() {
      }, handler1);
      Thread.sleep(10);
      service.registerDirectoryWatch("/dir2", new TypeReference<NodeData>() {
      }, handler2);

      verifySequentialNodeData(handler1, dir1Count);
      verifySequentialNodeData(handler2, dir2Count);
    } finally {
      dir1Events.join();
      dir2Events.join();
      dir3Events.join();
    }
  }

  @Test
  public void shouldHandleNoiseInSimilarPaths() throws InterruptedException {
    int eventsCount = 100;
    // add a directory watch.
    WatchService service = serviceRule.getService();

    @SuppressWarnings("unchecked")
    EtcdEventHandler<NodeData> handler = mock(EtcdEventHandler.class);

    EtcdDirectoryDao<NodeData> dirDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir", mapper,
        NODE_DATA_TYPE);
    EtcdDirectoryDao<NoiseDocument> noiseDao =
      new EtcdDirectoryDao<NoiseDocument>(clientRule::getClient, BASE_PATH + "/directory", mapper,
        new TypeReference<NoiseDocument>() {
        });

    Thread events = startNodeDataThread(dirDao, eventsCount);
    Thread noise = startNoiseThread(noiseDao, eventsCount);

    try {
      service.registerDirectoryWatch("/dir", new TypeReference<NodeData>() {
      }, handler);

      verifySequentialNodeData(handler, eventsCount);
    } finally {
      events.join();
      noise.join();
    }
  }

  @Test
  public void shouldHandleNoiseInSubPaths() throws InterruptedException {
    int eventsCount = 100;
    // add a directory watch.
    WatchService service = serviceRule.getService();

    @SuppressWarnings("unchecked")
    EtcdEventHandler<NodeData> handler = mock(EtcdEventHandler.class);

    EtcdDirectoryDao<NodeData> dirDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir", mapper,
        NODE_DATA_TYPE);
    EtcdDirectoryDao<NoiseDocument> noiseDao =
      new EtcdDirectoryDao<NoiseDocument>(clientRule::getClient, BASE_PATH + "/dir/sub", mapper,
        new TypeReference<NoiseDocument>() {
        });

    Thread events = startNodeDataThread(dirDao, eventsCount);
    Thread noise = startNoiseThread(noiseDao, eventsCount);

    try {
      service.registerDirectoryWatch("/dir", new TypeReference<NodeData>() {
      }, handler);

      verifySequentialNodeData(handler, eventsCount);
    } finally {
      events.join();
      noise.join();
    }
  }

  @Test
  public void shouldIgnoreEventsInSubPaths() throws InterruptedException {
    int eventsCount = 100;
    // add a directory watch.
    WatchService service = serviceRule.getService();

    @SuppressWarnings("unchecked")
    EtcdEventHandler<NodeData> handler = mock(EtcdEventHandler.class);

    EtcdDirectoryDao<NodeData> dirDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir", mapper,
        NODE_DATA_TYPE);
    EtcdDirectoryDao<NodeData> subDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir/sub", mapper,
        NODE_DATA_TYPE);

    Thread events = startNodeDataThread(dirDao, eventsCount);
    Thread noise = startNodeDataThread(subDao, eventsCount);

    try {
      service.registerDirectoryWatch("/dir", new TypeReference<NodeData>() {
      }, handler);

      verifySequentialNodeData(handler, eventsCount);
    } finally {
      events.join();
      noise.join();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldWatchSingleFile() throws InterruptedException {
    int eventsCount = 100;
    // add a directory watch.
    WatchService service = serviceRule.getService();

    EtcdEventHandler<NodeData> handler = mock(EtcdEventHandler.class);

    EtcdDirectoryDao<NodeData> dirDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir", mapper,
        NODE_DATA_TYPE);
    Thread events = startNodeDataThread(dirDao, eventsCount);

    try {
      service.registerValueWatch("/dir", "10", new TypeReference<NodeData>() {
      }, handler);

      verify(handler, timeout(10000)).handle(
        atAnyIndex(EtcdEvent.<NodeData> builder().withKey("10").withType(EtcdEvent.Type.added)
          .withValue(new NodeData().withName("10")).build()));

      verify(handler, times(1)).handle(any(EtcdEvent.class));

    } finally {
      events.join();
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void shouldWatchSingleFileWithNoise() throws InterruptedException {
    int eventsCount = 100;
    // add a directory watch.
    WatchService service = serviceRule.getService();

    EtcdEventHandler<NodeData> handler = mock(EtcdEventHandler.class);

    EtcdDirectoryDao<NodeData> dirDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir", mapper,
        NODE_DATA_TYPE);
    
    startNoiseThread(externalNoiseDao, 4000).join();
    
    Thread events = startNodeDataThread(dirDao, eventsCount);

    try {
      service.registerValueWatch("/dir", "10", new TypeReference<NodeData>() {
      }, handler);

      verify(handler, timeout(10000)).handle(
        atAnyIndex(EtcdEvent.<NodeData> builder().withKey("10").withType(EtcdEvent.Type.added)
          .withValue(new NodeData().withName("10")).build()));

      verify(handler, times(1)).handle(any(EtcdEvent.class));

    } finally {
      events.join();
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void shouldWatchSingleFileWithNoiseAndTimeout() throws InterruptedException {
    int eventsCount = 100;
    // add a directory watch.
    WatchService service = serviceRule.getService();

    CountDownLatch latch = new CountDownLatch(eventsCount);
    EtcdEventHandler<NodeData> handler = (event)->{
      latch.countDown();
    };

    EtcdDirectoryDao<NodeData> dirDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir", mapper,
        NODE_DATA_TYPE);
    
    service.registerDirectoryWatch("/dir", NODE_DATA_TYPE, handler);

    Thread noiseThread = startNoiseThread(externalNoiseDao, 4000);
    Thread waitThread = startWaitThread(1, TimeUnit.SECONDS);
    
    noiseThread.join();
    waitThread.join();
      
    startNodeDataThread(dirDao, eventsCount).join();

    if( !latch.await(10, TimeUnit.SECONDS) ) {
      throw new IllegalStateException("could not catch up with state.");
    }
  }

  @Test
  public void shouldPublishEventsForUpdate() {
    WatchService service = serviceRule.getService();

    @SuppressWarnings("unchecked")
    EtcdEventHandler<NodeData> handler = mock(EtcdEventHandler.class);

    EtcdDirectoryDao<NodeData> dirDao =
      new EtcdDirectoryDao<NodeData>(clientRule::getClient, BASE_PATH + "/dir", mapper,
        NODE_DATA_TYPE);

    Watch watch = service.registerDirectoryWatch("/dir", new TypeReference<NodeData>() {
    }, handler);

    dirDao.put("id", new NodeData().withName("original"));

    dirDao.update("id", n -> "original".equals(n.getName()), n -> n.withName("updated"));

    verify(handler, timeout(1000)).handle(
      EtcdEvent.<NodeData> builder().withKey("id").withType(Type.added)
        .withValue(new NodeData().withName("original")).build());
    verify(handler, timeout(1000)).handle(
      EtcdEvent.<NodeData> builder().withKey("id").withType(Type.updated)
        .withValue(new NodeData().withName("updated"))
        .withPrevValue(new NodeData().withName("original")).build());

    watch.stop();

  }

  public static Thread startNodeDataThread(EtcdDirectoryDao<NodeData> dao, int count) {
    Thread events = new Thread(() -> {
      for (int i = 0; i < count; i++) {
        dao.put(String.valueOf(i), new NodeData().withName(String.valueOf(i)));
      }
    });
    events.start();
    return events;
  }

  public static Thread startNoiseThread(EtcdDirectoryDao<NoiseDocument> dao, int count) {
    Random random = new Random();
    Thread events =
      new Thread(() -> {
        for (int i = 0; i < count; i++) {
          switch (random.nextInt(4)) {
            case 0:
            case 1:
            case 2:
              dao.put("noise_" + String.valueOf(i),
                new NoiseDocument().withNoise(String.valueOf(i)));
              break;
            case 3:
              dao.putDir("/noise_" + String.valueOf(i));
              break;
          }
        }
      });
    events.start();
    return events;
  }
  
  public static Thread startWaitThread(long timeout, TimeUnit unit) {
    Thread waitThread = new Thread(()->{try {
      unit.sleep(timeout);
    } catch( Exception e ) {
      // oh well!
    }});
    waitThread.start();
    return waitThread;
  }

  public static void verifySequentialNodeData(EtcdEventHandler<NodeData> handler, int count) {
    for (int i = 0; i < count; i++) {
      verify(handler, timeout(10000)).handle(
        atAnyIndex(EtcdEvent.<NodeData> builder().withKey(String.valueOf(i))
          .withType(EtcdEvent.Type.added).withValue(new NodeData().withName(String.valueOf(i)))
          .build()));
    }
  }

  public static class NodeData {
    protected String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public NodeData withName(String name) {
      this.name = name;
      return this;
    }

    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }

    public boolean equals(Object o) {
      return EqualsBuilder.reflectionEquals(this, o);
    }
  }

  public static class NoiseDocument {
    protected String noise;

    public String getNoise() {
      return noise;
    }

    public void setNoise(String noise) {
      this.noise = noise;
    }

    public NoiseDocument withNoise(String noise) {
      this.noise = noise;
      return this;
    }

    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }

    public boolean equals(Object o) {
      return EqualsBuilder.reflectionEquals(this, o);
    }

  }
}
