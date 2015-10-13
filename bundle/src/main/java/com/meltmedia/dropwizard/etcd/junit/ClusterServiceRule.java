package com.meltmedia.dropwizard.etcd.junit;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.joda.time.DateTime;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.meltmedia.dropwizard.etcd.cluster.ClusterNode;
import com.meltmedia.dropwizard.etcd.cluster.ClusterService;
import com.meltmedia.dropwizard.etcd.json.EtcdJson;
import com.meltmedia.dropwizard.etcd.json.EtcdJson.MappedEtcdDirectory;

public class ClusterServiceRule implements TestRule  {
  public static class Builder {
    IntFunction<MetricRegistry> registrySupplier;
    ClusterNode clusterNode;
    Supplier<EtcdJson> etcdJsonSupplier;
    String directory;
    int serviceCount;
    
    public Builder withMetricRegistry(IntFunction<MetricRegistry> registrySupplier) {
      this.registrySupplier = registrySupplier;
      return this;
    }
    
    public Builder withServiceCount( int serviceCount ) {
      this.serviceCount = serviceCount;
      return this;
    }
    
    public Builder withEtcdJsonSupplier( Supplier<EtcdJson> etcdJsonSupplier) {
      this.etcdJsonSupplier = etcdJsonSupplier;
      return this;
    }
    
    public Builder withDirectory( String directory ) {
      this.directory = directory;
      return this;
    }

    public ClusterServiceRule build() {
      if( registrySupplier == null ) registrySupplier = index->new MetricRegistry();
      if( directory == null ) directory = "/nodes";
      return new ClusterServiceRule(directory, serviceCount, etcdJsonSupplier, registrySupplier);
    }
  }

  public static Builder builder() {
    return new Builder();
  }
  
  IntFunction<MetricRegistry> registrySupplier;
  IntFunction<ClusterNode> clusterNodeFactory = index->new ClusterNode().withId("node"+index).withStartedAt(new DateTime());
  Supplier<EtcdJson> etcdJsonSupplier;
  String directory;
  int serviceCount;
  List<ClusterService> services;

  public ClusterServiceRule( String directory, int serviceCount, Supplier<EtcdJson> etcdJsonSupplier, IntFunction<MetricRegistry> registrySupplier ) {
    this.serviceCount = serviceCount;
    this.etcdJsonSupplier = etcdJsonSupplier;
    this.registrySupplier = registrySupplier;
    this.directory = directory;
  }
  
  public List<ClusterService> getServices() {
    return Collections.unmodifiableList(services);
  }

  @Override
  public Statement apply( Statement base, Description description ) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        EtcdJson etcdJson = etcdJsonSupplier.get();
        
        services = IntStream.range(0, serviceCount).mapToObj(index->{
          ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
          
          MappedEtcdDirectory<ClusterNode> nodeDir = etcdJson.newDirectory(directory, new TypeReference<ClusterNode>() {});

          return ClusterService.builder()
            .withEtcdFactory(etcdJson)
            .withExecutor(executor)
          .withNodesDirectory(nodeDir)
          .withThisNode(clusterNodeFactory.apply(index))
          .withMetricRegistry(registrySupplier.apply(index))
          .build();          
        }).collect(Collectors.toList());
        
        services.forEach(ClusterService::start);
        try {
          base.evaluate();
        } finally {
          services.forEach(ClusterService::stop);
        }
      }
    };
  }
}
