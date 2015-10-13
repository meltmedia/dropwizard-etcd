package com.meltmedia.dropwizard.etcd.json;

import java.util.List;

import com.codahale.metrics.health.HealthCheck;
import com.meltmedia.dropwizard.etcd.json.WatchService.Watch;

public class WatchServiceHealthCheck extends HealthCheck {

  private WatchService watchService;
  
  public WatchServiceHealthCheck( WatchService watchService ) {
    this.watchService = watchService;
  }

  @Override
  protected Result check() throws Exception {
    List<Watch> watchers = watchService.outOfSyncWatchers();
    
    if( watchers.isEmpty() ) {
      return Result.healthy("all watches synchronized");
    }
    else {
      return Result.unhealthy("%d unsynchronized watches", watchers.size());
    }
  }

}
