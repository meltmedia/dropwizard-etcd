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

import java.net.URI;
import java.util.List;

import javax.validation.constraints.NotNull;

public class EtcdConfiguration {
  @NotNull
  List<URI> urls;
  int maxFrameSize = 1024 * 100;
  String hostName;

  public List<URI> getUrls() {
    return urls;
  }

  public void setUrls(List<URI> urls) {
    this.urls = urls;
  }

  public String getHostName() {
    return this.hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public int getMaxFrameSize() {
    return maxFrameSize;
  }

  public void setMaxFrameSize( int maxFrameSize ) {
    this.maxFrameSize = maxFrameSize;
  }
}
