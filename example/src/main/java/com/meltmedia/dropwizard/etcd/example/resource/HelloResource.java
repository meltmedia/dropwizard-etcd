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
package com.meltmedia.dropwizard.etcd.example.resource;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.meltmedia.dropwizard.etcd.cluster.ClusterProcess;
import com.meltmedia.dropwizard.etcd.json.EtcdDirectoryDao;

@Singleton
@Path("/hello")
public class HelloResource {
  @Inject
  @Named("hello")
  EtcdDirectoryDao<ClusterProcess> dao;

  @GET
  @Produces("application/json")
  public List<ClusterProcess> list(@QueryParam("name") String name) {
    if (name != null) {
      return dao.stream()
          .filter(process -> name.equals(process.getConfiguration().get("name").asText()))
          .collect(Collectors.toList());
    }
    return dao.stream().collect(Collectors.toList());
  }

  @GET
  @Path("{id}")
  @Produces("application/json")
  public ClusterProcess get(@PathParam("id") String id) {
    return dao.get(id);
  }

  @DELETE
  @Path("{id}")
  public Response delete(@PathParam("id") String id) {
    dao.remove(id);

    return Response.noContent().build();
  }

  @POST
  @Consumes("application/json")
  public Response create(@Context UriInfo uriInfo, ClusterProcess process) {
    String id = UUID.randomUUID().toString();
    dao.put(id, process.withAdditionalProperty("id", id));

    return Response.created(uriInfo.getAbsolutePathBuilder().path(id).build()).build();
  }
}
