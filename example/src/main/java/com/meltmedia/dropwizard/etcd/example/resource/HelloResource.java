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
    if( name != null ) {
      return dao.stream()
        .filter(process->name.equals(process.getConfiguration().get("name").asText()))
        .collect(Collectors.toList());
    }
    return dao
      .stream()
      .collect(Collectors.toList());
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
    
    return Response.created(uriInfo.getAbsolutePathBuilder().path(id).build())
      .build();
  }
}
