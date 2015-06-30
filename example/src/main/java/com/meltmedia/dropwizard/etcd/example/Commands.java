package com.meltmedia.dropwizard.etcd.example;

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;

import java.net.URI;
import java.util.List;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.meltmedia.dropwizard.etcd.cluster.ClusterProcess;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;

public class Commands {
  public static class AddCommand extends Command {

  protected AddCommand() {
    super("add", "adds a name");
  }

  @Override
  public void configure( Subparser subparser ) {
    subparser.addArgument("endpoint")
      .help("server api root url");
    subparser.addArgument("name")
      .help("the name to add");
  }

  @Override
  public void run( Bootstrap<?> bootstrap, Namespace namespace ) throws Exception {
    URI host = URI.create(namespace.getString("endpoint"));
    String name = namespace.getString("name");
    
    ClientResponse response = Client.create().resource(host).path("hello")
      .entity(new ClusterProcess()
      .withConfiguration(JsonNodeFactory.instance.objectNode()
        .put("name", name)), "application/json")
        .post(ClientResponse.class);
    
    if( response.getStatus() != 201 ) {
      System.err.printf("unexpected status code %d%n", response.getStatus());
      System.exit(1);
    }
    
    System.exit(0);
  }
  }
  
  public static class RemoveCommand extends Command {

    protected RemoveCommand() {
      super("remove", "removes a name");
    }

    @Override
    public void configure( Subparser subparser ) {
      subparser.addArgument("endpoint")
      .help("server api root url");
    subparser.addArgument("name")
      .help("the name to remove");
    }

    @Override
    public void run( Bootstrap<?> bootstrap, Namespace namespace ) throws Exception {
      URI host = URI.create(namespace.getString("endpoint"));
      String name = namespace.getString("name");
      
      WebResource helloResource = Client.create().resource(host).path("hello");
      
      // lookup process uri.
      ClientResponse response = helloResource
        .queryParam("name", name)
        .get(ClientResponse.class);
      
      if( response.getStatus() != 200 ) {
        System.err.printf("unexpected status code %d finding process%n", response.getStatus());
        System.exit(1);
      }
      
      List<ClusterProcess> processList = response
        .getEntity(new GenericType<List<ClusterProcess>>(){});
      
      if( processList.size() == 0 ) {
        System.err.println("not found");
        System.exit(0);
      }
      
      String id = (String)processList.get(0).getAdditionalProperties().get("id");
      
      ClientResponse delete = helloResource.path(id).delete(ClientResponse.class);
      
      if( delete.getStatus() != 204 ) {
        System.err.printf("unexpected status code %d deleting process %s%n", response.getStatus(), id);
        System.exit(1);
      }
      
      System.exit(0);
    }
    
  }
  
  public static class ListCommand extends Command {

    protected ListCommand() {
      super("list", "lists current names");
    }

    @Override
    public void configure( Subparser subparser ) {
      subparser.addArgument("endpoint")
      .help("server api root url");
    }

    @Override
    public void run( Bootstrap<?> bootstrap, Namespace namespace ) throws Exception {
      URI host = URI.create(namespace.getString("endpoint"));
      
      WebResource helloResource = Client.create().resource(host).path("hello");
      
      ClientResponse response = helloResource.accept("application/json")
        .get(ClientResponse.class);
      
      if( response.getStatus() != 200 ) {
        System.err.printf("unexpected status code %d%n", response.getStatus());
        System.exit(1);        
      }
      
      response.getEntity(new GenericType<List<ClusterProcess>>(){})
        .stream()
        .map(process->process.getConfiguration().get("name"))
        .forEach(System.out::println);
      
      System.exit(0);
    }
  }
}


