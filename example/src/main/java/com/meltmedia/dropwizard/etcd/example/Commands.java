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

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;

import java.net.URI;
import java.util.List;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.meltmedia.dropwizard.etcd.cluster.ClusterProcess;
import javax.ws.rs.client.*;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

public class Commands {
  public static class AddCommand extends Command {

    protected AddCommand() {
      super("add", "adds a name");
    }

    @Override
    public void configure(Subparser subparser) {
      subparser.addArgument("endpoint").help("server api root url");
      subparser.addArgument("name").help("the name to add");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
      URI host = URI.create(namespace.getString("endpoint"));
      String name = namespace.getString("name");

      Response response =
          ClientBuilder
              .newClient()
              .target(host)
              .path("hello")
              .request()
              .post(Entity.json(new ClusterProcess().withConfiguration(JsonNodeFactory.instance.objectNode().put(
                      "name", name))));

      if (response.getStatus() != 201) {
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
    public void configure(Subparser subparser) {
      subparser.addArgument("endpoint").help("server api root url");
      subparser.addArgument("name").help("the name to remove");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
      URI host = URI.create(namespace.getString("endpoint"));
      String name = namespace.getString("name");

      WebTarget helloResource = ClientBuilder.newClient().target(host).path("hello");

      // lookup process uri.
      Response response = helloResource.queryParam("name", name).request().get();

      if (response.getStatus() != 200) {
        System.err.printf("unexpected status code %d finding process%n", response.getStatus());
        System.exit(1);
      }

      List<ClusterProcess> processList =
          response.readEntity(new GenericType<List<ClusterProcess>>(){});

      if (processList.size() == 0) {
        System.err.println("not found");
        System.exit(0);
      }

      String id = (String) processList.get(0).getAdditionalProperties().get("id");

      Response delete = helloResource.path(id).request().delete();

      if (delete.getStatus() != 204) {
        System.err.printf("unexpected status code %d deleting process %s%n", response.getStatus(),
            id);
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
    public void configure(Subparser subparser) {
      subparser.addArgument("endpoint").help("server api root url");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
      URI host = URI.create(namespace.getString("endpoint"));

      WebTarget helloResource = ClientBuilder.newClient().target(host).path("hello");

      Response response = helloResource.request().accept("application/json").get();

      if (response.getStatus() != 200) {
        System.err.printf("unexpected status code %d%n", response.getStatus());
        System.exit(1);
      }

      response.readEntity(new GenericType<List<ClusterProcess>>() {
      }).stream().map(process -> process.getConfiguration().get("name"))
          .forEach(System.out::println);

      System.exit(0);
    }
  }
}
