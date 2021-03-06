# Dropwizard Etcd

A Dropwizard bundle for Etcd.

[![Build Status](https://travis-ci.org/meltmedia/dropwizard-etcd.svg)](https://travis-ci.org/meltmedia/dropwizard-etcd)

## Usage

### Maven

This project releases to Maven Central.  To use the bundle, simply include its dependency in your project.

```
<dependency>
  <groupId>com.meltmedia.dropwizard</groupId>
  <artifactId>dropwizard-etcd</artifactId>
  <version>0.3.0</version>
</dependency>
```

To use SNAPSHOTs of this project, you will need to include the sonatype repository in your POM.

```
<repositories>
    <repository>
        <snapshots>
        <enabled>true</enabled>
        </snapshots>
        <id>sonatype-nexus-snapshots</id>
        <name>Sonatype Nexus Snapshots</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
</repositories>
```

### Client Bundle

To simply get access to a client for Etcd, you will need to set up the `EtcdBundle`.

First, define the EtcdConfiguraion class somewhere in your applications configuration.

```
import com.meltmedia.dropwizard.etcd.EtcdConfiguration;

...

  @JsonProperty
  protected EtcdConfiguration etcd;

  public EtcdConfiguration getEtcd() {
    return etcd;
  }
```

and add configuration for the client to your configuration

```
etcd:
  urls:
    - http://localhost:2379
  hostName: localhost
```

Then include the bundle in the `initialize` method of your application.

```
import com.meltmedia.dropwizard.etcd.EtcdBundle;

...
protected EtcdBundle etcdBundle;

@Override
public void initialize(Bootstrap<ExampleConfiguration> bootstrap) {
  bootstrap.addBundle(etcdBundle = EtcdBundle.<ExampleConfiguration>builder()
    .withConfiguration(ExampleConfiguration::getEtcd)
    .build());
}
```

and access the client in your run command.

```
@Override
public void run(ExampleConfiguration config, Environment env) throws Exception {
  EtcdClient client = etcdBundle.getClient();
}
```


### Json Factory Bundle

If you want to get support for dealing with Etcd values as JSON, then you can additionally configure the `EtcdJsonBundle`.

First, define the root etcd directory for your application somewhere in your configuration

```
  @JsonProperty
  protected String etcdDirectory;
  
  public String getEtcdDirectory() {
    return etcdDirectory;
  }
  
  public void setEtcdDirectory( String etcdDirectory  ) {
    this.etcdDirectory = etcdDirectory;
  }
```

and add the directory to your configuration

```
etcdDirectory: /example-app
```

Then include the bundle in the `initialize` method of your application.  Initialization will require a `ScheduledExecutorService` to be defined.

```
import com.meltmedia.dropwizard.etcd.EtcdJsonBundle;
import java.util.concurrent.ScheduledExecutorService;

...
protected EtcdJsonBundle etcdJsonBundle;
protected ScheduledExecutorService executor;

@Override
public void initialize(Bootstrap<ExampleConfiguration> bootstrap) {
  ...
  bootstrap.addBundle(etcdJsonBundle = EtcdJsonBundle.<ExampleConfiguration>builder()
    .withClient(etcdBundle::getClient)
    .withExecutor(()->{return executor;})
    .withDirectory(ExampleConfiguration::getEtcdDirectory)
    .build());
}
```

Then you can create DAOs, add watches and start heartbeats on Etcd.

```
@Override
public void run(ExampleConfiguration config, Environment env) throws Exception {
  EtcdJson factory = etcdJsonBundle.getFactory();
  
  // get a handle to a directory.
  MappedEtcdDirectory directory = factory.newDirectory("/dir", new TypeReference<MyType>(){});
}
```

## Cluster Bundle

The cluster bundle supports running processes across multiple instances of your application.  It requires the
`EtcdJsonBundle` to also be installed.

You will need to add the bundle to your `initialize` method

```
import com.meltmedia.dropwizard.etcd.cluster.ClusterBundle;

...
protected ClusterBundle clusterBundle;
protected ScheduledExecutorService executor;

@Override
public void initialize(Bootstrap<ExampleConfiguration> bootstrap) {
  ...
  bootstrap.addBundle(clusterBundle = ClusterBundle.<ExampleConfiguration>builder()
    .withExecutorSupplier(()->{return executor;})
    .withFactorySupplier(etcdJsonBundle::getFactory)
    .build());
}
```

then you can get access to the cluster service from the bundle.

```
@Override
public void run(ExampleConfiguration config, Environment env) throws Exception {
  ClusterService clusterService = clusterBundle.getService();
}
```

There is an example of using this service in [the example project](example/src/main/java/com/meltmedia/dropwizard/etcd/example/HelloProcessor.java).

## Building

This project builds with Java 8 and Maven 3.  After cloning the repo, install the bundle from the root of the project.

```
mvn clean install
```

### Integration Tests

Run the build with the `integration-tests` profile.

```
mvn clean install -P integration-tests
```

## Contributing

This project accepts PRs, so feel free to fork the project and send contributions back.

### Formatting

This project contains formatters to help keep the code base consistent.  The formatter will update Java source files and add headers to other files.  When running the formatter, I suggest the following procedure:

1. Make sure any outstanding stages are staged.  This will prevent the formatter from destroying your code.
2. Run `mvn format`, this will format the source and add any missing license headers.
3. If the changes look good and the project still compiles, add the formatting changes to your staged code.

If things go wrong, you can run `git checkout -- .` to drop the formatting changes. 
