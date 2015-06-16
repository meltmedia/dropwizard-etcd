# Dropwizard Etcd

A Dropwizard bundle for Etcd.

[![Build Status](https://travis-ci.org/meltmedia/dropwizard-etcd.svg)](https://travis-ci.org/meltmedia/dropwizard-etcd)

## Usage

### Maven

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

You will also need to include the project in your dependencies.

```
<dependency>
  <groupId>com.meltmedia.dropwizard</groupId>
  <artifactId>dropwizard-etcd</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Java

Define the EtcdConfiguraion class somewhere in your applications configuration.

```
import com.meltmedia.dropwizard.etcd.EtcdConfiguration;

...

  @JsonProperty
  protected EtcdConfiguration etcd;
  
  @JsonProperty
  protected String etcdDirectory;

  public EtcdConfiguration getEtcd() {
    return etcd;
  }
  
  public String getEtcdDirectory() {
    return etcdDirectory;
  }
  
  public void setEtcdDirectory( String etcdDirectory  ) {
    this.etcdDirectory = etcdDirectory;
  }
```

Then include the bundle in the `initialize` method of your application.

```
import com.meltmedia.dropwizard.etcd.EtcdBundle;

...
protected EtcdBundle etcdBundle;         // the basic bundle that supplies the client
protected EtcdJsonBundle etcdJsonBundle; // adds support for JSON values in etcd
protected ClusterBundle clusterBundle;   // adds support for clustered processing
protected ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(10);

@Override
public void initialize(Bootstrap<ExampleConfiguration> bootstrap) {
  bootstrap.addBundle(etcdBundle = EtcdBundle.<ExampleConfiguration>builder()
    .withConfiguration(ExampleConfiguration::getEtcd)
    .build());
  bootstrap.addBundle(etcdJsonBundle = EtcdJsonBundle.<ExampleConfiguration>builder()
    .withClient(etcdBundle::getClient)
    .withExecutor(()->{return executor;})
    .withDirectory(ExampleConfiguration::getEtcdDirectory)
    .build());
  bootstrap.addBundle(clusterBundle = ClusterBundle.<ExampleConfiguration>builder()
    .withExecutorSupplier(()->{return executor;})
    .withFactorySupplier(etcdJsonBundle::getFactory)
    .build());
}
```

Finally, use the bundle to access the client.

```
@Override
public void run(ExampleConfiguration config, Environment env) throws Exception {
  EtcdClient client = etcdBundle.getClient();
}
```

### Configuration

Add the etcd configuraiton block to your applications config.

```
etcd:
  urls:
    - http://localhost:2379
  hostName: localhost
```

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
