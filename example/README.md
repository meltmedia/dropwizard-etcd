# Dropwizard Etcd Example

An example application that demonstraits clustering processes using etcd.  This example simply tracks a set of names and logs Hello NAME! and Goodbye NAME! as the names are assigned to and unassigned from running instances of the app.

## Before You Begin

To run this example, you will need to have the following installed on your system.

- Vagrant
- VirtualBox
- Java 8 SDK
- Maven 3
- Git

## Setup

Clone the repo and build the project.

```
git clone git@github.com:meltmedia/dropwizard-etcd.git
cd dropwizard-etcd
mvn clean install
```

Then start vagrant.  This will get an instance of Etcd running on your system.

```
vagrant up
```

Finally, change your working directory to the example module.

```
cd example
```

## Running the Example

To start, open a new terminal and start a node.

```
./target/etcd-example server config/localhost-8080.yml
```

Now that we have a node running, open a second terminal and add some names.

```
./target/etcd-example add http://localhost:8080 Coda
./target/etcd-example add http://localhost:8080 Nick
./target/etcd-example add http://localhost:8080 Graham
./target/etcd-example add http://localhost:8080 Ryan
./target/etcd-example add http://localhost:8080 Camille
```

You should see hello messages get logged.

```
Hello Coda!
Hello Nick!
Hello Graham!
Hello Ryan!
Hello Camille!
```

Now, open a third terminal and start a second node on a different port.

```
./target/etcd-example server config/localhost-8180.yml
```

Once this node starts, you should see the names get balanced between the nodes.  In node one's terminal,
you will see two of the names being told goodbye.

```
Goodbye Graham!
Goodbye Camille!
```

In node two's terminal, you will find logs with these messages.

```
Hello Graham!
Hello Camille!
```

If we remove one of these names, we will see the nodes rebalance and move one of the remaining names from the first node to the second node.

```
./target/etcd-example remove http://localhost:8080 Camille
```

That is it for this example application.  Check out [HelloProcessor](./src/main/java/com/meltmedia/dropwizard/etcd/example/HelloProcessor.java) for the code that makes this example work.
