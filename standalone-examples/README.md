# Pravega Examples 
These applications only need a running Pravega to execute against.


## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build `pravega-samples` repository

---

# Feature Examples Catalog

## `gettingstarted`
This example consists of two applications, a `HelloWorldReader` that reads from a `Stream` and a 
`HelloWorldWriter`, that writes to a `Stream`.  

### Execution

First, execute `HelloWorldWriter`in a console:
```
$ bin/helloWorldWriter [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090] [-routingkey myRK] [-message 'hello world']
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "helloStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes
 * routingKey - "helloRoutingKey"
 * message - "hello world"

The program writes the given message with the given routing key to the `Stream` with given scope/stream 
name.

Then, execute `HelloWorldReader`in another console:

```
$ bin/helloWorldReader [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "helloStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes

The program reads all the events from the `Stream` with given scope/stream name and prints each event to 
the console.


## `consolerw`
This example includes two applications, a `ConsoleReader` and a `ConsoleWriter`. On the one hand,
`ConsoleReader` continuously reads from a `Stream` and emits all of the events onto the console. 
Moreover, it allows you to select a `StreamCut` at a particular point, and then re-read existing
events either from the head of the `Stream` until that point, or from that point to the end of the
`Stream`.

On the other hand, `ConsoleWriter` can write to `Stream`s or `Transaction`s, and manage `Transaction`s.
This application uses the console to present an interactive DSL environment that presents 
operations to write events to a `Stream` or into a `Transaction`. In addition, it presents operations 
to begin, commit, abort, ping, check status on and retrieve the id of a `Transaction`.

### Execution
You might want to run `ConsoleReader` in one window and `ConsoleWriter` in another window.
To run `ConsoleReader`, you can execute the following command:

```
$ bin/consoleReader [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "someStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes
 
To run `ConsoleWriter`, please execute:

```
$ bin/consoleWriter [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

All args are optional, if not included, the defaults are:

 * scope - "examples"
 * name - "someStream"
 * uri - "tcp://127.0.0.1" (the URI to one of the controller nodes
 
## `noop`
 
 An example of a simple reader that continually reads the contents of any `Stream`. A binary serializer is used so it 
 works against any event types. The sample emits basic information about number of events/bytes read every 30 seconds. 
 
 ```
 $ bin/noopReader [--uri tcp://127.0.0.1:9090] [--stream <SCOPE>/<STREAM>]
 ```

## `statesynchronizer`
This example illustrates the use of the Pravega `StateSynchronizer` API.
The application implements a `SharedMap` object using `StateSynchronizer`.  We implement a 
`SharedConfig` object using the `SharedMap`. The `SharedConfig` simulates the idea of a 
properties configuration that needs to be kept in sync across multiple processes.

### Execution

To demonstrate manipulating the properties of the `SharedConfig` object, we provide a CLI.

```
$ bin/sharedConfigCli [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

Use the simple DSL to `GET`, `PUT`, `REMOVE` keys from the `SharedConfig` object identified by 
scope and name. It is worthwhile to launch two or more separate CLIs in separate windows using 
the same settings and observe how changes in one CLI process are not visible in another CLI 
process until that other CLI process invokes `REFRESH`.

## `streamcuts`
This application aims at demonstrating the use of `StreamCut`s four bounded processing
on multiple `Stream`s. At the moment, the application contains two examples accessible via
command line interface: i) Simple example: The user decides which `Stream` slices s/he wants 
to read from all the st`Stream`reams by specifying indexes, and the application prints these slices 
using `ReaderGroupConfig` methods for bounded processing. ii) Time series example: `Stream`s are 
filled with events that are supposed to belong to a certain day with a given value: "_day1:5_". 
There is a variable number of events per day in each `Stream`. The user selects a day number, 
and the program makes use of `BatchClient` and `StreamCuts` to sum all the values from events 
in all `Stream`s belonging to that day.

### Execution

To demonstrate the use of `StreamCut`s, we provide a CLI. To use it, please execute:

```
$ bin/streamCutsCli [-scope myScope] [-name myStream] [-uri tcp://127.0.0.1:9090]
```

---

# Scenario Examples Catalog

## `turbineheatsensor`

An example of a lightweight IOT application that writes simulated sensor events to a Pravega 
`Stream`.

```
$ bin/turbineSensor [--stream <stream name>]
```



