# Exactly Once Example

This example demonstrates how Pravega EXACTLY_ONCE mode works in conjection with Flink checkpointing and exactly-once mode. More information on Pravega EXACTLY_ONCE semantics can be found at [here](http://pravega.io/docs/latest/key-features/#exactly-once-semantics).

The example consists of two applications, a writer and a checker.

```
$ cd flink-examples/build/install/pravega-flink-examples
$ bin/exactlyOnceChecker  [--controller tcp://localhost:9090] [--scope examples] [--stream mystream]
$ bin/exactlyOnceWriter   [--controller tcp://localhost:9090] [--scope examples] [--stream mystream] [--num-events 50] [--exactlyonce true]
```

The writer application generates a set of "integer" events and introduces an artificial exception to 
simulate transaction failure. It takes checkpoints periodically. It is configured to restore 
from the latest Flink checkpoint in case of failures.
For demo purpose, it also generates "start" and "end" events for the checker to detect duplicate events.

Start the ExactlyOnceChecker app in one window. Come back to the window to observe the output 
after running the writer app.

```
$ bin/exactlyOnceChecker --scope examples --stream mystream --controller tcp://localhost:9090
```


In another window, start the ExactlyOnceWriter with EXACTLY_ONCE mode set to false.
This is to demonstrate what happens when Pravega EXACTLY_ONCE mode not enabled 

```
$ bin/exactlyOnceWriter --controller tcp://localhost:9090 --scope examples --stream mystream --exactlyonce false
```

Snippets of output shown below:

```
......

Start checkpointing at position 16
Complete checkpointing at position 16
Artificial failure at position 26
05/02/2018 17:02:02	Source: Custom Source -> Map(1/1) switched to FAILED 
io.pravega.examples.flink.primer.util.FailingMapper$IntentionalException: artificial failure

......

Restore from checkpoint at position 16
Start checkpointing at position 50
Complete checkpointing at position 50

......

```
The app completes checkpointing at position 16, and in the meantime, continues to write to the 
Pravega stream until it introduces an artificial exception to simulate transaction failure 
at position 26. Upon the failure, the app restores from its last successful checkpoint 
at position 16. Without the Pravega EXACTLY_ONCE enabled, it is likely that duplicate events, 
from position 17 to 26, will be written to the Pravega stream. 

And indeed, that's what checker app shows. Note that output of duplicate events may not necessarily 
be within the checker start and end block. 

```
============== Checker starts ===============
Duplicate event: 18
Duplicate event: 20
Duplicate event: 22
Duplicate event: 24
Duplicate event: 17
Duplicate event: 19
Duplicate event: 21
Duplicate event: 23
Found duplicates
============== Checker ends  ===============
```

If you wish, run the writer app a few more times by specifying different values for --num-events option.
The app will restore from different positions and the checker window should continue to show duplicate events. 

Now run the ExactlyOnceWriter in EXACTLY_ONCE mode.

```
$ bin/exactlyOnceWriter --controller tcp://localhost:9090 --scope examples --stream mystream --num-events 50  --exactlyonce true
```

The output should look like the followings:

```
============== Checker starts ===============
No duplicate found. EXACTLY_ONCE!
============== Checker ends  ===============
```
