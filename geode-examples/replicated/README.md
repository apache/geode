# Geode replicated region example

This is one of the most basic examples for Geode. 
It created a replicated region and insert a couple of entries and then print the number of entries in the server.

## Steps
1. Start servers
```
$ scripts/startAll.sh
```
1. Run producer
```
$ gradle run -Pmain=Producer
...
... 
INFO: Done. Inserted 50 entries.
```
1. Run consumer
```
$ gradle run -Pmain=Consumer
...
...
INFO: Done. 50 entries available on the server(s).
```
1. Kill a node
```
$ ???
```
1. Run the consumer a 2nd time
``` 
$ gradle run -Pmain=Consumer
...
...
INFO: Done. 50 entries available on the server(s).
```

This example is a simple demonstration on basic APIs of Geode as well how to write tests using mocks for Geode applications.

TODO: assume jUnit4
