# Geode replicated region example

This is one of the most basic examples. 
Two servers host a replicated region.
The producer puts 50 entries into the replicated region. The consumer prints the number of entries in the region.

## Steps
1. From the ```geode-examples/replicated``` directory, start the locator and two servers:

        $ scripts/startAll.sh

2. Run the producer:

        $ gradle run -Pmain=Producer
        ...
        ... 
        INFO: Done. Inserted 50 entries.

3. Run the consumer:

        $ gradle run -Pmain=Consumer
        ...
        ...
        INFO: Done. 50 entries available on the server(s).

4. Kill one of the servers:

        $ gfsh
        ...
        gfsh>connect
        gfsh>stop server --name=server1
        gfsh>quit

5. Run the consumer a second time, and notice that all the entries are still available due to replication: 

        $ gradle run -Pmain=Consumer
        ...
        ...
        INFO: Done. 50 entries available on the server(s).

6. Shutdown the system:

        $ scripts/stopAll.sh

This example is a simple demonstration on basic APIs of Geode, as well how to write tests using mocks for Geode applications.
