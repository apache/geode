# Gemcached

Geode servers can be configured to talk memcached protocol. Geode server is [memcapable](http://libmemcached.org/Memcapable.html), this means any existing memcached application can be pointed to a Geode cluster with zero lines of code change. All you need to do is to specify a port and/or the protocol (Binary or ASCII) while starting the Geode server.

```gfsh>start server --name=server1 --memcached-port=11211 --memcached-protocol=BINARY```

The Geode server creates a region named “gemcached” for storing all memcached data. The gemcached region is PARTITION by default.

## Configure “gemcached”

To change the region attributes for the gemcached region, use a cache.xml to define the attributes you want. Example cache.xml below shows how to change total number of buckets to 251.
```
<?xml version="1.0" encoding="UTF-8"?>
<cache
    xmlns="http://schema.pivotal.io/gemfire/cache"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://schema.pivotal.io/gemfire/cache http://schema.pivotal.io/gemfire/cache/cache-8.1.xsd"
    version="8.1">
	<region name="gemcached"> 
	  <region-attributes refid="PARTITION"> 
	    <partition-attributes total-num-buckets="251"/> 
	  </region-attributes> 
	</region>
</cache>
```
Then use this cache.xml while starting the Geode server like so:
```
gfsh>start server --name=server1 --memcached-port=11211 --memcached-protocol=BINARY --cache-xml-file=/path/to/cache.xml
```

## Why move from memcached?

One of the fundamental problems with memcached is that it only supports “cache-aside” (as opposed to “write-through”) i.e. the application is responsible for updating the cache as well as the database. This results in:
- Potential for inconsistency between the cache and the DB
- Polluting the business logic in each of your application with same infrastructure concerns.  

![memcached workflow](http://i.imgur.com/Jjf4AKC.png?2)

In a typical workflow, your application will read data from memcached, if not found it will read from the DB, then write the fetched data to memcached. When an update occurs, you would update the database followed by updating/invalidating the cache. Since this is a two step operation, you could run into race conditions which leave your cache and database inconsistent. 

### Stale cache
 A client may die just after it updated the DB but before it wrote the change to memcached. All other clients are oblivious of the changed DB and happily continue serving stale data.

### Inconsistent cache
Applications can use the CAS command to ensure that they are not overwriting data that they had not seen. For using CAS, the workflow is: read from memcached for getting the cas identifier (So, we always have to do one extra read for each write), then update the DB followed by writing to memcached using the CAS operation, if the CAS operation failed, invalidate/destroy the cached entry.
Even with using CAS, there is a small window where your DB and cache are still inconsistent. Say you have 2 clients (c1 and c2) trying to write the same key (K). Both clients will first fetch the same CAS identifier from memcached, update the DB followed by updating memcached. From the time that a CAS fails till when the application turns around to destroy the key, memcached will have stale data. If the client dies before sending the destroy, your cache and data will be forever inconsistent.

### Thundering herds
Say your application is serving up very popular content from one of your memcached servers. When this server crashes, all clients hitting that server will get a cache miss, and now all the clients end up going to the database potentially overwhelming it.

## Gemcached to the rescue!
With Gemcached, you can use Geode as a write-through cache.
![Geode as write through cache](http://i.imgur.com/QGozVMm.png?1)

This means that your application does not have to talk to the database anymore, simplifying your application code. All database reads and writes are done through Geode. To read data from DB you can use Geode's CacheLoader and to write data back to the DB use AsyncEventListener. Lets look at how the above problems are solved with gemcached.

### Stale cache
 The client only writes to Geode, and when that write completes, Geode guarantees that the write has already been replicated to the redundant copy. Even if the primary bucket for the key died before replicating to the redundant copy, none of the other clients, nor the database will see the update.

### Inconsistent cache
Since all changes are first made to Geode and then persisted to the DB, they will always be consistent. All updates will make it to the DB even in case of Geode node failures since the AsyncEventListener queue is replicated as well as persistent.

### Thundering herds
When there are multiple concurrent requests for the same key in Geode and they result in a cache miss, they all invoke the CacheLoader, however the CacheLoader will only allow one request to hit the database. Other concurrent requests will use the newly fetched value from the database.