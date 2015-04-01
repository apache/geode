## Gemcached

GemFire servers can be configured to talk memcached protocol. GemFire server is [memcapable](http://libmemcached.org/Memcapable.html), this means any existing memcached application can be pointed to a GemFire cluster with zero lines of code change. All you need to do is to specify a port and/or the protocol (Binary or ASCII) while starting the GemFire server.

```gfsh>start server --name=server1 --memcached-port=11211 --memcached-protocol=BINARY```

The GemFire server creates a region named “gemcached” for storing all memcached data. The gemcached region is PARTITION by default.

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
Then use this cache.xml while starting the GemFire server like so:
```
gfsh>start server --name=server1 --memcached-port=11211 --memcached-protocol=BINARY --cache-xml-file=/path/to/cache.xml
```

## Why move from memcached?

In a typical workflow, your application will read data from database if it is not found in memcached, then writes the fetched data to memcached. When an update occurs, you would update the database followed by updating the cache. Since this is a two step operation, you could run into race conditions which leaves your cache and database inconsistent. 

### Stale cache
Say you have 2 clients trying to write the same key. Both clients will first update the DB followed by updating memcached. c1 updates K to value to V1 in the DB, c2 updates K to value to V2, then manages to update the value in memcached to V2 before c1, then when c1 eventually updates the value to V1, memcached and DB are going to be inconsistent. You can use CAS to get around this issue, but now each write will have to first read from memcached, then write to DB followed by a write (CAS) to memcached. A client may die just after it updated the DB but before it wrote the change to memcached. All other clients are oblivious of the changed DB and happily continue serving stale data.

### Thundering herds
Say your application is serving up very popular content from one of your memcached servers. When this server crashes, all clients hitting that server will get a cache miss, and now all the clients end up going to the database potentially overwhelming it.

With GemFire you can use GemFire's loaders and AsyncWriters to read data from and write changes to your database. This will essentially eliminate coding errors from your applications.