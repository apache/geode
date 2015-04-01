GemFire servers can be configured to talk memcached protocol. GemFire server is [memcapable](http://libmemcached.org/Memcapable.html), this means any existing memcached application can be pointed to a GemFire cluster with zero lines of code change. All you need to do is to specify a port and/or the protocol (Binary or ASCII) while starting the GemFire server.

`gfsh>start server --name=server1 --memcached-port=11211 --memcached-protocol=BINARY`

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
`gfsh>start server --name=server1 --memcached-port=11211 --memcached-protocol=BINARY --cache-xml-file=/path/to/cache.xml`