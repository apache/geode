#Native Disk Persistence

##Persisting data regions to disk

Geode ensures that all the data you put into a region configured for persistence will be written to disk in a way that it can be recovered the next time you create the region. This allows data to be recovered after a machine or process failure or after an orderly shutdown and restart of Geode.

A Geode cache can host multiple regions and any of them can be made persistent. Unlike a traditional database management system, with Geode, the application designer makes a conscious decision as to which data sets should be in memory, which should be stored on disk, how many copies of data should be available at any time across the distributed system, etc. This granular control permits the application designer to evaluate the trade offs between memory-based performance and disk-based durability. 

Geode uses a shared-nothing disk persistence model. No two cache-members share the disk files during writing. This permits Geode applications to be deployed on commodity hardware and still achieve very high throughput.

All persistence writes are performed initially to an operation log (the term _oplog_ is used throughout this article) by appending to the oplog. If synchronous persistence is configured then these appends will be flushed from the Java VM's heap to the file system buffer before the write operation completes. Flushing completely to disk is not done in order to provide better performance. It is not needed in most use cases with Geode because multiple copies of the data are kept in memory using Geode replication.

An example use case for synchronous persistence is when a data set being stored in Geode is not managed anywhere else (at least for a period of time). For instance, in financial trading applications, the orders coming from customers could arrive at a much higher rate than the database can handle and Geode would be the only data repository to manage the durability of the data. The data might be replicated to a data warehouse, but multiple applications are dependent on the data being available at all times in the data fabric. Here, it might make sense to synchronously persist the data to disk on at least one node in the distributed system.

Persistence can also be configured to be asynchronous. In this mode, changes will be buffered in memory until they can be written to disk. This means that a configurable amount of data may be lost if the system crashes, but it provides greater performance for applications where the data loss is tolerable. An example use case for asynchronous persistence is when Geode is used for session state management. The session state across thousands of users might change very rapidly and you need the extra speed that asynchronous writes give you.

You can use persistence in conjunction with overflow to keep all of your data on disk, but only some of it in memory.

An alternative to persistence is a partitioned region with redundancy. The redundancy ensures that you do not lose data even when you have a node fail or a VM crash. However if all the nodes are taken down you must have a way to reload the data. Persistence takes care of this problem by recovering the data at startup from the disk.

##How to configure a persistent region

To configure a persistent region on Peers and Servers, a region of type replicate or partition can have a data-policy of _**persistent**_ and/or _**overflow**_.  Likewise, a local or client cache can be configured to be persistent and/or overflow as well.

Region shortcuts are groupings of pre-configured attributes that define the characteristics of a region.  You can use region shortcuts as a starting point when configuring regions and add additional configurations to customize for your application.  Use the refid attribute of the <region> element to reference a region shortcut in a Geode cache.xml.

Example cache.xml for peer/server:
Configure disk-store …

```
<cache>
…
	<disk-store name="myPersistentStore" . . . > 
	<disk-store name="myOverflowStore" . . . >


	<region name="partitioned_region1" refid="PARTITION_PERSISTENT">   
		<region-attributes disk-store-name="myPersistentStore">
		</region-attributes> </region>
	</region>
```

The above indicates a partitioned region with data-policy: PERSISTENT-PARTITION

```
	<region name="partitioned_region2_with_persistence_and_overflow" refid=”partition_persistent_overflow” >   
		<region-attributes disk-store-name="myPersistenceStore" disk-synchronous="true">     
			<eviction-attributes>       
				<!-- Overflow to disk when 100 megabytes of data reside in the region -->       
				<lru-memory-size maximum="100" action="overflow-to-disk"/>     
			</eviction-attributes>   
		</region-attributes> 
	</region>
```

The above indicates a partitioned region with default attribute: data-policy:  PERSISTENT-PARTITION with eviction-attribute of lru-heap-percentage and eviction-action of overflow-to-disk.  However, since with specified eviction-attributes, we are over-riding the default behavior.

```
	<region name="myReplicatedPersistentAndOverflowedRegion">     
		<region-attributes scope="distributed-ack"	data-policy="persistent-replicate">        
			<eviction-attributes>          
				<lru-heap-percentage action="overflow-to-disk"/>       
			</eviction-attributes>
		</region-attributes>
	</region>
```

The above chooses to bypass the region shortcut:  REPLICATE_PERSISTENT_OVERFLOW and simply specifies all attributes for a persisted replicated region with overflow.  In most cases you will also want to set the scope region attribute to distributed-ack although any of the scopes can be used with a persistent region.   For more information on configuring persistence and overflow, see 

http://gemfire.docs.pivotal.io/latest/userguide/index.html#developing/storing_data_on_disk/storing_data_on_disk.html

##How persistence works

###Overview

When a persistent region is created, either declaratively through the cache.xml or programmatically using APIs, it checks to see if persistence files already exist in the configured disk directories that it can recover from.

If it does not find any existing files, it creates new ones (see what files are created).

If it does find existing files, it initializes the contents of the region from the data found in those files.

Once recovery is complete, the region will have been created and can be used by the applications or clients. Any write operations executed against the region will write their entry data to disk. 

Entries are first written to an operation log, or oplog. Oplogs contain all of the logic operations that have been applied to the cache. Each new update is appended to the end of the current oplog. At some point, the oplog will be considered full, and a new oplog will be created. Updates to the oplog may be done either synchronously or asynchronously.

Because oplogs are only appended, your disk usage will continue to grow until the oplogs are rolled. When an oplog is rolled, the logical changes in the oplog are applied to the db files.

The advantage of this two staged approach is that the synchronous writes to disk that your application must wait for are only to the oplog. Because the oplogs are only appended to writes can be made to the oplog without causing the disk head to seek. 

Further reading on how persistence works, see http://gemfire.docs.pivotal.io/latest/userguide/index.html#developing/storing_data_on_disk/how_persist_overflow_work.html

###What disks and directories will be used?

By default, the current directory of the Java VM when it the process was started will be used for all persistent files. You can override this by setting the –dir attribute of the gfsh process when starting.  You can also configure multiple. This allows you to exceed the space available on a single file system and can provide better performance.

To declare the _disk-dirs_ in cache.xml add a _disk-dir_ sub-element to the _disk-dirs_ element you are adding.

Note that an optional _dir-size_ can also be configured. For further information, see 

http://gemfire.docs.pivotal.io/latest/userguide/index.html#managing/disk_storage/disk_store_configuration_params.htm
 
###Useful Information

A single Java VM may have more than one persistent or overflow region. Multiple regions in the same VM can all use the same disk-dirs without conflict. Each region will have its own DiskDirStatistics and its own dir\-size even though they are sharing the same physical disk directory.
 
###Be Careful

If multiple Java VMs want to share the same directory then they must not both use it for the same region. If they do, then the second VM that attempts to create the region will fail with a region already exists error. The best practice is for each VM to have its own set of directories.

###What files are created?

At creation, each oplog is initialized at the disk store’s max-oplog-size divided between the crf and drf files.  When it’s closed, GemFire shrinks the size of these files to the actual space used in each file.  After the oplog is closed, a krf file is created which contains the key names as well as the offset for the value within the crf file.  Although this krf file is not required at startup, if available, it improves startup by allowing GemFire to load the entry values in the background after the keys are loaded into memory. 
 
When an oplog is full, GemFire closes it and a new log with the next sequence number is created.

###File Extensions

<table>
  <tr>
    <td>FILE EXTENSION</td>
    <td>USED FOR</td>
    <td>NOTES</td>
  </tr>
  <tr>
<td>if</td>
<td>Disk store metadata</td>
<td>Stored in the first disk-dir listed for the store. Negligible size - not considered in size control.</td>
</tr>
<tr>
<td>lk</td>	
<td>Disk store access control</td>
<td>Stored in the first disk-dir listed for the store. Negligible size - not considered in size control.</td>
</tr>
<tr>
<td>crf</td>
<td>Oplog: create, update, and invalidate operations</td>	
<td>Pre-allocated 90% of the total max-oplog-size at creation.</td>
</tr>
<tr>
<td>drf</td>
<td>Oplog: delete operations</td>	
<td>Pre-allocated 10% of the total max-oplog-size at creation.</td>
</tr>
<tr>
<td>krf</td>	
<td>Oplog: key and crf offset information</td>
<td>Created after the oplog has reached the max-oplog-size. Used to improve performance at startup.</td>
</tr>
</table>

####Further reading 
Disk Storage operation logs:
 http://gemfire.docs.pivotal.io/latest/userguide/index.html#managing/disk_storage/operation_logs.html 

System startup with disk stores:
http://gemfire.docs.pivotal.io/latest/userguide/index.html#managing/disk_storage/how_startup_works_in_system_with_disk_stores.html#how_startup_works_in_system_with_disk_stores

###When is data written to disk?

Data is written to disk when any write operation is done on a persistent region.

The following table describes the region write operations: 

<table>
<tr>
<td>write operation</td>
<td> data written</td>
<td>methods</td>
</tr>
<tr>
<td> entry create </td>
<td>one oplog record containing the key, value and an entry id</td> 	
<td>create, put, putAll, get due to load, region creation due to initialization from peer </td>
</tr>
<tr>
<td>entry update</td>
<td>one oplog record containing the new value and an entry id</td>
<td> put(), putAll(), invalidate(), localInvalidate(), Entry.setValue()</td>
</tr>
<tr>
<td>entry destroy</td>
<td> one oplog record containing an entry id</td>
<td> remove(), destroy(), localDestroy()</td>
</tr>
<tr>
<td>region close</td>
<td>closes all files but leaves them on disk</td>
<td>close(), Cache.close()</td>
</tr>
<tr>
<td>region destroy</td>
<td>closes and deletes all files from disk</td>
<td> destroyRegion(), localDestroyRegion() </td>
</tr>
<tr>
<td>region clear</td>
<td>deletes all files from disk and creates new empty files</td>
<td>clear(), localClear()</td>
</tr>
<tr>
<td>region invalidate</td>
<td>does an entry update with a new value of null for every entry</td>
<td>invalidateRegion(), localInvalidateRegion()</td>
</tr>
</table>

Even if synchronous disk writes are configured, Geode only writes synchronously to the file system buffers, not the disk itself. This means that it is possible that some data is in the buffer when the machine crashes and it may never get written to disk. However, data is protected if the Java VM that is hosting the persistent region crashes.
 
####Be Careful

You can configure flushing these synchronous oplog writes to disk but it usually causes a significant performance decrease. If you are using very fast hard disk or solid state memory, you might choose to configure the oplog writes to flush. To configure flushing to disk set this system property gemfire.syncWrites to true.

###Asynchronous writes

Using asynchronous writes can give you better performance at the cost of using more memory (for buffering) and the risk of your data still being in the Java VM's object memory after your write operation has completed.

Instead of immediately appending to the current oplog like a sync write, async writes add the current operation to an async buffer. When this buffer is full (based on bytes-threshold), or when its time expires (based on time-interval), or when it is forced (by calling writeToDisk) it will be flushed to the current oplog. The flush takes all the ops currently in the buffer, copies them all into one buffer, and appends that buffer to the current oplog with a single disk write.

When operations are added to the async buffer conflation may occur to those updates in memory. For example if the async buffer already contains a create for key X at the time a destroy of key X is done then the buffer ends up having nothing for key X and no writes to disk are needed. Or if key X is modified five times before the async buffer flushes then only the most recent modify is kept in the buffer and it is the only one written to disk when the flush occurs.

###How domain data is written on disk

A persistent region writes every key and value added to a region to disk. It does this be serializing the keys and values. See the developer's guide for information on how to serialize your data.

##Performance

###Network File Systems

Keep in mind that if the directories you configure for persistence are on a network file system then the persistence writes will compete for network bandwidth with GemFire data distribution.
If a network file system is going to be used, it is best for the data directory to be on local disk(s). 

###Statistics related to disk persistence
 
See
http://gemfire.docs.pivotal.io/latest/userguide/index.html#managing/statistics/statistics_list.html

####DiskDirStatistics

DiskDirStatistics instances can be used to see how much physical disk space is being used by persistent regions. 

<table>
<tr>
<td>statistic</td>
<td>description</td>
</tr>
<tr>
<td>dbSpace</td>
<td>measures the space, in bytes, used by db files</td>
</tr>
<tr>
<td>diskSpace</td>
</td>measures the space, in bytes, used by db files and oplog files</td>
</tr>
</table>


An instance of DiskDirStatistics will exist for each directory on each persistent region. Its name is the name of the region followed by a directory number. The first directory is numbered 0, the second one 1, etc.

The space measured by these statistics is the actual disk space used, not the space reserved. Each time an oplog is created an attempt is made to reserve enough space for it to grow to its maximum size. Various operating system utilities will report the reserved space as the size of the file. For example on Unix ls -l reports the reserved size. This number will not change for the lifetime of the oplog. However the actual disk spaced used does change. It starts at zero and keeps increasing as records are appended to the oplog. This is the value reported by diskSpace. You can also see this value with operating system utilities. For example on Unix du -s reports the used size.

####DiskRegionStatistics

DiskRegionStatistics instances describe a particular persistent region. The name of the instance will be the region name it describes.

statistic 	description 
entriesInVM 	The current number of entries with a value in stored in the VM. For a persistent region every value stored in the VM will also be stored on disk.
entriesOnDisk 	The current number of entries whose value is stored on disk and not in the VM. All recovered entries are in this state initially and evicted entries.
rollableOplogs 	Current number of oplogs that are ready to be rolled. They are ready when they are no longer being written to even if rolling is not enabled. 
writes 	The total entry creates or modifies handed off to the disk layer. 
writeTime 	The total nanoseconds spent handing off entry creates or modifies to the disk layer. 
writtenBytes 	The total bytes of data handed off to the disk layer doing entry creates or modifies. 
reads 	The total entry values faulted in to memory from disk. For a persistent region this only happens with recovered entries or entries whose value was evicted. 
readTime 	The total nanoseconds spent faulting entry values in to memory from disk. 
readBytes 	The total bytes read from disk because of entry values being faulted in to memory from disk. 
removes 	The total entry destroys handed off to the disk layer. 
removeTime 	The total nanoseconds spent handing off entry destroys to the disk layer. 
statistic 	description 
rolls 	Total number of completed oplog rolls to the db files. 
rollTime 	Total amount of time, in nanoseconds, spent rolling oplogs to the db files. 
rollsInProgress 	Current number of oplog rolls to the db files that are in progress. 
rollInserts 	Total number of times an oplog roll did a db insert (also called a create). 
rollInsertTime 	Total amount of time, in nanoseconds, spent doing inserts into the db during a roll. 
rollUpdates 	Total number of times an oplog roll did a db update (also called a modify). 
rollUpdateTime 	Total amount of time, in nanoseconds, spent doing updates to the db during a roll. 
rollDeletes 	Total number of times an oplog roll did a db delete. 
rollDeleteTime 	Total amount of time, in nanoseconds, spent doing deletes from the db during a roll. 
statistic 	description 
recoveriesInProgress 	Current number of persistent regions being recovered from disk. 
recoveryTime 	The total amount of time, in nanoseconds, spent doing recovery. 
recoveredBytes 	The total number of bytes that have been read from disk during recovery. 
oplogRecoveries 	The total number of oplogs recovered. A single recovery may read multiple oplogs. 
oplogRecoveryTime 	The total amount of time, in nanoseconds, spent doing an oplog recovery. 
oplogRecoveredBytes 	The total number of bytes that have been read from oplogs during recovery. 
statistic 	description 
flushes 	The total number of times the async write buffer has been written to the oplog. 
flushTime 	The total amount of time, in nanoseconds, spent doing a buffer flush. 
flushedBytes 	The total number of bytes flushed out of the async write buffer to the oplog. 
bufferSize 	The current number of bytes buffered to be written by the next async flush. 
statistic 	description 
openOplogs 	Current number of open oplogs this region has. Each open oplog consumes one file descriptor. 
oplogReads 	Total number of oplog reads. An oplog read must be done to fault values in to memory that have not yet rolled to the db files. 
oplogSeeks 	Total number of oplog seeks. Seeks only need to be done for oplogReads. Reads done on the active oplog require two seeks, all other reads require one seek. 
statistic 	description 
dbWrites 	Total number of writes done to the db files. 
dbWriteTime 	Total time, in nanoseconds, spent writing to the db files. 
dbWriteBytes 	Total number of bytes written to the db files. 
dbReads 	Total number of reads from the db files. 
dbReadTime 	Total time, in nanoseconds, spent reading from the db files. 
dbReadBytes 	Total number of bytes read from the db files. 
dbSeeks 	Total number of db file seeks. 

####CachePerfStatistics

The CachePerfStatistics instance has a statistic named rollsWaiting which tells you how many of this VM's disk regions are ready to roll an oplog to the db files but are waiting for a thread to be available to do this work.
