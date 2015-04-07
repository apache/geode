#What To Monitor in Geode

##Introduction

This is a summary document. You should always refer to the Geode documentation on the Pivotal site for all official support and information. It is far more in-depth and detailed. What this document can do is provide a quick hit of things to look out for when dealing with DevOps lifecycle issues. Within every organization, as the use of Geode matures, it’s usage will generally expand. This often means the original Use Case is no longer applicable. That’s where this document comes in. It was written to hopefully fill the gap between initial Geode implementation and the seasoned Geode user. Hopefully this will tell you what to look our for before you get to critical mass.

This document addresses a typical distributed system comprised of Geode Locator(s) and Geode Data Fabric which includes Senders, Receivers and dedicated Servers. Operational monitoring of the Geode Data Fabric is imperative to insure continuous delivery of services. Other topologies are available, including Peer-to-Peer with and without a locator as well as various WAN configurations.

Guidance for [[Sizing a Geode Cluster]] are available and it is assumed that these have been evaluated and implemented. This document is primarily concerned with Strategic Operational Capacity Planning. The difference is between what was planned in the implementation of the Geode cluster versus the reality of daily usage. In the best Agile tradition, this document attempts to summarize the primary pain points when it comes to the DevOps portion of Geode and how to adjust as the Use Case changes.  This document is based on real life experiences with Geode over a number of years, not theoretical discussions. 

Most of these monitor points should be obvious to most experienced Capacity Planning  teams, however, there are instances where the distributed nature of Geode introduces monitoring requirements not found in traditional databases.


###Assumptions

This document assumes you are using the recommended Oracle JDK. Although the metrics listed herein are inclusive of all topologies, you can pick and choose which metrics best fit your installation needs. Although the subject of migration and upgrades are mentioned, that subject is a separate issue and the Best Practices for that subject is addressed in the Geode documentation provided online.

It is also assumed the reader has a deeper understanding of the Geode architecture and implementation. For instance, while it is not expected that the reader understand the hashing algorithm used to distribute data to the buckets in a Geode cluster, it is  expected the reader understands the role of buckets in the cluster and the consequences of having too many or to few. 
	##Quickstart

You may not have either resources or the tools to monitor Geode in ways recommended here. Maybe you just want to lay down a simple monitoring tool and not do any alerting - or maybe you don’t want to read through all of this and want a quick and dirty way to set up monitoring.  If so, do the following:
+	Install and run Pulse to monitor your cluster
+	Read Section 2 on Daily Care and Feeding Questions
+	Employ a triage strategy per your Geode Best Practices when you encounter an issue.
 
###Baseline server

As a starting point, if you had to have some idea of what you might need in the way of hardware resources, a starting point is the following:
To process at least 2000 tps, a single Geode server should have:

+128G ram
+8 cores at 3.8 Ghz

Disk space is variable but must be SAN, not NAS.

##What are the questions to ask about monitoring a Geode cluster?

The questions below are the primary concerns for an Operations organization. Usually, after the cluster is deployed, you have a group assigned to monitor the cluster to assure it availability and operation integrity. Invariably, every Geode customer asks the same question: What do I monitor?

###System is nominal.

This is the goal of every Operations team. System is nominal means it’s running fine and is your goal. However, monitoring every data point is tedious and not necessary. Instead, there are critical metrics you can review and monitor for levels of throughput, usage and utilization. The questions below represent the normal questions you might ask yourself on a constant basis about a Geode cluster.

###The JMX server and the log files are the most important tools you have. 

Use them. Plug in your monitoring tool such as SiteScope, Wiley or IPSoft to the JMX server. If you have an aggregation tool that scrapes your log files and provides query capability, employ it and generate alerts per your organizational requirements. Splunk does a great job doing this, for instance.

###The Questions

####Are CPUs too busy?

Review the CPU usage. Do you have some setting that may be causing For instance, there has been cases where CPU’s were running hot because their CMSInitiatingOccupancyFraction was set too close to the actual heap usage and Garbage Collection was running all the time. Run netstat or TOP to review processes.  

Receommnded action:

If the CPU is running above 80%, then look at the garbage collection processes. If garbage collection is constantly running, that may be why you see this level of usage. Constantly running garbage collection on the Eden space is not necessarily a bad thing – if you are optimized for low latency. But, if you are optimized for throughput, the converse could be true. Other places to look: the TPS rate. If you are processing 15-20k tps (or a comparatively high rate of tps for your cluster), you may need to add another server or beef up your current machines. 

Remember: If you are using a virtualized environment, expect at least a 20% drop in performance. Deploy on non-virtualized servers if you really need to squeeze every millisecond out of the server. 

####Is the member running out of file descriptors?

If you are running on RHEL, what does the ulimit command report? ulimit covers a lot of values and they should be reviewed. Depending on the *nix version and vendor, the values might include:

+The maximum size of core files created. 
+The maximum size of a process's data segment. 
+The maximum size of files created by the shell(default option)  
+The maximum size that may be locked into memory.        
+The maximum resident set size.    
+The maximum number of open file descriptors.        
+The pipe buffer size.        
+The maximum stack size.     
+The maximum amount of cpu time in seconds.     
+The maximum number of processes available to a single user.       
+The maximum amount of virtual memory available to the process. 

Microsoft Windows implementations of Geode should refer to their documentation for handling this.

On *nix systems, set this to at least 50k but unlimited is generally suggested.


####Is the load average too high?

Generally, a Geode SME can give you reasonable estimates for :

+The amount of memory you require for a cluster determined by Partitioned and Replicated databases and redundancy
+The number of CPU’s 
+Severs required based on your N+1 Redundancy strategy
+Predicted Growth

However, there may be instances where certain servers are too loaded. For instance, improper partitioning hashes, failure to properly co-locate data or maybe one server has less resources available to being with are causing load imbalances.

Recommended level: Heap usage should be at 50% consumption or below for optimum performance.

####Is there unexpected memory growth?

What was the Capacity Plan for the cluster? Is there some reason why you are adding more objects to the cluster? Maybe there is a runaway thread that is filling up the heap space? Use your forensic skills to determine the issue.

Recommended action: If your cluster heap usage jumps over 10% unexpectedly, you may have a problem. Review to see if a batch of new entries have been added. 

####Is the member running out of heap?

A lot of times, as Geode clusters mature, it simply fills up over time. This is remedied easily by adding a new member and rebalancing. But what if a particular member is filling up out of proportion to other members? 
Maybe it’s time to rebalance. If that doesn’t work, examine the log files. Are all the cluster members evenly loaded?

Recommended setting: Heap usage must be below 50% or less for optimum performance.

####Are GCs taking too long?

What is the GC policy set? Is it the recommended setting for Geode? If so, remember Concurrent Mark and Sweep only affects Tenured space. What is the policy you have set for Eden or Survivor space? CMS has a number of accompanying flags. Check that they are set according to the Geode User Guide recommendations for your particular topology and implementation.

Recommended action: If Eden space (Young Generation) garbage collections are taking longer than 100-150ms, review your Eden space sizing and overall settings. Java will perform minor garbage collections constantly if you are adding or updating regions. This affects promotions to Survivor and Tenured spaces. If major gcs are taking longer than 200-300ms, review the recommended CMS settings for your JVM.

####Is there a client load imbalance?

What clients are connecting and what activities are occurring? Is one of the members more active? Turn up the logging levels or gather statistics to conduct an investigation.

Recommended actions: Review the key distribution. Check network availability to the servers. Review the client and server log files. There should not be more than a 5-10 percent load distribution difference on your servers.

####Are clients unexpectedly disconnecting?

The best way to monitor this is to look at the log files generated in the cluster. If your organization uses aggregation tools like Splunk, use its query tools to grep out the messages indicating disconnects. Is there a pattern? Does it always seem to occur from a certain part of the network?

Recommended action: Perform a NetTrace from the client to the servers. This will tell you all the router hops you are taking and how long. If you are getting hops that are taking more than 10ms, you need to review the network settings with your infrastructure team. 

####Are clients getting no or slow service?

There a myriad of reasons, but you might start by looking at whether you have separated client and server traffic with different bind addresses for clients and the server traffic. Geode servers have gossip data – they share metadata with each other, they check connectivity – lots of things. If you have a number of servers, maybe the servers are using all the network bandwidth and the clients are not getting enough resources. Server traffic can “starve” the clients.

Recommended action: Review the sizing of all the servers. If one server is slow, it can slow down the whole cluster. If you are constantly seeing client timeouts, review the read-timeout (default 10 seconds), free-connection-timeout, retry-attempts. Also, check the socket-buffer-size. If you are sending datagrams as part of stream that are too large for the IP settings, these datagrams will be broken into fragments and reassembled. Prevent this by setting the proper socket-buffer-size.

####Are the expected number of members in the distributed system?

Take an inventory of the deployed cluster. If you use a DevOps tool like Puppet, write a script to compare the Puppet Master database with the actual deployed members. Do they match?
Recommended action: Run an inventory review at least once an hour.

####Is a peer unresponsive?

Network issues are the bane of any distributed system and Geode is not immune. In fact, Geode is sensitive to the network. It constantly requires peer members to check in and assure they are alive. Members will attempt to connect multiple times before reporting to the locators (if employed) that a member is unresponsive. The locator will attempt to establish communications with a member. If unsuccessful, the cluster will shun the unresponsive member. In the newest releases, departed members can rejoin but in older versions, depending on your topology, redundancy and HA settings, it may be necessary to simply start up a new member and rebalance.

Recommended action: Check the member timeout setting. The default is 5 seconds and generally the cluster will not start the process of shunning a member for at least 2-3 times that long. Run a NetTrace. If there a lot of gateways and routers in the network between the servers, this may be the issue. 

####Are the partitioned regions' primary buckets balanced?

Do you have custom partitioning implemented? Are you partitioning on key fields that don’t distribute well? These are the things you might look at. 

Recommended action: If there is a greater than 5-10% difference in bucket balance, then rebalance and re-evaluate.

####Is the partitioned regions' put rate too low?

Socket buffer sizes, MTU’s, ulimit values – any of these could cause issues. If you have synchronous persistence turned on, maybe the SAN storage is responding too slowly. Turn your logging level up and investigate accordingly.

Recommended action: If the put rate is 10% below the performance benchmark, you probably have an issue. Review the following: 
+NetTrace the cluster. 
+Review logs. Look for messages about client disconnects.
+Review heap usage.  If it is above 50%, it’s time to widen the cluster.
+Have the infrastructure team look at shmmax and shmall on *nix systems (similar values on Windows). If they are too large, there can be memory page locking issues on high update systems.
+Check the JVM settings. If you are using Java Large Pages you could be causing memory locking issues.


##Metrics
The metrics listed are available from a number of sources:  

+Geode JMX server
+Pulse

We include a Metric name and Metric Description of the primary metrics that should be monitored. In general, these metrics should not be alerted on. 

There are a number of monitoring tools available and Geode itself provides a JMX server that you may query. Writing your own direct querying approach of the JMX server is not recommended but there are instances of third party products that have been enhanced to provide a view into the JMX server and the metrics maintained by Geode. This document doesn’t recommend any particular third party product as the tools provided by VMware and Pivotal do allow for a reasonable view of the health of the cluster. 

##Migration points to remember

###Scaling

The Capacity Planning team should be evaluating the trends of these metrics. If the values for the metrics began to consume all available resources, such as Heap space is consistently at 95% usage, then the team should already have a plan prepared for scaling the Data Fabric that has been thoroughly tested in the EXACT same environment as the Production environment, approved and signed off on by the Development team, QA, Operations and management. Failing to provide an EXACT testing environment replica of the Production environment will often result in the catastrophic failure of  any scaling plans.
 
###Point Release Compatibility

Geode can easily be scaled horizontally and can even co-exist with different point releases side by side. For instance, any 7.0.x release should be completely compatible with a different point release. Major releases will NOT be compatible, however. For instance. A 7.0.x release will not be compatible with a 7.5 release.
 
##Primary Monitoring points

These metrics are the primary points to examine when evaluating for the purposes of Capacity Planning. While it is true that many of these metrics should be alarmed on when they reach a certain level, the purpose in noting them here for monitoring is strictly to watch trends. The final section is a list of possible monitoring points by category. Most of the items are self evident. It is recommended that these be monitored on a as-needed basis. For instance, it is not necessary to dive deeper into Bucket Size metrics for Queue issues, however, disk space would be interesting when it comes to queue sizes as they will overflow to disk. 


###Tenured Gen Heap Used 

This is the main memory space to monitor.

This area is for long-lived objects and the primary area where Geode objects live, especially if it is the Database of Record. They will move to the Tenured spaces if they survive a certain number of GC cycles over time. The primary reason for monitoring this space is to examine whether it is too large or small for the particular use case. If Geode is being used for a Reference database, Postal Codes for instance, then it is likely the Tenured space will be sized to meet the bare requirements as static tables have few changes. However, if you are employing Geode as a temporary cache, then the Tenured space may well need to decrease as Survivor space and Tenured space become less important. 


+If Tenured Gen heap used is mostly on or above 50% 

Levels this high require action. Increase the available memory, preferably by adding another Geode server.

###Monitor CMS GC collection time.

Using the StatSampler jvmPauses stat (available via the MemberMXBean) allows you to look at whether there are too many GC’s occurring. Garbage Collection is a complex subject, but heap memory moves from one area to another. The JVM specification doesn’t compel the implementation to look exactly the same – it’s flexible enough to allow different implementations to innovate as they see fit, so the IBM implementation looks different from either the Azul or Oracle implementations. The important take away is to understand that Garbage Collection can slow, or stop, your JVM from processing while it clears old objects or promotes them to other areas. 

+If Young gcs are greater than 100-150ms , review the JVM gc settings. 
+If major gcs are taking more than 300-400ms, review the JVM gc settings. 

It may be time to increase capacity by adding another server or your JVM gc settings are incorrect.



###Process CPU Time

####Process CPU time on a Virtual deployment

Assuming that the Geode cluster  is deployed on a virtual machine, usually you have the ability to assign CPU’s to a cluster and further assuming each CPU has multiple cores, then you have the ability to assign as many cores as you might think necessary. However, between allocating threads to handle Garbage Collection, various synchronous and asynchronous queues, senders, receivers, number of client connections per server and more, it becomes obvious that what was once considered a robust allocation of resources is now inadequate. In times past, vertical scaling was the solution to alleviating the issues surrounding resource consumption, however, given the distributed nature of Geode and it’s ability to partition, horizontal scaling in almost always the best solution. Essentially, you can allocate another cache server to the cluster and rebalance. The new server will assume it’s responsibilities as necessary, according to the load balancing of the locators if they are used, or by some other means if you are using a hardware or software load balancing solution.  

The processCpuTime shows nanos per second. So, you need to divide by the number of CPUs to see the actual percentage. An alternative is the cpuActive stat (available through MemberMXBean.getHostCpuUsage).

+If Process CPU Time is mostly on or above 95% 

Levels this high require action. Review to see if gc is constantly running. 
Review tps load. It may be time to widen the cluster.

####Process CPU time on Commodity hardware deployment

Geode can be deployed on commodity hardware and it will function just fine, but the ability to scale does become more difficult depending on the environment. Generally, commodity hardware is used for proxy servers that forward traffic onto more robust machines. The important thing to remember is that Geode is sensitive to network changes and each expansion much be carefully planned.

+If Process CPU Time is mostly on or above 95% 

Levels this high require action. Review to see if gc is constantly running. 
Review tps load. It may be time to widen the cluster.




##Network Partition

Also known as split brain, this is a situation where one server departs from the cluster. Generally, unless the server can recover and rejoin according to the specifications of the Geode configuration in the max-wait-time-reconnect and max-num-reconnect-tries, Geode should be configured to detect network partitioning via enable-network-partition-detection in order to protect the integrity of the data. Constant or regular instances of network partition is indicative of fundamental operational issues surround either the physical servers or the network deployment. In either case, network partitions are a serious event and should be treated as such. They should not be happening if at all possible, but when they do, it is important to discover the root cause and take immediate action to prevent future instances.

Note: If any of the above conditions are seen, you would need to perform additional analysis. If it is related to a capacity issues caused due to actual utilization, you need to see if the issue requires a configuration change or to horizontal scale the cluster.

 You could ask your Operations group for the statistics files from your Geode Data Fabrics servers and analyzing the capacity planning statistics in the Visual Statistics Display (VSD).
 	
For details on installing and using VSD, please refer Geode documentation. 

##VSD

VSD is extremely helpful and it is recommended you keep the statistic-sample-rate at 1000ms. Also, it isn’t really necessary to keep more than 24 hours of statistical data on hand. A note about statistics files: if you specify the .gz ending, Geode will automagically create a compressed file for you. Do not use the .gz format. There is currently a bug in the formatting of the file and renders them unreadable. 

Also, do not create unwieldy sized gfs files. The entire file is read into memory and if you attempt to use these files and they are very large, an inadequate amount of ram will result in your machine being locked up trying to load all the data, hence reducing the sample time is not a good idea as the VSD files will become too larger to be useful.

##Hotspots

These “hotspots” means that perhaps one or two servers are doing all the workload, which defeats the purpose of a distributed system. The reasons vary but generally include:

+Custom partitioning was implemented incorrectly 
+A key was selected to partition on that was not suitable. 
+The number of buckets selected was too small for the cluster size
 
##Filing Support Requests

Occasionally it will be necessary to file Support Requests. The following items are extremely helpful in resolving your issues:

+Log files from affected cache servers and locators 
+VSD files
+Screen shots of any relevant issues (from Pulse, Hyperic, etc.)
+Copies of the cache.xml and gemfire.properties files.
+A complete description of the events surrounding the issue

The SR site has the ability to accept your files, however, it is imperative that when you upload the files, they are loaded into the directory you create, which is the SR number assigned to your issue. Failure to do this results in your files being scattered and unavailable.

##Additional Geode Metrics by category (Suggested for Capacity Planning) 

###Trending Evaluation

Capacity Planning teams may be more interested in this area. None of the metrics below have recommended values to look for. Review of these metrics requires you to observe trends. For instance, suppose your organization uses SiteScope. SiteScope can and does capture sampling metrics over time that you can graph and evaluate. Depending on the area you are examining, you may notice trends that show an increasing usage of resources. This should signal to the team that maybe it’s time to consider scaling the cluster. 

Use the metrics below to evaluate the Planned versus Actual Usage and reconsider the Capacity Plan accordingly.

**Recommended action on all metrics: If you see performance degradation greater than 20% on any metric over time or if you see resources consumed at 80% (except Heap, which should remain at or below 50%), you need to expand capacity.**

###Eden Heap Used Memory

The primary reason you might look here is to see if the promotion rate from Eden space to a Survivor space is occurring too often. This can be the result of setting the Eden space too small and an object that you really don’t want promoted goes into Survivor space because the Eden space is filling up. Although this is not a primary area of concern for Geode performance, frequent promotion DOES slow down performance. 

+Free Memory
+Heap Committed Memory
+Perm Gen Non-Heap Used Memory
+Survivor Heap Used Memory
+Tenured Gen Heap Used Memory
+Average MarkSweep Collection Time
+Average Scavenge Collection Time
+File Descriptor Limit
+MarkSweep Collection Time
+MarkSweep Collections
+MarkSweep Collections per Minute
+Max Memory
+Non-Heap Committed Memory
+Number of CPUs
+Number of Threads
+Peak Threads
+Process Cpu Time
+Total Memory

###Disk Space

This should be self evident. If you find you are rolling files off too frequently or constantly allocating more space, you should look at possibly allocating more space in larger increments.
ALWAYS USE SAN OR LOCAL STORAGE. DO NOT USE NAS.

+Maximum Space (In)

Recommended action: If you are above 80% usage, then consider adding more space or archiving older files.

###Removes per Minute

This allows you to re-examine your initial assumptions about the cluster. If you assumed that the cluster was going to be read –only and you are seeing increased Write or Delete activity, it may be time to re-think the deployment topology.

+Writes per Minute
+Read Bytes per Minute

###Reads per Minute

The reasons for reviewing this metric are similar to the previous metric. Review and plan accordingly.

+Writes per Minute
+Average Flush Time
+Average Read Time
+Average Write Time
+Compact Deletes per Minute
+Compact Inserts per Minute
+Compact Updates per Minute

###Event Queue Size

+Events Processed per Minute
+Events Received per Minute
+Events Processed
+Events Received
+Events Distributed per Minute
+Events Queued per Minute
+Events Distributed
+Events Queued


###Average Bucket Size

All data is stored in buckets. For HA, purposes, it’s always better to not overload a resource with too many assets – having too few buckets may mean if the member departs, it may take a lot of data with it. If your redundancy is set, you may end up with members large amounts of data. Conversely, having too many buckets can be harmful as you end up with lots of overhead for maintaining all the metadata for each and every bucket. Remember, if you have redundant copies set to 1, there are just as many secondary buckets as primary buckets. 

Review this metric and plan accordingly.
	
+Bucket Count
+Primary Bucket Count
+Actual Redundant Copies
+Configured Redundant Copies
+Get Entry Completed
+Get Entry Completed per Minute
+Get Entry Time
+Get Entry Time per Minute
+Get Retries
+Get Retries per Minute
+Get Time
+Get Time per Minute
+Gets Completed
+Gets Completed per Minute
+Max Bucket Count
+Max Bucket Size
+Partition Messages Processed
+Partition Messages Processed per Minute
+Partition Messages Received
+Partition Messages Received per Minute
+Partition Messages Sent
+Partition Messages Sent per Minute
+Primary Transfer Time
+Primary Transfer Time per Minute
+Primary Transfers Completed
+Primary Transfers Completed per Minute
+Primary Transfers Failed
+Primary Transfers Failed per Minute
+Primary Transfers In Progress
+Put Time
+Put Time per Minute
+Total Bucket Size

###Connection Wait Time per Minute
+	Connection Waits per Minute
+	Connection Wait Time
+	Connection Waits
+	Connections
+	Connects
+	Connects per Minute
+	Disconnects
+	Disconnects per Minute
+	Locator Requests
+	Locator Requests per Minute
+	Locator Responses
+	Locator Responses per Minute
+	Locators
+	Min Pool Size Connects
+	Min Pool Size Connects per Minute
+	Pool Connections
 


###Gets per Minute
+	Misses per Minute
+	Puts per Minute
+	Updates per Minute
+	Average Get Time
+	Average Put All Time
+	Average Put Time
+	Average Query Execution Time
+	Average Update Time
+	Event Queue Size
+	Event Threads
+	Gets
+	Misses
+	Partitioned Regions
+	Puts
+	Update Time
+	Update Time per Minute
+	Updates



###Rebalances Completed per Minute
+	Critical Threshold
+	Eviction Stop Events
+	Eviction Stop Events per Minute
+	Heap Critical Events
+	Heap Critical Events per Minute
+	Rebalance Bucket Transfers Completed
+	Rebalance Bucket Transfers Completed per Minute
+	Rebalance Bucket Transfers Failed
+	Rebalance Membership Changes
+	Rebalance Membership Changes per Minute
+	Rebalances Completed
+	Tenured HeapUsed
