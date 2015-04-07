## Overview


Sizing a Geode deployment is a process that involves some number crunching, as well as experimentation and testing. To arrive at reasonably accurate values for the key sizing parameters that will work well in practice, some experimentation and testing is required involving representative data and workload, starting at a very small scale. 

One major reason for this is memory overhead that can vary greatly due to variations in data and workload. It is impossible to calculate this overhead precisely, as it is a function of too many variables, many of which stem from the Java runtime environment (JVM) and its memory management mechanism.

As the primary means of data storage in Geode, memory is the first resource to consider for sizing purposes. When the memory requirements are satisfied, and the adequate cluster size determined, usually most everything else falls into place, and often only small adjustments may be needed to cover all the other required resources and complete the sizing process. The reason for this is, horizontal scaling to satisfy memory requirements also scales out all the other hardware resources, the CPU, network and disk. Typically, it’s the memory that drives horizontal scaling, but it can be any of these resources. In addition to these hardware resources, there are also soft resources to consider. The most important ones are file descriptors (mostly for sockets in use), and threads (processes). 

In a nutshell, the sizing is done by deploying a small representative data set and workload in a small cluster, tuning it to the desired performance, then scaling out while making sure the key performance metrics stay within the desired SLA. If there are sufficient resources available to test at full scale, that is ideal. Otherwise, scaling out can be done multiple times, a few nodes at a time, to provide more data points for a projection to the full scale. This is an iterative process that involves analysis and tuning each step of the way. The analysis can be aided greatly by Geode statistics. 

For large scale deployments involving large data volumes, the rule of thumb is to scale vertically as much as possible (how much may well depend on the desired SLA around node failure), in order to fit as much data as possible in a single Geode instance. That helps keep the cluster size down. 

## Requirements and Assumptions

To do the sizing as accurately as possible, and avoid surprises in production, you will need to run some tests to characterize the memory and other resource usage under a representative workload. That will require the following:
* A subset of representative data, the more closely matching the real data the better. 
* A matching subset of workload that matches the production workload as closely as possible. 
* Hardware resources for testing, ideally the same category as would be used in production (the same CPU, memory, network, and disk resources per node). At a minimum you will need to be able to run 3 Geode data nodes just to start, and then add at least a few more nodes to be able to validate the scalability. You will also need the same number of hosts for Geode clients to be able to create an adequate workload.

It is assumed that the documented best practices will be followed, such as the JVM GC configuration (CMS and ParNew), and that currently supported platforms will be used. 

Familiarity with key Geode concepts and features, such as partitioned regions, serialization, and so on, is also assumed.  

## Architectural and Design Considerations

Before a sizing effort can start, the overarching architectural decisions have to be made, such as what Geode regions to use for different types of data, and what redundancy level. Perhaps, some of the architectural and design decisions can be made based on the results of sizing. In other words, sizing can inform architectural and design decisions for which multiple options are possible. 

#### Serialization

One particularly interesting topic in this context is the choice of serialization, as it can make a big difference in the per-entry data overhead in memory, and subsequently in the overall memory requirements. Geode’s PDX serialization is worth mentioning here, as it is a serialization format that keeps data in a usable serialized form. It allows most operations on data entries without having to deserialize them, resulting in both space and performance benefits. These qualities make the PDX serialization the recommended serialization approach for most use cases.  

DataSerializable is another Geode serialization mechanism, and it is also worth mentioning as it is more space efficient than either PDX or Java Serializable. However, unlike PDX, it requires deserialization on any kind of access.  

#### Per-entry Memory Overhead

Listed below are factors that can have significant impact on the memory overhead for data on a per entry basis, as well as performance:
* Choice of Geode region type. Different regions have different per entry overhead. This overhead is documented (see below), and is also included in the sizing spreadsheet.
* Choice of the serialization mechanism. Geode offers multiple serialization options, as well as the ability to have values stored serialized. As mentioned above, Geode PDX serialization is the generally recommended serialization mechanism due to its space and performance benefits.
* Choice of Keys. Smaller and simpler keys are more efficient in terms of both space and performance.
* Use of indexes. Indexing incurs a per entry overhead, as documented in the below mentioned section of the User’s Guide. 

The section [Memory Requirements for Cached Data](http://gemfire.docs.pivotal.io/latest/userguide/index.html#reference/topics/memory_requirements_for_cache_data.html) of the GemFire User’s Guide provides more detailed information and guidelines on this topic.

If the data value objects are small, but great in number, the per-entry overhead can add up to a significant memory requirement. This overhead can be reduced by grouping multiple data values into a single entry or by using containment relationships. For instance, you may choose to have your Order objects contain their line items instead of having a separate OrderLineItems region. If this option is available, it is worth considering as it may yield performance improvements in addition to space savings. 

#### Partition Region Scalability

Geode partitioned regions scale out by rebalancing their data buckets (partitions) in order to distribute the data evenly across all available nodes in a cluster. When new nodes are added to the cluster, rebalancing causes some buckets to move from the old to the new nodes such that the data is evenly balanced across all the nodes. For this to work well, so that the end result is a well balanced cluster, for each partitioned region there should be at least one order of magnitude more buckets than data nodes. In general, the more buckets the better the data distribution. However, since the number of buckets cannot be changed dynamically, without downtime, it has to be chosen with the projected horizontal scale-out taken into account. Otherwise, over time as the system scales out, the data may become less evenly distributed. In the extreme case, when the number of nodes exceeds the number of buckets, adding new nodes has no effect; the ability to scale out is lost.

Related to this is the choice of data partitioning scheme, the goal of which is to yield even data and workload distribution in the cluster. If there is a problem with the partitioning scheme the data (and likely the workload) will not be evenly balanced, and the scalability will be lost.

#### Redundancy 

Choice of redundancy may be driven by data size, and whether data can be retrieved from some other backing store or Geode is the only store. Other considerations might go into that decision as well. For instance, Geode can be deployed in an active/active configuration in two data centers such that each can take on the entire load, but only will do so only if necessitated by a failure. In deployments like that typically there are 4 live copies of the data at any time, 2 in each datacenter.  A failure of 2 nodes in a single datacenter would cause data loss in that datacenter, but the other datacenter would take over the entire workload until those 2 nodes can be restored. Another possibility might be to set redundancy to 2 (for a total of 3 copies of data) in order to have high availability even in case of a single node failure, and avoid paying the price of rebalancing when a single node fails. Instead of rebalancing, a failed node is restarted, and in the meantime there are still 2 copies of data.

#### Relationship between horizontal and vertical scale

For deployments that can grow very large, it is important to allow for the growth by taking advantage of not just horizontal scalability, but also the ability to store as much data as possible in a single node. Geode has been deployed in clusters of over 100 nodes. However, smaller clusters are easier to manage. So, as a general rule, it is recommended to store as much data as possible in a single node while maintaining a comfortable data movement requirement for re-establishing the redundancy SLA after a single point of failure. Geode has been used with heaps of well over 64GB in size, and this trend is on the rise. 

###### NUMA Considerations

One thing to consider when deciding on the JVM size (and VM size in virtualized deployments) is the Non-Uniform Memory Architecture (NUMA) memory boundaries. Most modern CPUs implement this kind of architecture where memory is carved up across the CPUs such that memory directly connected to the bus of each CPU has very fast access whereas memory accesses by that same CPU on the other portions of memory (directly connected to the other CPUs) can pay a serious (as much as 2X slower) wait-state penalty for accessing data. An example is a system that has 4 CPUs with 8 cores each and a Non-Uniform Memory Architecture that assigns each CPU its own portion of the memory. Lets say that the total memory on the machine is 256GB. This means that each NUMA node is 64GB. Growing a JVM larger than 64GB on such a machine will cause wait-states to be induced when the CPUs need to cross NUMA node boundaries to access memory within the heap. For optimal performance, Geode JVMs should be sized to fit within a single NUMA node. 

#### Geode Queues

If any Geode queueing is used, such as for WAN distribution, client subscription, or asynchronous event listeners, it is important to consider the queues’ capacity in the context of the desired SLA. For example, for how long should gateway or client subscription queues be able to keep queueing events when the connection is lost? Given that, how large should the queues have to grow? The best way to find out is by watching the queues’ growth during sizing experiments, using Geode statistics (more on this in Vertical Sizing section of The Sizing Process, below) .

For WAN distribution it is important to consider the distribution volume requirements, and ensure adequate network bandwidth sizing. If sites connected via the WAN gateway may be down for extended periods of time such as days or weeks it will mean that you will need to overflow the gateway queues to disk, and ensure you have plenty of disk space for those queues. If you don’t have plenty of disk you may have to shut off the Gateway senders to prevent running out of disk. 

## The Sizing Process

The following are the steps in the sizing process:

1. **Domain object sizing** is done to get an entry size estimate for all the domain objects, which, together with number of entries, is used in estimating the total memory requirements.   
2. **Estimating total memory and system requirements**. Based on the data sizes, we can estimate the total memory and system requirements using the sizing spreadsheet, which takes into account Geode region overhead. It doesn’t account for other overhead, but it is a good starting point.
3. **Vertical sizing** uses the results of the previous step as the starting point in configuring a 3 node cluster, which is used to determine the “building block”—the sizing, configuration and workload for a single node, by experimentation.
4. **Scale-out validation** takes the single node, a building block, from the previous step and puts it to test in scaled out deployments, under load, scaling out at least a couple of times, tuning as needed, and verifying near-linear scalability and performance.
5. **Projection to full scale** is finally done using the results of scale-out validation to arrive at the sizing configuration that will meet the desired capacity and SLA.

The following sections go into the details of each step.

#### Step 1: Domain Object Sizing

Before any other estimates can be made, the size of the domain objects to be stored in the cluster has to be estimated. A good way to size a domain object is by running a single instance Geode test with Geode statistics enabled, in which each domain object to be sized is stored in a dedicated partitioned region. The test just loads a number of instances of each domain object, making sure they all stay in memory (no overflow). After running the test, load the statistics file from it into VSD and examine _dataStoreBytesInUse_ and _dataStoreEntryCount_ partition region stats for each partitioned region. Dividing the value of _dataStoreBytesInUse_ by the value of _dataStoreEntryCount_ will be as good an estimate for the average value size as you can get. Note that this estimate doesn’t include the key size and entry overhead, just the value itself.

Another approach is to use a heap histogram. In this approach it’s best to run a separate test for each domain object, as it makes it easier, based on the number of entries in memory, to figure out what classes are associated with data entries.  

Data sizing can also be done using [Data Sizer Java Utility](https://communities.vmware.com/docs/DOC-20695).

#### Step 2: Estimating Total Memory Requirements Using the Sizing Spreadsheet

Total memory and system requirements can be approximated using the sizing spreadsheet, which calculates in all the Geode region related per-entry overhead, and takes into account the desired memory headroom. The spreadsheet formulas are rough approximations that serve to inform a very high level estimate, as they do not account for any other overhead (buffers, threads, queues, application workload, etc). In addition, the results obtained from the spreadsheet do not have any performance context. For this reason, the next step is to take the results for memory allocation per server obtained from the spreadsheet and use them as the starting point for  the vertical sizing. 

#### Step 3: Vertical Sizing

This part of the sizing process is the most involved and most important. 

Vertical sizing should answer the question of what fraction of the total requirements for storage and workload can be satisfied with a single data node, and with what resources. The answer to this question represents a building block (a unit of scale) and includes both the size of the resources, and the workload capacity. It also includes the complete configuration of the building block (system, VM if present, JVM, and Geode).

For example, a result of this step for a simple read-only application might be that a single data node with a JVM sized to 64G can store 40G of data and support a workload of 5000 get operations per second within the required latency SLA, without exhausting any resources. It is important to capture all the key performance indicators for the application, and make sure they meet the desired SLA. A complete output of the vertical sizing step would include all the relevant details, for example, hardware resources per node, peak capacity and performance at peak capacity, and would note which resource becomes a bottleneck at peak capacity. 

This is a hands-on approach that uses experimentation to determine the optimal values for all the relevant configuration settings, including the system, VM if virtualization is used, JVM, and Geode configuration to be used. 

To run experiments and tests, a cluster of three data nodes plus a locator is needed, as well as additional hosts to run clients that will generate the application workload. Three data nodes are required to fully exercise the partitioning of data in partitioned regions across multiple nodes in presence of data redundancy. The data nodes should be sized based on the estimates obtained from the sizing spreadsheet. That is the starting point. Typically, a heap headroom of 50% of the old generation is used to begin with, CMSInitiatingOccupancyFraction set to 65%, and the young generation is sized to 10% of the total heap. Geode logging and statistics should be enabled for all the test runs. The logs should be routinely checked for problems. The statistics are analyzed for problems, verification of resources, and performance. Of course, performance metrics can be collected by the application test logic as well. Any relevant latency metrics will have to be collected by the test application.

If WAN distribution is needed, it is best to set up an identical twin cluster and configure the WAN distribution between the two clusters. WAN capacity sizing should be done as well. 

Test runs should exercise a representative application workload, and be long enough to incur multiple GC cycles, so that stable resource usage can be confirmed. Also, if any Geode queues are used, tests should be run to determine adequate queue sizes that meet the SLA. If disk storage is used, then adequate disk store size and configuration per node should be determined as well as part of this exercise.

Upon each test run, the latency metrics collected by the application are examined. VSD is used to examine the statistics and correlate the resource usage with latencies and throughput observed. The article [Using Visual Statistics Display to Analyze GemFire Runtime Configuration, Resources, and Performance](http://blogs.vmware.com/vfabric/2012/10/using-visual-statistics-display-to-analyze-gemfire-runtime-configuration-resources-and-performance.html) covers the basics of VSD and the relevant statistics we need. The resources that should be examined are memory (heap, and non-heap, GC), CPU, system load, network, file descriptors, and threads. In addition, the queue statistics should be examined as well.  

One of the objectives of vertical sizing is to determine the headroom required to accomplish the desired performance. This might take several tests, in order to tune the headroom to no more and no less than needed. A much larger headroom than needed could amount to a significant waste of resources. A smaller headroom could cause higher GC activity and CPU usage and hurt performance.

###### Locator Sizing

Locator JVM sizing may be necessary when JMX Manager is running in the locator JVM, and JMX is used for monitoring. The easiest way to do this is to set the locator heap to 0.5G, and watch it during the scale-out. 

###### Notes on GC

When it comes to GC, the most important goal is to avoid full GC’s, as they cause stop the world pauses, which can cause a Geode data node to be unresponsive, and as a result expelled from the cluster. 
The permanent generation space can trigger a full GC as well, which happens when it fills up. It should be sized to avoid this. In addition, the JVM can be instructed to garbage collect the permanent generation space along with CMS GC using the following option: 

        -XX:+CMSClassUnloadingEnabled

GC can be tuned for 2 out of the following 3: 
* latency,
* throughput,
* memory footprint.

With Geode we sacrifice the memory footprint to accomplish latency and throughput goals. This is why heap headroom is so important.

If minor GC pauses are too long, reducing the young generation might help. On the other hand, that will most likely increase the frequency of minor collections. In addition, for very large heaps (e.g. 64G and above), the old generation impact on minor GC pauses may be reduced by using the following GC settings:
      
        -XX:+UnlockDiagnosticVMOptions XX:ParGCCardsPerStrideChunk=32768

#### Step 4: Scale-out Validation

During this step, the initial three node cluster is scaled out at least a couple of times, adding at least a couple of nodes each time. The client hosts should be scaled out accordingly as well, in order to be able to create adequate workload at each step. It is important to remember to increase the workload proportionally to the scale-out. There is no hard rule about how much to increase the cluster size, or in what increments. Often, this is dictated by available hardware resources.

The goal of this step is to validate the building block configuration and capacity at some, larger than initial, scale, so that we can project the capacity to full scale with confidence. Some tuning may be required along the way. For example, with more nodes in the cluster there will be more socket connections and buffers in use on each node, as well as threads, resulting in somewhat higher memory usage per node (both heap and non-heap), as well as increased file descriptors in use. 

If JMX is used for monitoring, watch the heap usage of the locator running the JMX Manager. 

#### Step 5: Projection to Full Scale

Once the scale-out validation is done, and any adjustments have been made, we have everything we need to determine the total cluster size. We know the storage and workload capacity of a single node, and we know that we can scale horizontally to meet the full requirements. In addition, in the process we have tuned the cluster configuration to meet the demands of the application workload.

## Sizing Quick Reference

General recommendations that should be considered as the starting point in capacity planning and sizing:


|  **Data Node Heap Size** | **Use**  |
|:----:|:----:|
| Up to 32GB  | Smaller data volumes (up to a few hundred GB); very low latency required  |
| 64GB+  |  Larger data volumes (500GB+) |
| **CPU Cores per Data Node** |  **Use**   |
| 2 to 4  |  Development; smaller heaps |
| 6 to 8  |  Production; performance/system testing; larger heaps |
| **Network Bandwidth**  | **Use**  |
|  1GbE |  Development |
|  High bandwidth (e.g. 10GbE) |  Production; performance/system testing |
|  **Disk Storage** | **Use** |
| DAS, or SAN | Always |
| NAS | Do not use; performance and resilience issues |

* **Memory/CPU relationship**: mind the NUMA boundary
* **Virtualization**: Do not oversubscribe resources (memory, CPU, storage). Run a single Geode data node JVM per VM.
