#Function Best Practices in Geode

##Overview

One of the primary goals in a grid environment is to achieve linear scalability - add additional compute capacity to offer predictable latency when the concurrent load increases, to increase the overall throughput or to decrease the latency. When dealing with stateful applications it is often the access to shared state that becomes the bottleneck.

There are three primary features in Geode that enable linearly scalable architectures, and they are:

1. Data Partitioning: uniformly stripe the data in memory across the grid
2. Data Affinity: Keep related data that is frequently accessed or updated together colocated.
3. Data Aware Routing: Move the data dependent behavior to the node where the data resides rather than the other way around 

Geode supports Data Partitioning across peer server nodes in a Distributed System.  Partitioning spreads the data for a single data set (a.k.a. region) across a set of peer server nodes.  Each data element (by key) is owned by only one primary partition at any point in time.   Individual partitions are optionally replicated on one or more backup nodes to provide fault tolerance in case the primary node for the given partition fails.  Partitioning is a key feature to scale large applications, particularly for OLTP applications that grow in terms of data volume - the number of data items managed grows rather than individual items grow over time. For details on the Data Partitioning options and configuration, look [here]***
 
Geode supports Data Affinity (a.k.a. Data Colocation) which allows data in distinct regions to be partitioned in such a way as that related information from different regions is all hosted by the same node.  

We draw inspiration for the data model from <a href="http://www.google.com/url?q=http%3A%2F%2Fwww.ics.uci.edu%2F~cs223%2Fpapers%2Fcidr07p15.pdf&sa=D&sntz=1&usg=AFQjCNFGUcY5KLOPSA0ncgeLIH4JNxekGQ">Pat Helland's 'Life beyond distributed transactions'</a> by adopting entity groups as a first class artifact designers start with, and define relationships between entities within the group (associations based on reference as well as containment). Rationalizing the design around entity groups will force the designer to think about data access patterns and how the data will be colocated in partitions.

For example, consider a Customer and their Orders.  If partitioned independently, an individual customer's orders could be distributed randomly across all of the available nodes that host Order data.  But with Data Affinity, you can set up your region by stating something like 'create region Orders (...) colocate with Customer' and keep all contact information and all orders for a single customer in a single member. If for billing purposes you regularly query customer contact information and order information together to form billing statements, Then, anytime you perform an operation on those data types a single customer uses the cache of only a single member.

Geode support Data Aware Routing through the Geode Function Service. In the case of Data Aware Functions, the behavior can be directly routed to the node(s) that hosts the data needed to execute the function in parallel, stream and aggregate the results substantially reducing the time taken to execute complex data intensive tasks. The distribution and parallelism of the activity is abstracted away from the caller. 

Database vendors introduced stored procedures so that applications could off-load data intensive application logic to the database node where the behavior could be collocated with the data. Geode extends this paradigm by supporting invocation of application defined functions on highly partitioned data nodes such that the behavior execution itself can be parallelized. Behavior is routed to and executed on the node(s) that host(s) the data required by the behavior. Application functions can be executed on just one node, executed in parallel on a subset of nodes or in parallel across all the nodes. This programming model is similar to the now Map-Reduce model popularized by Google. 

With the three combined features of Data Partitioning, Data Affinity and Data Aware Routing, Geode can receive a request on any node and route that request to a specific peer server that hosts the data required to execute that function.  With Data Affinity (aka data colocation), all of the data needed to execute that function will be hosted by that same node, even when the data is spread over multiple data regions.  Because all of the data is hosted locally, there is no need to coordinate a transaction with any other node.  If pessimistic locking is required, it is reduced to a thread level semaphore in the local JVM which is much faster than coordinating with a transaction service.  With distributed transactions eliminated and processing spread over a series of nodes (that can be increased dynamically) the architecture is well on the way to linear scalability.

###Data Affinity in a single partitioned region (custom partitioning using PartitionResolver)

When storing entries in partitioned data regions, Geode uses a 'hashing' policy by default  - the entry key is hashed to compute an integer which should be evenly distributed across a large range of values.  The integer is used to identify a 'bucket' to which the entry will be assigned.  The bucket is in turn mapped to a physical cache member node where the primary copy of the data will be managed. Each data node hosting a partitioned region will normally manage multiple buckets.  When capacity is increased (more members are added) it is the buckets that are distributed across the members.  Through the Geode APIs and functions, applications are totally abstracted away from the physical location of the entry and (for optimal scalability) should not attempt to control where the data will be managed.

Custom partitioning or application-controlled partitioning is an explicit mechanism that permits applications to control collocation of related data entries. Applications can colocate multiple entries within a single partitioned region to be colocated or even configure colocation policy across multiple data regions. 

For instance, a financial risk analytics application may want to manage all trades, risk sensitivities and reference data associated with a single instrument so that they will always be colocated. Or, for instance, a Order management system may want to colocate all orders, line items, shipments associated with any given customer to all reside in the same process space. 

Applications derive the following benefits through collocation:    
1. Application can route a complex query to the node with the data set required for the query and localize the entire query processing.This will dramatically increase the speed of execution compared to a totally distributed query. 
2. Application that iterate over related data sets for aggregation can avoid unnecessary network hops.Compute heavy applications that are also data intensive can significantly increase the overall throughput of the application.

Applications implement the PartitionResolver interface to enable custom partitioning on Partitioned Region. 

The class of object used for the entry's key or the callback arg (an optional arg in the Region.put() call) can implement the PartitionResolver; interface to enable custom partitioning

                                                              	OR

Configure your own PartitionResolver class in partition attributes  (either via the APIs or in the cache configuration file).  This is useful when the entry key is a primitive type or String, or if you don't want to implement Geode interfaces in your data classes.

If you want to colocate all Trades by symbol, here is an example of how to do it in Geode. 

###Data Affinity across multiple partitioned regions

To collocate entries across multiple data regions, the application has to do two things:

1. The partitioned region has to be configured to be colocated with another partitioned region (programmatically or in the cache configuration file) as discussed below.

2. Entries across data regions have to return the same routing object (by implementing the PartitionResolver) for entries that have to be colocated. 
  
Take for instance, the classic Order management system, Customer -> (1-M) Orders -> (1-M) Shipments. And, say the application transacts or accesses data by joining related information together, but, only deals with one specific customer at a time. The idea here is all customers are partitioned, but, all associated Orders and, by corollary, related shipments are always collocated. With colocation established, Geode will prune queries so that they run only on the correct partition. Similarly, transaction coordination will be reduced to an efficient, local lock.

[Here](Colocating-related-entries-across-multiple-partitioned-regions"> is an example which demonstrates how you can achieve colocation for the above scenario. 

###Example use cases in finance where colocation is useful

####Bi-temporal data management for Financial Risk analytics

In many applications, particularly in financial applications, data has a temporal nature - it is valid 'at' and possibly 'for' a specific period of time. Bi-temporal modeling includes valid time ranges for every entry in the cache. This causes significant growth in the quantity of data. Every update or delete operation is recorded as a new entry in the cache. Any data change operation becomes a new cache entry and uses a timestamp along with the business (i.e entry) key to uniquely identify the object. Applications typically want to access the value of a financial instrument or product at a particular point in time ('asOf' some timestamp). Such a time based request requires Geode to execute a query (relational operators). The 'best practices' approach is to colocate all temporal data corresponding to any given business key (i.e. for a specific financial instrument).  The temporal query can then be focused on a target subset of entries that can potentially satisfy the query.  Growth of the system, in terms of handling additional financial instruments, is best achieved by establishing additional partitions in order to spread the data and processing across more machines.

####Pricing engine for Financial Derivative product pricing

A given financial security may have hundreds or thousands of 'derivative' produts.  Any time the security price changes, the new price of each derivative product is recalculated. These calculation can be complex and computationally expensive and hence it makes a lot of sense to distribute all the securities and the workload across many nodes. Securities and their derivatives need to be colocated.  Each calculation may also depend on other reference data which typically will be colocated and replicated on each node.

######Colocating related entries in a single partitioned region

Say, for example, you want to colocate all Trades by Symbol.The key is implemented by TradeKey class which also implements the PartitionResolver interface

<pre><code>
public class TradeKey implements PartitionResolver {
private String tradeID;
private String symbol;

public TradingKey(String id, Trade someTradeObj){
  tradeID = id;
  // Get the symbol from the Trade instance
  symbol = someTradeObj.getSymbol();
}

public Serializable getRoutingObject(EntryOperation opDetails){
  return this.symbol;
}
</code></pre>

Essentially, when data colocation is required, all entry keys returning the same 'routing object' (symbol in this case) are guaranteed to be collocated on the same partition. Geode hashes the returned symbol to a bucket which is mapped to a partition node.

Applications can also introduce a partition resolver for a partitioned data region non-intrusively by specifying the PartitionResolver class to invoke when data is published.


```
<cache>
  <region name="myPrDataRegion">
    <region-attributes>
      <partition-attributes>
         <partition-resolver>
              <class-name>com.gemstone.gemfire.cache.MyPartitionResolver</class-name>
          </partition-resolver>
       <partition-attributes>
     </region-attributes>
  </region>
<cache>
```
<pre><code>
//Create a new PartitionResolver 
PartitionResolver resolver = new MyPartitionResolver();

//Set the PartitionResolver to partition attributes
PartitionAttributes attrs = new PartitionAttributesFactory().setPartitionResolver(resolver).create();
//Create a partition data region
Region region = new RegionFactory().setPartitionAttributes(attrs).create("myPrDataRegion");
</code></pre>

######Colocating related entries across multiple partitioned regions 

So, for instance, in a Orders partitioned region, all order entries that return the same CustomerID will be guaranteed to reside on the same node.

```
<cache>
  <region name="Customers">
    <region-attributes>
      <partition-attributes>
         <partition-resolver>
              <class-name>com.gemstone.gemfire.cache.CustomerPartitionResolver</class-name>
          </partition-resolver>
       <partition-attributes>
     </region-attributes>
  </region>
  <region name="Orders">
    <region-attributes>
      <partition-attributes colocated-with="Customers"> // COLOCATION ATTRIBUTE
         <partition-resolver> // Name
              <class-name>com.gemstone.gemfire.cache.CustomerPartitionResolver</class-name>
          </partition-resolver>
       <partition-attributes>
     </region-attributes>
  </region>
  <region name="Shipments">
    <region-attributes>
      <partition-attributes colocated-with="Customers"> // COLOCATION ATTRIBUTE
         <partition-resolver> // Name
              <class-name>com.gemstone.gemfire.cache.CustomerPartitionResolver</class-name>
          </partition-resolver>
       <partition-attributes>
     </region-attributes>
  </region>
</cache>

```
<pre><code>
//Create a new PartitionResolver based on CustomerID, so that all orders from same CustomerId will yield to a single bucket
PartitionResolver resolver = new CustomerPartitionResolver();


//Set Partition resolver to partition attributes
PartitionAttributes attrs = new PartitionAttributesFactory().setPartitionResolver(resolver).create();
//Create a Customers Partition Region
Region customers = new RegionFactory().setPartitionAttributes(attrs).create("Customers");
// Entry ops allowed before creation of associated partitioned regions.
 
 
// Create PartitionAttributes for Orders which should colocated with Customers
attrs = new PartitionAttributesFactory().setPartitionResolver(resolver).setColocatedWith(customers.getFullPath()).create();
//Create a Orders partition region
Region orders = new RegionFactory().setPartitionAttributes(attrs ).create("Orders");
// Even orders partioned region is now ready for operations

// Create PartitionAttributes for Shipments which should colocated with Customers
attrs = new PartitionAttributesFactory().setPartitionResolver(resolver).setColocatedWith(customers.getFullPath()).create(); 
//Create a Shipments partition region
Region shipments = new RegionFactory().setPartitionAttributes(attrs ).create("Shipments");
</code></pre>

Following rules apply while defining colocation :

+The region name passed in setCollocatedWith() method should be previously created, otherwise IllegalStateException is thrown.
+Collocated entities should have custom partitioning enabled, otherwise IllegalStateException is thrown.
+Collocated Partitioned regions should have same PartitionResolver (must return the same routing object)
+Collocated Partitioned Regions should have same partition attributes (such as, totalNoOfBuckets, redundantCopies)

######Data aware behavior routing using Geode Function Service

Geode's function execution service enables both cache clients and peer nodes to execute arbitrary, application functions on the data fabric.  Then the data is partitioned across a number of members for scalability, Geode can route the function transparently to the node that carries the data subset required by the function and avoid moving the taget data around on the network.  This is called 'data aware' function routing.  Applications employing data aware routing do not need to have any knowledge of where the data is managed. 

Application functions can be executed on a single node, executed in parallel on a subset of nodes or executed in parallel across all the nodes. This programming model is similar to the now popularized Map-Reduce model from Google. Data-aware function routing is most appropriate for applications that require iteration over multiple data items (such a query or custom aggregation function).  By colocating the relevant data and parallelizing the calculation, the overall throughput of the system can be dramatically increased. More importantly, the calculation latency is inversely proportional to the number of nodes on which it can be parallelized.

Execution of a function on a single server node is similar to how applications execute stored procedures on database servers. This feature can be useful for the following cases:

1. Application wants to execute a server side transaction or carry out data updates using the Geode distributed lock service.
2. Application wants to initialize some of its components once on each server which might be used later by executed functions
3. Initialization and startup of a 3rd party service such a messaging service
4. Any arbitrary aggregation operation that requires iteration over local data sets done more efficiently through a single call to the cache server

######Registering Functions to FunctionService
Applications can declare and register the functions using declarative means (cache.xml) or through the Geode API.  All registered functions have an identifier. Identifying functions allows the administrator to monitor function activity and cancel them on demand. 

```
<cache>
 ...
<function-service>
  <function>
    <class-name> com.bigFatCompany.tradeService.cache.func.TradeCalc1</class-name> <!--implementsFunction and Declarable interfaces --> 
  </function>
  <function>
    <class-name> com.bigFatCompany.tradeService.cache.func.TradeCalc2</class-name> <!--implementsFunction and Declarable interfaces --> 
  </function>  
</function-service>
 ...
</cache>

```

<pre><code>
Registering functions in programmatic way :
Function function1 = new TradeCalc1();//TradeCalc1 implements Function interface
Function function2 =new TradeCalc2();//TradeCalc2 implements Function interface
FunctionService.registerFunction(function1);
FunctionService.registerFunction(function2);
</code></pre>

+Functions that need to be executed across remote members should be registered in each member before invoking.
+Applications may create inline functions which need not be registered.
+Id (returned from Function.getFunctionId()) can be any arbitrary string.
+Modifying function instance after registration has no effect on function execution.

#####Example 1 : Data aware routing and colocated transactions   
Suppose Customers, Orders and Shipments are colocated as described in last example , here. And a user wants following behavior:

<i>A Customer places an order, application needs to approve the order before scheduling shipment for that order. If the order is not approved, shipment should not be scheduled.</i>

Using FunctionService, this can be achieved as demonstrated [here]  

#####Example 2 : Data independent parallel execution on all data nodes.

Suppose a user wants to do an aggregation operation across the partitioned region on all nodes. Specifically, user is interested in avg sales from orders region.

Using FunctionService, this can be achieved as demonstrated [here]. 

#####Example 3 : fire-n-forget function execution. 

Suppose a user wants to execute a function which doesn't return any result  

#####What is available from Geode to application function ? 

An instance of  FunctionContext is made available to the function when and where it executes.  It is required by Function#execute(FunctionContext) to execute a Function on a particular member. An user can retrieve following information from FunctionContext

<table>
<tr>
 <td>API</td>
 <td>Description</td>
</tr>
<tr>
<td>getArguments()</td>
<td>These are the arguments specified by the caller using Execution#withArgs(Serializable)</td>
</tr>
<tr>
<td>getFunctionId()</td>
<td>Returns the identifier of the function.</td>
</tr>
<tr>
<td>getResultSender()</td>
<td>Returns the ResultSender which is used to add the ability for an execute method to send a single result back, 
or break its result into multiple pieces and send each piece back to the calling thread's ResultCollector.</td>
</tr>
</table>

A context can be data dependent or data independent. For data dependent functions, refer to RegionFunctionContext. Function code can retrieve the following information from RegionFunctionContext (which extends FunctionContext).

<table>
<tr>
<td>API</td>
<td>Description</td>
</tr>
<tr>
<td>getFilter()</td>
<td>Returns subset of keys (filter) provided by the invoking thread (aka routing objects). 
The set of filter keys are locally present in the datastore on the executing cluster member.</td>
</tr>
<tr>
<td>getDataSet()</td>

<td>Returns the reference to the Region on which the function is executed</td>
</tr>
</table>

 In adition to the above, if user has executed a function on colocated partitioned regions, the following can be retrieved using utility class PartitionRegionHelper.

<table>
<tr>
<td>API</td>
<td>Description</td>
</tr>
<tr>
<td>getLocalData(Region r)</td>
<td>Given a partitioned Region return a Region providing read access limited to the local heap, 
writes using this Region have no constraints and behave the same as a partitioned Region.</td>
</tr>
<tr>
<td>getColocatedRegions (Region r)</td>
<td>Given a partitioned Region, return a map of colocated Regions.</td>
</tr>
<tr>
<td>getLocalColocatedRegions(RegionFunctionContext context)</td>
<td>Given a RegionFunctionContext  for a partitioned Region return a map of colocated Regions with read access limited to the context of the function.</td>
</tr>
</table>

#####How does function execution work ?

When an user invokes a function, depending on filter passed, target nodes for this function execution are identified. If possible the nodes are pruned to minimum set of nodes where all the data is present. function execution message is sent asynchronously to all the target nodes, using a configurable thread pool. Each target node then sends the function execution results to the caller.The caller waits for the result using ResultCollector.getResult().

Default implementation of ResultCollector called DefaultResultCollector waits for each node to respond with a result and returns the unordered set of results to the caller. These results from the target nodes are added using ResultCollector#add(Serializable oneResult)API. Using this API an user can customize aggregation of results.

#####What if my function execution result is large ?

An user can optionally use the ResultSender to chunk the results, and send back to the caller. The ResultSender class provides methods to send results back to the ResultCollector. Instead of getting the result of a function execution when the execution is complete, ResultSender provides mechanism to send individual results back back to the caller prior to completion.  It may also break a result into multiple chunks and send each result back to the caller. To signal the calling thread to stop waiting for the result,  the function should use the lastResult using the ResultSender.

#####How does ResultSender play with the ResultCollector?

Each time a function sends a result using ResultSender it gets added to the ResultCollector at the caller node. So, the partial sent results are available to the application program instantaneously. This facilitates the developer to work on partial results and can decide on application logic without waiting for all the results.

#####What happens when one of the function execution target nodes goes down ?

FunctionException is thrown with cause as FunctionInvocationTargetException,this usually indicates that the node that was executing the function failed mid-process. Applications can catch the FunctionInvocationTargetException and choose to re-execute the function. It is the function implementation's responsibility to provide any desired idempotent behavior.

For instance, any generated state as the function is being executed should be stored in Geode with redundancy. So, when the function fails, the client can re-execute and with a flag that indicates that the function execution is a possible duplicate. The function implementation could check this flag, use the partial state stored in Geode to complete the remainder of the function.

#####Some useful FunctionService statistics

Geode captures several statistics on each member to allow monitoring application behavior on data nodes.

**functionExecutionsCompleted** :Total number of completed function.execute() calls

**functionExecutionsCompletedProcessingTime** :Total time consumed for all completed invocations

**functionExecutionsRunning** :A guage indicating the number of currently running invocations

**resultsSentToResultCollector** :Total number of results sent to the ResultCollector

**functionExecutionCalls** :Total number of FunctionService...execute() calls

**functionExecutionsHasResultCompletedProcessingTime** :Total time consumed for all completed execute() calls where hasResult() returns true

**functionExecutionsHasResultRunning**:A gauge indicating the number of currently active execute() calls for functions where hasResult() returns true

**functionExecutionsExceptions** :Total number of Exceptions Occured while executing function
resultsReceived :Total number of results sent to the ResultCollector
