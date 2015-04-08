Statistics are recorded by background threads for system monitoring and failure analysis.  Many statistics are gathered by Geode and the impact on performance for most statistics collection is minimal (stay away from clock probes as they are very expensive on some platforms!).

So, say you need to add a few statistics covering some runtime aspect of Geode. Perhaps you added some new functionality or modified Geode's existing behavior. Either way, you now want to expose Geode's behavioral characteristics as it relates to these changes in a tool such as VSD.

The process of adding stats to Geode is pretty straight forward. However, a general understanding of Geode's different stat types will make this process a bit clearer.

# Background

Geode uses 2 different types of stats to track the behavioral characteristics of Geode. The first type is a counter. As one would expect, this value increments over the life-cycle of a Geode distributed system and cache. It is expected that this stat value will only every increase over time (it should not be decremented). However, there are cases in Geode where this general principle has not been adhered to.


An example of a counter would be the number of puts/gets on a Region in the Cache. Clearly, the number of puts and get operations on a Region only increases the longer the Geode distributed system and cache are in existence. You can kind of think of this as uptime, however, there is a stat to track this information as well.


The other type of stat is a gauge. If a developer can count things, it makes only sense, that a developer may want to keep track of fluctuations in values that can go up or down. An example of such a stat might be keeping track of the current, open number of client connections at any given moment in time.


Geode performs stat sampling on a fixed interval basis that is controlled in the Geode configuration. The information of the statistic values are written as entries to *.vsd files in the file system that can be read by the Geode VSD tool.


One of the most important things to know about Geode stats, is that when you want to record a stat value, the operation should be a seamless, non-compute intensive operation, such as assigning the current value of an atomic counter, thereby avoiding the unnecessary performance impact of recording a stat. We do not want the act of recording a stat to affect the performance of a Geode operation due to a intensive calculation. A good example of this is time-based stats, which are disabled by default in Geode. An operation like System.currentTimeMillis() can add significant overhead to an cache operation depending on the JVM.


# Creating a Stat...

Stats in Geode are grouped according to functional area in their own class. For instance, the Geode `DistributionStats` in the `com.gemstone.gemfire.distributed.internal` package track statistical information concerning Geode's distribution layer. These stats are all related to p2p message distribution across the Geode distributed system along with sender and receiver connections, and so on.


The first thing to do is add accessors/mutators to the appropriate Geode statistics class (such as `DistributionStats`) and it's corresponding interface (for `DistributionStats`, the interface is `DMStats`.


For instance, let's suppose you want to add a statistic for Geode's p2p Connection factory and pooling mechanism to keep track of the number of "pool sweeps" in order to clean up any unused and/or stale Connection resources. I might declare a method like so on the `DMStats` interface...

    public void incConnectionFactoryPoolSweeps();


Then, I would modify the `DistributionStats` class like so..

    
    private static final int connectionFactoryPoolSweepsId;

    ...
      
    static {
      
      ...
      
      private StatisticsTypeFactory typeFactory = StatisticsTypeFactoryImpl.singleton();
      
      private StatisticsType type = typeFactory.createType("DistributionStats",
        "Statistics on the geode distribution layer.",
      
      new StatisticDescriptor[] {
        ...,
        typeFactory.createIntCounter("cxpConnectionFactoryPoolSweeps", "The number of times "+
          "the poolable Connection Factory Sweeper Thread has run.", "sweeps"),
        ...
      });
      
      ...
      
      connectionFactoryPoolSweepsId = type.nameToId("cxpConnectionFactoryPoolSweeps");
    }
    ...
    
    private final Statistics stats;
    
    public DistributionStats(StatisticsFactory statsFactory, long statId) {
      this.stats = statsFactory.createAtomicStatistics(type, "distributionStats", statId);
    }
    
    public void incConnectionFactoryPoolSweeps() {
      stats.incInt(connectionFactoryPoolSweepsId, 1);
    }
    

The Geode `DistributedSystem` class is an instance of the `StatisticsFactory` interface and is the 
reference value passed to the constructor of the `DistributionStats` class when it is created. This `StatisticsFactory` implementation is used to obtain the `Statistics` references to the aspect of Geode you want to track. The `Statistics` interface is a facade to managing and accessing the statistics information and values.


To create a new "type of stat", I would use the `StatisticsTypeFactory` to create the type of stat I wanted, like a counter (`createIntCounter`) or a gauge (`createIntGauge`), name it appropriately (`"cxConnectionFactoryPoolSweeps"`) along with given it a description and a value for it's units ("sweeps"), then obtain a it's "id" for later reference and access in the mutator method (`incConnectionFactoryPoolSweeps`).


Note, the name of my stat in this example was `"cxConnectionFactoryPoolSweeps"`, and it is the name of the stat as is recorded in the *.vsd file and shown in VSD. It is sometimes helpful to group a subset of stats in a given functional area of Geode by name related to some aspect of the code, like connection pooling for instance. All "distribution stats" related to "connection pooling" are prefixed with the name "cxp".


A gauge is created similarly, however, the accessor method might look a bit different. For instance, suppose I wanted to keep track of the number of pooled connections at any given moment in the P2P Connection factory and pooling mechanism, then I might do this...

    
    ...
    
    typeFactory.createIntGauge("cxpPooledConnectionResourceCount", 
      "A count at any given moment of the number of pooled Connection resources "+
      "maintained by the Connection Factory.", "pooled-connections"),
    
    ...
    
    pooledConnectionResourceCountId = type.nameToId("cxpPooledConnectionResourceCount");
    
    ...
    
    public void setPooledConnectionResourceCount(int pooledConnectionResourceCount) {
      stats.setInt(pooledConnectionResourceCountId, pooledConnectionResourceCount);
    }


The code required to make use of these new statistic values is pretty straight forward as well. Provided a reference to a our stats class, like `DistributionStats`, I can make method calls in the normal manner...

    
    getDistributionStats().setPooledConnectionResourceCount(pooledConnectionResourceCount);
    


For more information about Stats, please see the Geode User's Guide.



