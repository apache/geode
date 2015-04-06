The Geode team is actively working on several ground breaking features that seek to extend the product's usefulness in transactional environments while adding new features that make it very relevant to big data applications. Here are some of the key features being built into the product at this time.

**HDFS Integration:**
Geode as a transactional layer that microbatches data out to Hadoop. This capability makes Geode a NoSQL store that can sit on top of Hadoop and parallelize the process of moving data from the in memory tier into Hadoop, making it very useful for capturing and processing fast data while making it available for Hadoop jobs relatively quickly. The key requirements being met here are
* Ingest data into HDFS parallely
* Cache bloom filters and allow fast lookups of individual elements
* Have programmable policies for deciding what stays in memory
* Roll files in HDFS
* Index  data that is in memory
* Have expiration policies that allows the transactional set to decay out older data
* Solution needs to support replicated and partitioned regions

**Spark Integration:**
Geode as a data store for Spark applications is what is being enabled here. By providing a bridge style connector for Spark applications, Geode can become the data store for storing intermediate and final state for Spark applications and allow reference data stored in the in memory tier to be accessed very efficiently for applications
* Expose Geode regions as Spark RDDs
* Write Spark RDDs to Geode Regions
* Execute arbitrary OQL queries in your spark applications

**Off-Heap data management:**
Increasing the memory density for the in memory tier has been an important goal for customers. Moving data out of the ambit of the JVM garbage collector allows for higher throughput because GC threads are no longer actively copying data from one memory space to the next. It also reduces the need to restrict JVM sizes in order to ensure that memory allocation and garbage generation never outruns the garbage collector. This in turn reduces complexity by reducing cluster sizes and moving parts in a running cluster
* Storing values, indexes and keys off heap
* Optimizing Geode so that interacting with data minimizes the amount of data deserialized and the number of times data is deserialized

**Lucene Integration:**
Allow Lucene indexes to be stored in Geode regions allowing users to do text searches on data stored in Geode. One way of leveraging this work would be to use the Gem/Z connector to push data into a Geode cluster and allow all kinds of text analysis to be done on this data. 

**General product improvements:**

* Making authentication and authorization for all channels (gfsh, admin, client-server and REST) to follow the highly effective client server model that we support today.
* Extending the transactions mechanism in Geode to support distributed transactions with eager locking and extend the colocated transaction model already supported in the product.
* WAN improvements that deliver significantly higher throughput.
