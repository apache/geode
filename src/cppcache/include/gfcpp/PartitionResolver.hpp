#ifndef GEMFIRE_PARTITION_RESOLVER
#define GEMFIRE_PARTITION_RESOLVER
/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "SharedPtr.hpp"
#include "Cacheable.hpp"
#include <string>

namespace gemfire {

class EntryEvent;

/**
* Implementers of interface <code>PartitionResolver</code> enable custom
* partitioning on the <code>PartitionedRegion</code>.
* 1. The Key class or callback arg can implement PartitionResolver interface to
* enable custom partitioning OR
* 2. Configure your own PartitionResolver class in partition attributes (For
* instance when the Key is a primitive type or String) Implement the
* appropriate equals - For all implementations, you need to be sure to code the
* class equals method so it properly verifies equality for the
* PartitionResolver implementation. This might mean verifying that class names
* are the same or that the returned routing objects are the same etc.. When you
* initiate the partitioned region on multiple nodes, GemFire uses the equals
* method to ensure you are using the same PartitionResolver implementation for
* all of the nodes for the region.
* GemFire uses the routing object's hashCode to determine where the data is
* being managed. Say, for example, you want to colocate all Trades by month and
* year.The key is implemented by TradeKey class which also implements the
* PartitionResolver interface.
* public class TradeKey implements PartitionResolver {<br>
* &nbsp &nbsp private String tradeID;<br>
* &nbsp &nbsp private Month month ;<br>
* &nbsp &nbsp private Year year ;<br>
*
* &nbsp &nbsp public TradingKey(){ } <br>
* &nbsp &nbsp public TradingKey(Month month, Year year){<br>
* &nbsp &nbsp &nbsp &nbsp this.month = month;<br>
* &nbsp &nbsp &nbsp &nbsp this.year = year;<br>
* &nbsp &nbsp } <br>
* &nbsp &nbsp public Serializable getRoutingObject(EntryOperation
* opDetails){<br>
* &nbsp &nbsp &nbsp &nbsp return this.month + this.year;<br>
* &nbsp &nbsp }<br> }<br>
*
* In the example above, all trade entries with the same month and year are
* guaranteed to be colocated.
*/
class CPPCACHE_EXPORT PartitionResolver : public SharedBase {
  /**
  * @brief public methods
  */

 public:
  /**
  * @brief destructor
  */
  virtual ~PartitionResolver();

  /**
  * Returns the name of the PartitionResolver
  * @return String name
  */
  virtual const char* getName();

  /**
  * @param opDetails the detail of the entry event
  * @throws RuntimeException - any exception thrown will terminate the operation
  * and the exception will be passed to the
  * calling thread.
  * @return object associated with entry event which allows the Partitioned
  * Region to store associated data together
  */
  virtual CacheableKeyPtr getRoutingObject(const EntryEvent& opDetails) = 0;

 protected:
  /**
  * @brief constructors
  */
  PartitionResolver();

 private:
  // never implemented.
  PartitionResolver(const PartitionResolver& other);
  void operator=(const PartitionResolver& other);
};
}
#endif
