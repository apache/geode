/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * A probe which calculates the load of a PR in a given member. This class
 * is designed to be created in the member that is doing a rebalance operation
 * and sent to all of the data stores to gather their load. In the future, this
 * class or something like it may be exposed to customers to allow them to 
 * provide different methods for determining load.
 * @author dsmith
 *
 */
public interface LoadProbe extends DataSerializable {
  PRLoad getLoad(PartitionedRegion pr);
}
