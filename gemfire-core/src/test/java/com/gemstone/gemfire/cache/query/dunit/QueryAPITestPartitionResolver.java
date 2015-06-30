/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 *
 */
package com.gemstone.gemfire.cache.query.dunit;

import java.io.Serializable;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;

/**
 * This resolver is used in QueryUsingFunctionContextDUnitTest.
 * @author shobhit
 *
 */
public class QueryAPITestPartitionResolver implements PartitionResolver {

  @Override
  public void close() {
  }

  @Override
  public Serializable getRoutingObject(EntryOperation opDetails) {
    return (((Integer)opDetails.getKey()).intValue()%QueryUsingFunctionContextDUnitTest.numOfBuckets);
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }
}
