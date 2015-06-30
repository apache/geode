/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.cache.*;

/**
 * subclass of UpdatePropagationDUnitTest to exercise partitioned regions
 *
 * @author Bruce Schuchardt
 */
public class RegisterInterestKeysPRDUnitTest extends RegisterInterestKeysDUnitTest {

  public RegisterInterestKeysPRDUnitTest(String name) {
    super(name);
  }
  public static void createImpl() {
    impl = new RegisterInterestKeysPRDUnitTest("temp");
  }
  
  protected RegionAttributes createServerCacheAttributes()
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
    return factory.create();
  }
}
