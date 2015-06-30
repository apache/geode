/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;

/**
 * Tests region reliability defined by MembershipAttributes using 
 * DISTRIBUTED_NO_ACK scope.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class RegionReliabilityDistNoAckDUnitTest extends RegionReliabilityTestCase {

  public RegionReliabilityDistNoAckDUnitTest(String name) {
    super(name);
  }
  
  protected Scope getRegionScope() {
    return Scope.DISTRIBUTED_NO_ACK;
  }
  
}

