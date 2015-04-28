/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.server.internal;

import org.junit.experimental.categories.Category;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.junit.UnitTest;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class ConnectionCountProbeJUnitTest extends TestCase {
  
  public void test() {
    ConnectionCountProbe probe = new ConnectionCountProbe();
    ServerMetricsImpl metrics = new ServerMetricsImpl(800);
    ServerLoad load = probe.getLoad(metrics);
    Assert.assertEquals(0f, load.getConnectionLoad(), .0001f);
    Assert.assertEquals(0f, load.getSubscriptionConnectionLoad(), .0001f);
    Assert.assertEquals(1/800f, load.getLoadPerConnection(), .0001f);
    Assert.assertEquals(1f, load.getLoadPerSubscriptionConnection(), .0001f);

    for(int i = 0; i < 100; i++) {
      metrics.incConnectionCount();
    }
    
    load = probe.getLoad(metrics);
    Assert.assertEquals(0.125, load.getConnectionLoad(), .0001f);
  }
    
}
