/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests the proper intialization of redundancyLevel property.
 * 
 */
@Category(IntegrationTest.class)
public class RedundancyLevelJUnitTest
{
  private static final String expectedRedundantErrorMsg = "Could not find any server to create redundant client queue on.";
  private static final String expectedPrimaryErrorMsg = "Could not find any server to create primary client queue on.";
  
  final String expected = "Could not initialize a primary queue on startup. No queue servers available";
  
  /** The distributed system */
  DistributedSystem system;

  /** The distributed system */
  Cache cache;

  /**
   * Close the cache and proxy instances for a test and disconnect from the
   * distributed system.
   */
  @After
  public void tearDown() throws Exception
  {
    if (cache != null) {
      cache.close();
    }
    if (system != null) {
      
      final String removeExpectedPEM =
          "<ExpectedException action=remove>" + expectedPrimaryErrorMsg + "</ExpectedException>";
      final String removeExpectedREM =
          "<ExpectedException action=remove>" + expectedRedundantErrorMsg + "</ExpectedException>";
      
      system.getLogWriter().info(removeExpectedPEM);
      system.getLogWriter().info(removeExpectedREM);
      
      system.disconnect();
    }
  }

  /**
   * Tests that value for redundancyLevel of the failover set is correctly
   * picked via cache-xml file.(Please note that the purpose of this test is to
   * just verify that the value is initialized correctly from cache-xml and so
   * only client is started and the connection-exceptions due to no live
   * servers, which appear as warnings, are ignored.)
   * 
   * @author Dinesh Patel
   * 
   */
  @Test
  public void testRedundancyLevelSetThroughXML()
  {
      String path = TestUtil.getResourcePath(getClass(), "RedundancyLevelJUnitTest.xml");

      Properties p = new Properties();
      p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      p.setProperty(DistributionConfig.LOCATORS_NAME, "");
      p.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, path);
      final String addExpected =
        "<ExpectedException action=add>" + expected + "</ExpectedException>";
      
      system = DistributedSystem.connect(p);
      system.getLogWriter().info(addExpected);
      
      final String addExpectedPEM =
          "<ExpectedException action=add>" + expectedPrimaryErrorMsg + "</ExpectedException>";
      final String addExpectedREM =
          "<ExpectedException action=add>" + expectedRedundantErrorMsg + "</ExpectedException>";
      system.getLogWriter().info(addExpectedPEM);
      system.getLogWriter().info(addExpectedREM);
     
      try {
        
        cache = CacheFactory.create(system);
        assertNotNull("cache was null", cache);
        Region region = cache.getRegion("/root/exampleRegion");
        assertNotNull(region);
        Pool pool = PoolManager.find("clientPool");
        assertEquals(
            "Redundancy level not matching the one specified in cache-xml", 6,
            pool.getSubscriptionRedundancy());
      } finally {
        final String removeExpected =
          "<ExpectedException action=remove>" + expected + "</ExpectedException>";
        system.getLogWriter().info(removeExpected);
      }
  }

}
