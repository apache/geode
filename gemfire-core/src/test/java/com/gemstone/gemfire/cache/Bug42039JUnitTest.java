/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Keep calling DistributedSystem.connect over and over again
 * with a locator configured. Since the locator is not running
 * expect the connect to fail.
 * See if threads leak because of the repeated calls
 * @author Darrel Schneider
 * @since 5.0
 */
@Category(IntegrationTest.class)
public class Bug42039JUnitTest {
  
  /**
   * Keep calling DistributedSystem.connect over and over again
   * with a locator configured. Since the locator is not running
   * expect the connect to fail.
   * See if threads leak because of the repeated calls
   */
  @Test
  public void testBug42039() throws Exception {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0");
    p.setProperty("locators", "localhost[6666]");
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    for (int i=0; i < 2; i++) {
      try {
        DistributedSystem.connect(p);
        fail("expected connect to fail");
      } catch (Exception expected) {
      }
    }
    int initialThreadCount = threadBean.getThreadCount();
    for (int i=0; i < 5; i++) {
      try {
        DistributedSystem.connect(p);
        fail("expected connect to fail");
      } catch (Exception expected) {
      }
    }
    Thread.sleep(1000); // give chance for thread to exit
    int endThreadCount = threadBean.getThreadCount();
    if (endThreadCount > initialThreadCount) {
      assertEquals(initialThreadCount, endThreadCount);
    }
  }
}
