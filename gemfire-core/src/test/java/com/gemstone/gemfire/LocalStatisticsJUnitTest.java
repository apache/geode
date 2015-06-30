/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import java.util.*;

import org.junit.experimental.categories.Category;

/**
 * Tests the functionality of JOM {@link Statistics}.
 */
@Category(IntegrationTest.class)
public class LocalStatisticsJUnitTest extends StatisticsTestCase {

  /**
   * Returns a distributed system configured to not use shared
   * memory.
   */
  protected DistributedSystem getSystem() {
    if (this.system == null) {
      Properties props = new Properties();
      props.setProperty("statistic-sampling-enabled", "true");
      props.setProperty("statistic-archive-file", "StatisticsTestCase-localTest.gfs");
      props.setProperty("mcast-port", "0");
      props.setProperty("locators", "");
      props.setProperty(DistributionConfig.NAME_NAME, getName());
      this.system = DistributedSystem.connect(props);
    }

    return this.system;
  }
}
