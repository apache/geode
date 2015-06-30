/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

/**
 * Tests regions operations when entries are not yet persisted
 * in HDFS but are in HDFSAsyncQueue
 * @author sbawaska
 */
@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSQueueRegionOperationsJUnitTest extends
    HDFSRegionOperationsJUnitTest {

  @Override
  protected int getBatchTimeInterval() {
    return 50*1000;
  }

  @Override
  protected void sleep(String regionPath) {
  }
}
