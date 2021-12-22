/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.partitioned;


import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.PartitionedRegionTestHelper;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Class verifies Region#query(Select Query) API for PartitionedRegion on a single VM.,As
 * region#query doesn't support Select Query for Local Querying it should throw
 * QueryInvalidException
 *
 *
 */
@Category({OQLQueryTest.class})
public class PRInvalidQueryJUnitTest {
  String regionName = "Portfolios";

  LogWriter logger = null;

  @Before
  public void setUp() throws Exception {
    if (logger == null) {
      logger = PartitionedRegionTestHelper.getLogger();
    }
  }

  /**
   * Populates the region with the Objects stores in the data Object array.
   *
   */
  private void populateData(Region region, Object[] data) {
    logger.info("PRInvalidQueryJUnitTest#populateData: Populating Data to the region");

    for (int j = 0; j < data.length; j++) {
      region.put(j, data[j]);
    }
  }
}
