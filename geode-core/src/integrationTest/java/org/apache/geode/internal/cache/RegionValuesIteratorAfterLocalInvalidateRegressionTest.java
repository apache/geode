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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;

/**
 * Confirm that bug 34583 is fixed. Cause of bug is recursion is entries iterator that causes stack
 * overflow.
 *
 * <p>
 * TRAC #34583: StackOverflowError while performing region.put() at bridge client.
 */
public class RegionValuesIteratorAfterLocalInvalidateRegressionTest {

  private static final int PUT_COUNT = 25000;

  private Cache cache;
  private Region<String, String> region;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    cache = new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(RegionShortcut.LOCAL);
    region = regionFactory.create(uniqueName);

    for (int i = 1; i <= PUT_COUNT; i++) {
      region.put("key" + i, "value" + i);
    }
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testBunchOfInvalidEntries() throws Exception {
    // make sure iterator works while values are valid
    Collection<String> valuesBefore = region.values();
    assertThat(valuesBefore).hasSize(PUT_COUNT);

    int iteratorCount = 0;
    for (Iterator iterator = valuesBefore.iterator(); iterator.hasNext(); iterator.next()) {
      iteratorCount++;
    }

    assertThat(iteratorCount).isEqualTo(PUT_COUNT);

    region.localInvalidateRegion();

    // now we expect iterator to stackOverflow if bug 34583 exists
    Collection<String> valuesAfter = region.values();
    assertThat(valuesAfter).isEmpty();

    iteratorCount = 0;
    for (Iterator iterator = valuesAfter.iterator(); iterator.hasNext(); iterator.next()) {
      iteratorCount++;
    }

    assertThat(iteratorCount).isZero();
  }
}
