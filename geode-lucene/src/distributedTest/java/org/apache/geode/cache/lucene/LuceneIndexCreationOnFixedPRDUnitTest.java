/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({LuceneTest.class})
@RunWith(GeodeParamsRunner.class)
public class LuceneIndexCreationOnFixedPRDUnitTest extends LuceneIndexCreationDUnitTest {

  @Override
  protected RegionTestableType[] getListOfRegionTestTypes() {
    return new RegionTestableType[] {RegionTestableType.FIXED_PARTITION};
  }

  @Override
  protected void initDataStore(SerializableRunnableIF createIndex, RegionTestableType regionType,
      String message) throws Exception {
    try {
      initDataStore(createIndex, regionType);
      fail("Should not have been able to create index");
    } catch (IllegalStateException e) {
      assertEquals(message, e.getMessage());
    }
  }

  @Override
  protected String getClassSimpleName() {
    return getClass().getSuperclass().getSimpleName();
  }
}
