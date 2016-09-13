/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.REGION_NAME;

import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class LuceneQueriesPeerPRDUnitTest extends LuceneQueriesPRBase {

  @Override protected void initDataStore(final SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    getCache().createRegionFactory(RegionShortcut.PARTITION)
      .setPartitionAttributes(getPartitionAttributes())
      .create(REGION_NAME);
  }

  @Override protected void initAccessor(final SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    getCache().createRegionFactory(RegionShortcut.PARTITION_PROXY)
        .setPartitionAttributes(getPartitionAttributes())
        .create(REGION_NAME);
  }
}
