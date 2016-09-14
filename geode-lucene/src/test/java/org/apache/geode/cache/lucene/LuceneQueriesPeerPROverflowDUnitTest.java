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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.*;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class LuceneQueriesPeerPROverflowDUnitTest extends LuceneQueriesPRBase {

  @Override protected void initDataStore(final SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    EvictionAttributes evicAttr = EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK);
    getCache().createRegionFactory(RegionShortcut.PARTITION_OVERFLOW)
      .setPartitionAttributes(getPartitionAttributes())
      .setEvictionAttributes(evicAttr)
      .create(REGION_NAME);
  }

  @Override protected void initAccessor(final SerializableRunnableIF createIndex) throws Exception {
    initDataStore(createIndex);
  }
}
