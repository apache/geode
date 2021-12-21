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
package org.apache.geode.cache.lucene.internal.repository;


import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.converters.AvailableCommandsConverter;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIntegrationTest;
import org.apache.geode.cache.lucene.test.TestObject;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.size.ObjectGraphSizer;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.categories.PerformanceTest;

@Category({PerformanceTest.class, LuceneTest.class})
@Ignore("Tests have no assertions")
public class LuceneIndexMemoryOverheadTest extends LuceneIntegrationTest {
  private static final String REGION_NAME = "index";
  private static final int NUM_BATCHES = 30;
  private static final int NUM_ENTRIES = 10000;
  private final Random random = new Random(0);

  private static final Logger logger = LogService.getLogger();
  private final Callable flush =
      () -> luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);
  protected static ObjectGraphSizer.ObjectFilter filter =
      (parent, object) -> !(object instanceof AvailableCommandsConverter);

  @Test
  public void perEntryNonIndexMemoryWithTombstoneGC() throws Exception {
    measureOverhead(((InternalCache) cache).getTombstoneService(), () -> true);
  }

  @Test
  public void perEntryNonIndexMemoryWithoutTombstoneGC() throws Exception {
    measureOverhead(null, () -> true);
  }

  @Test
  public void perEntryIndexMemoryWithTombstoneGC() throws Exception {
    luceneService.createIndexFactory().setFields("field1", "field2").create(INDEX_NAME,
        REGION_NAME);

    measureOverhead(((InternalCache) cache).getTombstoneService(), flush);
  }

  @Test
  public void perEntryIndexMemoryWithoutTombstoneGC() throws Exception {
    luceneService.createIndexFactory().setFields("field1", "field2").create(INDEX_NAME,
        REGION_NAME);

    measureOverhead(null, flush);
  }

  private void waitForFlush(Callable flush) throws Exception {
    while (flush.call().equals(false)) {
      logger.info("Flush returned false. Waiting for flush to return true...");
    }
  }

  private void measureOverhead(TombstoneService service, Callable flush) throws Exception {

    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
    PartitionRegionHelper.assignBucketsToPartitions(region);

    long emptySize = ObjectGraphSizer.size(cache, filter, false);
    waitForFlush(flush);

    long size;
    int numEntries = 0;

    for (int batch = 0; batch < NUM_BATCHES; batch++) {

      size =
          addEntriesAndGetMemorySize(region, numEntries, NUM_ENTRIES, service, flush) - emptySize;
      numEntries = region.size();

      logger.info(
          numEntries + " entries take up " + size + ", per entry overhead " + size / numEntries);
    }
  }

  private long addEntriesAndGetMemorySize(Region region, int start, int count,
      TombstoneService service, Callable flush) throws Exception {
    int c = 0;
    for (int i = start; i < start + count; i++) {
      byte[] field1 = new byte[3];
      byte[] field2 = new byte[5];
      random.nextBytes(field1);
      random.nextBytes(field2);
      TestObject test = new TestObject(new String(field1, StandardCharsets.US_ASCII),
          new String(field2, StandardCharsets.US_ASCII));
      region.put(i, test);
    }

    waitForFlush(flush);
    if (service != null) {
      service.forceBatchExpirationForTests((int) service.getScheduledTombstoneCount());
    }

    return ObjectGraphSizer.size(cache, filter, false);
  }
}
