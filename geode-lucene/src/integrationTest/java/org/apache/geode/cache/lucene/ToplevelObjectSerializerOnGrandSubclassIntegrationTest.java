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

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.test.GrandSubCustomer;
import org.apache.geode.logging.internal.LogService;

public class ToplevelObjectSerializerOnGrandSubclassIntegrationTest extends LuceneIntegrationTest {
  protected static int WAIT_FOR_FLUSH_TIME = 10000;
  protected static final Logger logger = LogService.getLogger();
  LuceneQuery<Integer, Object> query;
  PageableLuceneQueryResults<Integer, Object> results;

  protected Region createRegionAndIndex() {
    luceneService.createIndexFactory().addField("name").create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    return region;
  }

  @Test
  public void shouldIndexOnToplevelFieldUsingDefaultSerializer()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    GrandSubCustomer grandSubCustomer = new GrandSubCustomer("Tommy Jackson", null, null, null);
    region.put("key-1", grandSubCustomer);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "Tommy*",
        "name");
    results = query.findPages();
    assertEquals(1, results.size());

    results.next().stream().forEach(struct -> {
      assertTrue(struct.getValue() instanceof GrandSubCustomer);
    });

  }
}
