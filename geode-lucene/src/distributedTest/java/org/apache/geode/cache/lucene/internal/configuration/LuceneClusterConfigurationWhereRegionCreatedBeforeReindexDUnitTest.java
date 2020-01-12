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
package org.apache.geode.cache.lucene.internal.configuration;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneClusterConfigurationWhereRegionCreatedBeforeReindexDUnitTest
    extends LuceneClusterConfigurationDUnitTest {

  MemberVM[] servers = new MemberVM[3];

  @Override
  MemberVM createServer(int index) throws IOException {
    servers[index - 1] = super.createServer(index);
    servers[index - 1].invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    return servers[index - 1];

  }

  @After
  public void clearLuceneReindexFlag() {
    for (MemberVM server : servers) {
      if (server != null) {
        server.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
      }
    }
  }

  @Override
  void createLuceneIndexAndDataRegion() throws Exception {
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);
    createLuceneIndexUsingGfsh();
  }

  @Override
  void createLuceneIndexWithAnalyzerAndDataRegion() throws Exception {
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);
    createLuceneIndexWithAnalyzerUsingGfsh();
  }

  @Override
  void createAndAddIndexes() throws Exception {
    // Create region
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Create lucene index.
    createLuceneIndexUsingGfsh(INDEX_NAME + "0");

    // Create another lucene index.
    createLuceneIndexUsingGfsh(INDEX_NAME + "1");

  }

  @Override
  void createLuceneIndexWithSerializerAndDataRegion() throws Exception {
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);
    createLuceneIndexWithSerializerUsingGfsh();
  }

  @Override
  MemberVM createServer(Properties properties, int index) throws Exception {
    servers[index - 1] = super.createServer(properties, index);
    servers[index - 1].getVM().invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    return servers[index - 1];
  }
}
