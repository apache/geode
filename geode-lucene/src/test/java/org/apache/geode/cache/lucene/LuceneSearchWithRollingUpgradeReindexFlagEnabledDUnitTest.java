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
package org.apache.geode.cache.lucene;

import java.io.File;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category({DistributedTest.class, BackwardCompatibilityTest.class})
public class LuceneSearchWithRollingUpgradeReindexFlagEnabledDUnitTest
    extends LuceneSearchWithRollingUpgradeDUnit {
  public LuceneSearchWithRollingUpgradeReindexFlagEnabledDUnitTest(String version) {
    super(version);
  }

  @Override
  protected VM createLuceneIndexAndRegionOnRolledServer(String regionType, File diskdir,
      String shortcutName, String regionName, VM rollServer) throws Exception {
    rollServer.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    return super.createLuceneIndexAndRegionOnRolledServer(regionType, diskdir, shortcutName,
        regionName, rollServer);
  }
}
