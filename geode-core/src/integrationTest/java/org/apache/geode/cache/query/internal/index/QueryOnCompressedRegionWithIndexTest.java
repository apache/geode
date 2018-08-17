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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT_OVERFLOW;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({OQLIndexTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class QueryOnCompressedRegionWithIndexTest {

  private Cache cache;

  private Region region;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<RegionShortcut> getRegionShortcuts() {
    List<RegionShortcut> shortcuts = new ArrayList<>();
    shortcuts.add(PARTITION);
    shortcuts.add(PARTITION_REDUNDANT);
    shortcuts.add(PARTITION_PERSISTENT);
    shortcuts.add(PARTITION_REDUNDANT_PERSISTENT);
    shortcuts.add(PARTITION_OVERFLOW);
    shortcuts.add(PARTITION_REDUNDANT_OVERFLOW);
    shortcuts.add(PARTITION_PERSISTENT_OVERFLOW);
    shortcuts.add(PARTITION_REDUNDANT_PERSISTENT_OVERFLOW);
    shortcuts.add(REPLICATE);
    shortcuts.add(REPLICATE_PERSISTENT);
    shortcuts.add(REPLICATE_OVERFLOW);
    shortcuts.add(REPLICATE_PERSISTENT_OVERFLOW);
    return shortcuts;
  }

  @Parameterized.Parameter
  public RegionShortcut shortcut;

  @Before
  public void createCache() {
    // Create cache
    CacheFactory cf = new CacheFactory();
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cf.setPdxPersistent(true);
    this.cache = cf.create();

    // Create region with compression enabled
    this.region = createRegion("region_" + shortcut, shortcut);
  }

  @After
  public void closeCache() {
    // Destroy the region
    if (this.region != null) {
      this.region.destroyRegion();
    }

    // Destroy the cache
    if (this.cache != null) {
      this.cache.close();
    }

    // Delete backup files
    deleteBackupFiles();
  }

  private void deleteBackupFiles() {
    File backupBaseDir = new File(".");
    Pattern pattern = Pattern.compile("BACKUP.*");
    File[] files = backupBaseDir.listFiles((dir, name) -> pattern.matcher(name).matches());
    for (File file : files) {
      file.delete();
    }
  }

  @Test
  public void testCreateIndexThenAddEntries() throws Exception {
    // Create index
    String indexName = this.region.getName() + "_index";
    createIndex(indexName, "status", this.region.getFullPath());

    // Load entries
    int numObjects = 10;
    loadEntries(this.region, numObjects, false);

    // Execute queries and validate results
    executeQueriesAndValidateResults(numObjects, indexName);
  }

  @Test
  public void testCreateIndexThenAddPdxEntries() throws Exception {
    // Create index
    String indexName = this.region.getName() + "_index";
    createIndex(indexName, "status", this.region.getFullPath());

    // Load entries
    int numObjects = 10;
    loadEntries(this.region, numObjects, true);

    // Execute queries and validate results
    executeQueriesAndValidateResults(numObjects, indexName);
  }

  @Test
  public void testAddEntriesThenCreateIndex() throws Exception {
    // Load entries
    int numObjects = 10;
    loadEntries(this.region, numObjects, false);

    // Create index
    String indexName = this.region.getName() + "_index";
    createIndex(indexName, "status", this.region.getFullPath());

    // Execute queries and validate results
    executeQueriesAndValidateResults(numObjects, indexName);
  }

  @Test
  public void testAddPdxEntriesThenCreateIndex() throws Exception {
    // Load entries
    int numObjects = 10;
    loadEntries(this.region, numObjects, true);

    // Create index
    String indexName = this.region.getName() + "_index";
    createIndex(indexName, "status", this.region.getFullPath());

    // Execute queries and validate results
    executeQueriesAndValidateResults(numObjects, indexName);
  }

  private Region createRegion(String regionName, RegionShortcut shortcut) {
    return this.cache.createRegionFactory(shortcut).setCompressor(new SnappyCompressor())
        .create(regionName);
  }

  private Index createIndex(String indexName, String indexedExpression, String regionPath)
      throws Exception {
    return this.cache.getQueryService().createIndex(indexName, indexedExpression, regionPath);
  }

  private void loadEntries(Region region, int numObjects, boolean usePdx) {
    for (int i = 0; i < numObjects; i++) {
      region.put(i, usePdx ? new PortfolioPdx(i) : new Portfolio(i));
    }
  }

  private void executeQueriesAndValidateResults(int numObjects, String indexName) throws Exception {
    executeQuery("select * from " + this.region.getFullPath() + " where status = 'inactive'",
        numObjects / 2, indexName);
    executeQuery("select * from " + this.region.getFullPath() + " where status = 'active'",
        numObjects / 2, indexName);
    executeQuery("select * from " + this.region.getFullPath() + " where status = null", 0,
        indexName);
  }

  private void executeQuery(String queryStr, int expectedResults, String indexName)
      throws Exception {
    // Set query observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);

    // Execute query
    Query query = this.cache.getQueryService().newQuery(queryStr);
    SelectResults results = (SelectResults) query.execute();

    // Validate results size
    assertThat(results.size()).isEqualTo(expectedResults);

    // Validate index was used
    assertThat(observer.wasIndexUsed(indexName)).isTrue();
  }

  class QueryObserverImpl extends QueryObserverAdapter {
    List indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public boolean wasIndexUsed(String indexName) {
      return indexesUsed.contains(indexName);
    }
  }
}
