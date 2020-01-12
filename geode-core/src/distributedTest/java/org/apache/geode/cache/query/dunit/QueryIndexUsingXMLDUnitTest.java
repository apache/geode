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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.functional.StructSetOrResultsSet;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category({OQLIndexTest.class})
public class QueryIndexUsingXMLDUnitTest extends JUnit4CacheTestCase {

  private static final String NAME = "PartitionedPortfolios";
  private static final String REP_REG_NAME = "Portfolios";
  private static final String PERSISTENT_REG_NAME = "PersistentPrPortfolios";
  private static final String NAME_WITH_RANGE = "PartitionedPortfoliosWithRange";
  private static final String NAME_WITH_HASH = "PartitionedPortfoliosWithHash";
  private static final String REP_REG_NAME_WITH_RANGE = "PortfoliosWithRange";
  private static final String REP_REG_NAME_WITH_HASH = "PortfoliosWithHash";
  private static final String PERSISTENT_REG_NAME_WITH_RANGE = "PersistentPrPortfoliosWithRange";
  private static final String PERSISTENT_REG_NAME_WITH_HASH = "PersistentPrPortfoliosWithHash";
  private static final String NO_INDEX_REP_REG = "PortfoliosNoIndex";
  private static final String STATUS_INDEX = "statusIndex";
  private static final String ID_INDEX = "idIndex";

  private static final String[][] QUERY_STR = new String[][] {
      {"Select * from /" + NAME + " where ID > 10",
          "Select * from /" + REP_REG_NAME + " where ID > 10",
          "Select * from /" + PERSISTENT_REG_NAME + " where ID > 10",},
      {"Select * from /" + NAME + " where ID = 5",
          "Select * from /" + REP_REG_NAME + " where ID = 5",
          "Select * from /" + PERSISTENT_REG_NAME + " where ID = 5",
          "Select * from /" + NAME_WITH_HASH + " where ID = 5",
          "Select * from /" + REP_REG_NAME_WITH_HASH + " where ID = 5",
          "Select * from /" + PERSISTENT_REG_NAME_WITH_HASH + " where ID = 5"},
      {"Select * from /" + NAME + " where status = 'active'",
          "Select * from /" + REP_REG_NAME + " where status = 'active'",
          "Select * from /" + PERSISTENT_REG_NAME + " where status = 'active'",
          "Select * from /" + NAME_WITH_HASH + " where status = 'active'",
          "Select * from /" + REP_REG_NAME_WITH_HASH + " where status = 'active'",
          "Select * from /" + PERSISTENT_REG_NAME_WITH_HASH + " where status = 'active'"}};

  private static final String[] QUERY_STR_NO_INDEX =
      new String[] {"Select * from /" + NO_INDEX_REP_REG + " where ID > 10",
          "Select * from /" + NO_INDEX_REP_REG + " where ID = 5",
          "Select * from /" + NO_INDEX_REP_REG + " where status = 'active'"};

  private static final String PERSISTENT_OVER_FLOW_REG_NAME = "PersistentOverflowPortfolios";

  private static final String CACHE_XML_FILE_NAME = "IndexCreation.xml";

  private File cacheXmlFile;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void before() throws Exception {
    addIgnoredException("Failed to create index");

    URL url = getClass().getResource(CACHE_XML_FILE_NAME);
    assertThat(url).isNotNull(); // precondition

    this.cacheXmlFile = this.temporaryFolder.newFile(CACHE_XML_FILE_NAME);
    FileUtils.copyURLToFile(url, this.cacheXmlFile);
    assertThat(this.cacheXmlFile).exists(); // precondition
  }

  @After
  public void after() throws Exception {
    invokeInEveryVM(resetTestHook());
    disconnectFromDS();
  }

  /**
   * Creates partitioned index from an xml description.
   */
  @Test
  public void testCreateIndexThroughXML() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    AsyncInvocation async0 = vm0.invokeAsync(createIndexThroughXML(NAME));
    AsyncInvocation async1 = vm1.invokeAsync(createIndexThroughXML(NAME));

    async1.await();
    async0.await();

    // Check index for PR
    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, -1));
    vm0.invoke(prIndexCreationCheck(NAME, ID_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(NAME, ID_INDEX, -1));
    vm0.invoke(prIndexCreationCheck(NAME, "secIndex", -1));
    vm1.invoke(prIndexCreationCheck(NAME, "secIndex", -1));

    // Check index for replicated
    vm0.invoke(indexCreationCheck(REP_REG_NAME, STATUS_INDEX));
    vm1.invoke(indexCreationCheck(REP_REG_NAME, STATUS_INDEX));

    // Check index for persistent pr region
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, STATUS_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, STATUS_INDEX, -1));

    // check range index creation
    vm0.invoke(prIndexCreationCheck(NAME_WITH_RANGE, STATUS_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_RANGE, STATUS_INDEX, -1));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_RANGE, ID_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_RANGE, ID_INDEX, -1));
    vm0.invoke(indexCreationCheck(REP_REG_NAME_WITH_RANGE, STATUS_INDEX));
    vm1.invoke(indexCreationCheck(REP_REG_NAME_WITH_RANGE, STATUS_INDEX));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_RANGE, STATUS_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_RANGE, STATUS_INDEX, -1));

    // check hash index creation
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, -1));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, ID_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, ID_INDEX, -1));
    vm0.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, STATUS_INDEX));
    vm1.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, STATUS_INDEX));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, STATUS_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, STATUS_INDEX, -1));
  }

  /**
   * Creates partitioned index from an xml description.
   */
  @Test
  public void testCreateIndexWhileDoingGII() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    vm0.invoke(createIndexThroughXML(NAME));

    // LoadRegion
    vm0.invoke(loadRegion(NAME));
    vm0.invoke(loadRegion(NAME_WITH_HASH));
    vm0.invoke(loadRegion(NAME_WITH_RANGE));
    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, -1));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, -1));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_RANGE, STATUS_INDEX, -1));

    vm1.invoke(setTestHook());
    vm1.invoke(createIndexThroughXML(NAME));

    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME, ID_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME, ID_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(NAME, "secIndex", 50));

    // check range index creation
    vm0.invoke(prIndexCreationCheck(NAME_WITH_RANGE, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_RANGE, STATUS_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_RANGE, ID_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_RANGE, ID_INDEX, 50));

    // check hash index creation
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, ID_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, ID_INDEX, 50));

    // Execute query and verify index usage
    vm0.invoke(executeQuery(NAME));
    vm1.invoke(executeQuery(NAME));
  }

  /**
   * Creates partitioned index from an xml description.
   */
  @Test
  public void testReplicatedRegionCreateIndexWhileDoingGII() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    vm0.invoke(createIndexThroughXML(REP_REG_NAME));

    // LoadRegion
    vm0.invoke(loadRegion(REP_REG_NAME));
    vm0.invoke(loadRegion(REP_REG_NAME_WITH_HASH));
    vm0.invoke(loadRegion(NO_INDEX_REP_REG));
    vm0.invoke(indexCreationCheck(REP_REG_NAME, STATUS_INDEX));
    vm0.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, STATUS_INDEX));

    vm1.invoke(setTestHook());
    vm1.invoke(createIndexThroughXML(REP_REG_NAME));

    vm0.invoke(indexCreationCheck(REP_REG_NAME, STATUS_INDEX));
    vm1.invoke(indexCreationCheck(REP_REG_NAME, STATUS_INDEX));
    vm0.invoke(indexCreationCheck(REP_REG_NAME, ID_INDEX));
    vm1.invoke(indexCreationCheck(REP_REG_NAME, ID_INDEX));
    vm0.invoke(indexCreationCheck(REP_REG_NAME, "secIndex"));
    vm1.invoke(indexCreationCheck(REP_REG_NAME, "secIndex"));

    // check hash index creation
    vm0.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, STATUS_INDEX));
    vm1.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, STATUS_INDEX));
    vm0.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, ID_INDEX));
    vm1.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, ID_INDEX));
    vm0.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, "secIndex"));
    vm1.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, "secIndex"));

    // Execute query and verify index usage
    vm0.invoke(executeQuery(REP_REG_NAME));
    vm1.invoke(executeQuery(REP_REG_NAME));
  }

  /**
   * Creates persistent partitioned index from an xml description.
   */
  @Test
  public void testPersistentPRRegionCreateIndexWhileDoingGII() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    vm0.invoke(createIndexThroughXML(PERSISTENT_REG_NAME));

    // LoadRegion
    vm0.invoke(loadRegion(PERSISTENT_REG_NAME));
    vm0.invoke(loadRegion(NO_INDEX_REP_REG));
    vm0.invoke(loadRegion(PERSISTENT_REG_NAME_WITH_HASH));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, STATUS_INDEX, -1));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, STATUS_INDEX, -1));

    vm1.invoke(setTestHook());
    vm1.invoke(createIndexThroughXML(PERSISTENT_REG_NAME));

    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, STATUS_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, ID_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, ID_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, "secIndex", 50));

    // check hash index creation
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, STATUS_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, ID_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, ID_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, "secIndex", 50));

    // Execute query and verify index usage
    vm0.invoke(executeQuery(PERSISTENT_REG_NAME));
    vm1.invoke(executeQuery(PERSISTENT_REG_NAME));

    // close one vm cache
    vm1.invoke(resetTestHook());
    vm1.invoke(() -> closeCache());

    // restart
    vm1.invoke(setTestHook());
    vm1.invoke(createIndexThroughXML(PERSISTENT_REG_NAME));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, STATUS_INDEX, 50));
  }

  /**
   * Creates partitioned index from an xml description.
   */
  @Test
  public void testCreateIndexWhileDoingGIIWithEmptyPRRegion() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("### in testCreateIndexWhileDoingGIIWithEmptyPRRegion.");

    vm0.invoke(createIndexThroughXML(NAME));
    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, -1));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, -1));

    vm1.invoke(setTestHook());
    vm1.invoke(createIndexThroughXML(NAME));
    vm1.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, -1));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, -1));

    // LoadRegion
    vm0.invoke(loadRegion(NAME));
    vm0.invoke(loadRegion(NAME_WITH_HASH));

    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, 50));
  }

  /**
   * Creates partitioned index from an xml description.
   */
  @Test
  public void testCreateAsyncIndexWhileDoingGII() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    AsyncInvocation async0 = vm0.invokeAsync(createIndexThroughXML(NAME));

    async0.await();

    // LoadRegion
    async0 = vm0.invokeAsync(loadRegion(NAME));

    vm1.invoke(setTestHook());

    AsyncInvocation async1 = vm1.invokeAsync(createIndexThroughXML(NAME));

    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));

    async1.await();

    vm1.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));

    async0.await();
  }

  /**
   * Creates indexes and compares the results between index and non-index results.
   */
  @Test
  public void testCreateIndexWhileDoingGIIAndCompareQueryResults() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    vm0.invoke(createIndexThroughXML(NAME));

    // LoadRegion
    vm0.invoke(loadRegion(NAME));
    vm0.invoke(loadRegion(REP_REG_NAME));
    vm0.invoke(loadRegion(PERSISTENT_REG_NAME));
    vm0.invoke(loadRegion(NO_INDEX_REP_REG));
    vm0.invoke(loadRegion(NAME_WITH_HASH));
    vm0.invoke(loadRegion(REP_REG_NAME_WITH_HASH));
    vm0.invoke(loadRegion(PERSISTENT_REG_NAME_WITH_HASH));
    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, -1));

    vm1.invoke(setTestHook());
    vm1.invoke(createIndexThroughXML(NAME));

    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME, ID_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME, ID_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(NAME, "secIndex", 50));

    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, STATUS_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, ID_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, ID_INDEX, 50));
    vm0.invoke(prIndexCreationCheck(NAME_WITH_HASH, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(NAME_WITH_HASH, "secIndex", 50));

    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, "secIndex", 50));
    vm0.invoke(indexCreationCheck(REP_REG_NAME, "secIndex"));
    vm0.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, "secIndex", 50));
    vm0.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, "secIndex"));

    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, "secIndex", 50));
    vm1.invoke(indexCreationCheck(REP_REG_NAME, "secIndex"));
    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME_WITH_HASH, "secIndex", 50));
    vm1.invoke(indexCreationCheck(REP_REG_NAME_WITH_HASH, "secIndex"));

    // Execute query and verify index usage
    vm0.invoke(executeQueryAndCompareResult(true));
    vm1.invoke(executeQueryAndCompareResult(true));
  }

  /**
   * Creates async partitioned index from an xml description.
   */
  @Test
  public void testCreateAsyncIndexWhileDoingGIIAndQuery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    AsyncInvocation async0 = vm0.invokeAsync(createIndexThroughXML(NAME));

    async0.await();

    // LoadRegion
    async0 = vm0.invokeAsync(loadRegion(NAME));

    vm1.invoke(setTestHook());

    AsyncInvocation async1 = vm1.invokeAsync(createIndexThroughXML(NAME));

    async1.await();
    async0.await();

    vm0.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));
    vm1.invoke(prIndexCreationCheck(NAME, STATUS_INDEX, 50));

    // Execute query and verify index usage
    vm0.invoke(executeQuery(NAME));
    vm1.invoke(executeQuery(NAME));
  }

  /**
   * Creates async indexes and compares the results between index and non-index results.
   */
  @Test
  public void testCreateAsyncIndexWhileDoingGIIAndCompareQueryResults() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    vm0.invoke(createIndexThroughXML(NAME));

    // LoadRegion
    vm0.invoke(loadRegion(NAME));
    vm0.invoke(loadRegion(REP_REG_NAME));
    vm0.invoke(loadRegion(PERSISTENT_REG_NAME));
    vm0.invoke(loadRegion(NO_INDEX_REP_REG));

    // Start async update
    vm0.invokeAsync(loadRegion(NAME, 500));
    vm0.invokeAsync(loadRegion(REP_REG_NAME, 500));

    AsyncInvocation async0 = vm0.invokeAsync(loadRegion(PERSISTENT_REG_NAME, 500));

    vm0.invokeAsync(loadRegion(NO_INDEX_REP_REG, 500));

    vm1.invoke(setTestHook());
    vm1.invoke(createIndexThroughXML(NAME));

    async0.await();

    vm1.invoke(prIndexCreationCheck(PERSISTENT_REG_NAME, "secIndex", 50));
    vm1.invoke(indexCreationCheck(REP_REG_NAME, "secIndex"));

    vm0.invoke(() -> validateIndexSize());
    vm1.invoke(() -> validateIndexSize());


    // Execute query and verify index usage
    vm0.invoke(executeQueryAndCompareResult(false));
    vm1.invoke(executeQueryAndCompareResult(false));
  }

  public void validateIndexSize() {
    await().untilAsserted(() -> {
      boolean indexSizeCheck_NAME = validateIndexSizeForRegion(NAME);
      boolean indexSizeCheck_REP_REG_NAME = validateIndexSizeForRegion(REP_REG_NAME);
      boolean indexSizeCheck_PERSISTENT_REG_NAME = validateIndexSizeForRegion(PERSISTENT_REG_NAME);
      assertEquals("Index does not contain all the entries after 60 seconds have elapsed ", true,
          (indexSizeCheck_NAME && indexSizeCheck_REP_REG_NAME
              && indexSizeCheck_PERSISTENT_REG_NAME));
    });
  }

  private boolean validateIndexSizeForRegion(final String regionName) {
    Region region = getCache().getRegion(regionName);
    QueryService queryService = getCache().getQueryService();
    return queryService.getIndex(region, "statusIndex").getStatistics().getNumberOfValues() == 500
        && queryService.getIndex(region, "idIndex").getStatistics().getNumberOfValues() == 500
        && queryService.getIndex(region, "statusIndex").getStatistics().getNumberOfValues() == 500;
  }

  @Test
  public void testIndexCreationForReplicatedPersistentOverFlowRegionOnRestart() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter().info("Creating index using an xml file name : " + CACHE_XML_FILE_NAME);

    // create index using xml
    vm0.invoke(createIndexThroughXML(PERSISTENT_OVER_FLOW_REG_NAME));
    // verify index creation
    vm0.invoke(indexCreationCheck(PERSISTENT_OVER_FLOW_REG_NAME, STATUS_INDEX));
    // LoadRegion
    vm0.invoke(loadRegion(PERSISTENT_OVER_FLOW_REG_NAME));
    // close cache without deleting diskstore
    vm0.invoke(closeWithoutDeletingDiskStore());
    // start cache by recovering data from diskstore
    vm0.invoke(createIndexThroughXML(PERSISTENT_OVER_FLOW_REG_NAME));
    // verify index creation on restart
    vm0.invoke(indexCreationCheck(PERSISTENT_OVER_FLOW_REG_NAME, STATUS_INDEX));
  }

  private CacheSerializableRunnable setTestHook() {
    return new CacheSerializableRunnable("TestHook") {
      @Override
      public void run2() {
        class IndexTestHook implements IndexManager.TestHook {
          @Override
          public void hook(int spot) {
            getLogWriter().fine("In IndexTestHook.hook(). hook() argument value is : " + spot);
            if (spot == 1) {
              throw new RuntimeException("Index is not created as part of Region GII.");
            }
          }
        }
        IndexManager.testHook = new IndexTestHook();
      }
    };
  }

  private CacheSerializableRunnable resetTestHook() {
    return new CacheSerializableRunnable("TestHook") {
      @Override
      public void run2() {
        IndexManager.testHook = null;
      }
    };
  }

  private CacheSerializableRunnable createIndexThroughXML(final String regionName) {
    return new CacheSerializableRunnable("RegionCreator") {
      @Override
      public void run2() {
        Properties properties = new Properties();
        properties.setProperty(CACHE_XML_FILE, cacheXmlFile.getAbsolutePath());
        getSystem(properties);
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        assertThat(region).isNotNull();
      }
    };
  }

  private CacheSerializableRunnable prIndexCreationCheck(final String regionName,
      final String indexName, final int bucketCount) {
    return new CacheSerializableRunnable(
        "pr IndexCreationCheck " + regionName + " indexName :" + indexName) {
      @Override
      public void run2() {
        Cache cache = getCache();
        LogWriter logger = cache.getLogger();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        PartitionedIndex index = (PartitionedIndex) region.getIndex().get(indexName);
        assertThat(index).isNotNull();

        logger.info("Current number of buckets indexed: " + index.getNumberOfIndexedBuckets());
        if (bucketCount >= 0) {
          waitForIndexedBuckets(index, bucketCount);
        }
        assertThat(index.isPopulated()).isTrue();
      }
    };
  }

  private CacheSerializableRunnable indexCreationCheck(final String regionName,
      final String indexName) {
    return new CacheSerializableRunnable(
        "IndexCreationCheck region: " + regionName + " indexName:" + indexName) {
      @Override
      public void run2() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(regionName);
        Index index = region.getIndexManager().getIndex(indexName);
        assertThat(index).isNotNull();
      }
    };
  }

  private void waitForIndexedBuckets(final PartitionedIndex index, final int bucketCount) {
    await().until(() -> index.getNumberOfIndexedBuckets() >= bucketCount);
  }

  private CacheSerializableRunnable loadRegion(final String name) {
    return new CacheSerializableRunnable("load region on " + name) {
      @Override
      public void run2() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        for (int i = 0; i < 100; i++) {
          region.put(i, new Portfolio(i));
        }
      }
    };
  }

  private CacheSerializableRunnable loadRegion(final String name, final int size) {
    return new CacheSerializableRunnable("LoadRegion: " + name + " size :" + size) {
      @Override
      public void run2() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        for (int i = 0; i < size; i++) {
          region.put(i, new Portfolio(i));
        }
      }
    };
  }

  private CacheSerializableRunnable executeQuery(final String regionName) {
    return new CacheSerializableRunnable("execute query on " + regionName) {
      @Override
      public void run2() {
        QueryService qs = getCache().getQueryService();
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        String queryString = "Select * from /" + regionName + " where ID > 10";
        Query query = qs.newQuery(queryString);
        try {
          query.execute();
        } catch (Exception ex) {
          throw new AssertionError("Failed to execute the query.", ex);
        }
        assertThat(observer.isIndexesUsed).isTrue().as("Index not used for query. " + queryString);
      }
    };
  }

  private CacheSerializableRunnable executeQueryAndCompareResult(final boolean compareHash) {
    return new CacheSerializableRunnable("execute query and compare results.") {
      @Override
      public void run2() {
        QueryService qs = getCache().getQueryService();

        StructSetOrResultsSet resultsSet = new StructSetOrResultsSet();
        SelectResults[][] selectResults = new SelectResults[1][2];
        String[] queryStrings = new String[2];

        int numQueries = QUERY_STR.length;
        for (int j = 0; j < numQueries; j++) {
          String[] queryArray = QUERY_STR[j];
          int numQueriesToCheck = compareHash ? queryArray.length : 3;
          for (int i = 0; i < numQueriesToCheck; i++) {
            QueryObserverImpl observer = new QueryObserverImpl();
            QueryObserverHolder.setInstance(observer);
            // Query using index.
            queryStrings[0] = QUERY_STR[j][i];
            // Execute query with index.
            Query query = qs.newQuery(queryStrings[0]);

            try {
              selectResults[0][0] = (SelectResults) query.execute();
            } catch (Exception ex) {
              throw new AssertionError("Failed to execute the query.", ex);
            }
            assertThat(observer.isIndexesUsed).isTrue()
                .as("Index not used for query. " + queryStrings[0]);

            // Query using no index.
            queryStrings[1] = QUERY_STR_NO_INDEX[j];
            try {
              query = qs.newQuery(queryStrings[1]);
              selectResults[0][1] = (SelectResults) query.execute();
            } catch (Exception ex) {
              throw new AssertionError("Failed to execute the query on no index region.", ex);
            }

            // compare.
            getLogWriter().info("Execute query : " + System.getProperty("line.separator")
                + " QUERY_STR with index: " + queryStrings[0] + " "
                + System.getProperty("line.separator") + " QUERY_STR without index: "
                + queryStrings[1]);
            resultsSet.CompareQueryResultsWithoutAndWithIndexes(selectResults, 1, queryStrings);
          }
        }
      }
    };
  }

  private CacheSerializableRunnable closeWithoutDeletingDiskStore() {
    return new CacheSerializableRunnable("close") {
      @Override
      public void run2() {
        IndexManager.testHook = null;
        // close the cache.
        closeCache();
        disconnectFromDS();
      }
    };
  }

  private static class QueryObserverImpl extends QueryObserverAdapter {

    boolean isIndexesUsed;
    List indexesUsed = new ArrayList();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      this.indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        this.isIndexesUsed = true;
      }
    }
  }
}
