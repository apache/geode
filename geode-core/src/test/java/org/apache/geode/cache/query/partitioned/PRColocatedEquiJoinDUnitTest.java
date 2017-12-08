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
/**
 *
 */
package org.apache.geode.cache.query.partitioned;

import static org.apache.geode.cache.query.Utils.*;
import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import parReg.query.unittest.NewPortfolio;
import util.TestException;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.partitioned.PRQueryDUnitHelper.TestQueryFunction;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDUnitTestCase;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

/**
 *
 */
@Category(DistributedTest.class)
public class PRColocatedEquiJoinDUnitTest extends PartitionedRegionDUnitTestCase {

  int totalNumBuckets = 100;

  int queryTestCycle = 10;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  final String name = "Portfolios1";

  final String coloName = "Portfolios2";

  final String localName = "LocalPortfolios1";

  final String coloLocalName = "LocalPortfolios2";

  final int cnt = 0, cntDest = 200;

  final int redundancy = 1;

  public PRColocatedEquiJoinDUnitTest() {
    super();
  }

  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }


  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER, "parReg.query.unittest.**");
    return properties;
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRLocalQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName, redundancy, name));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  @Test
  public void testNonColocatedPRLocalQuerying() throws Exception {
    IgnoredException.addIgnoredException("UnsupportedOperationException");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    // Create second PR which is not colocated.
    vm0.invoke(new CacheSerializableRunnable(coloName) {
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        Region partitionedregion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(NewPortfolio.class);

          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          PartitionAttributes prAttr = paf.setRedundantCopies(redundancy).create();

          attr.setPartitionAttributes(prAttr);

          partitionedregion = cache.createRegion(coloName, attr.create());
        } catch (IllegalStateException ex) {
          LogWriterUtils.getLogWriter().warning(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Creation caught IllegalStateException",
              ex);
        }
        assertNotNull(
            "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region "
                + coloName + " not in cache",
            cache.getRegion(coloName));
        assertNotNull(
            "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region ref null",
            partitionedregion);
        assertTrue(
            "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region ref claims to be destroyed",
            !partitionedregion.isDestroyed());
      }
    });

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(new CacheSerializableRunnable("PRQuery") {
      @Override
      public void run2() throws CacheException {

        Cache cache = getCache();
        // Querying the PR region

        String[] queries = new String[] {"r1.ID = r2.id",};

        Object r[][] = new Object[queries.length][2];
        Region region = null;
        region = cache.getRegion(name);
        assertNotNull(region);
        region = cache.getRegion(coloName);
        assertNotNull(region);

        QueryService qs = getCache().getQueryService();
        Object[] params;
        try {
          for (int j = 0; j < queries.length; j++) {
            getCache().getLogger().info("About to execute local query: " + queries[j]);
            Function func = new TestQueryFunction("testfunction");

            Object funcResult = FunctionService
                .onRegion((getCache().getRegion(name) instanceof PartitionedRegion)
                    ? getCache().getRegion(name) : getCache().getRegion(coloName))
                .setArguments("Select " + (queries[j].contains("ORDER BY") ? "DISTINCT" : "")
                    + " * from /" + name + " r1, /" + coloName + " r2 where " + queries[j])
                .execute(func).getResult();

            r[j][0] = ((ArrayList) funcResult).get(0);
          }
          fail(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Queries Executed successfully with non-colocated region on one of the nodes");

        } catch (FunctionException e) {
          if (e.getCause() instanceof UnsupportedOperationException) {
            LogWriterUtils.getLogWriter()
                .info("Query received FunctionException successfully while using QueryService.");
          } else {
            fail("UnsupportedOperationException must be thrown here");
          }
        }
      }
    });

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }


  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRLocalQueryingWithIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID",
        "/" + name + " r1", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11",
    // "r1.status", "/"+name+" r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName, redundancy, name));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "r2.id",
        "/" + coloName + " r2", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22",
    // "r2.status", "/"+coloName+" r2", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRLocalQueryingWithIndexOnOneRegion() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID",
        "/" + name + " r1", null));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status",
        "/" + name + " r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName, redundancy, name));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRRRLocalQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRRRLocalQueryingWithIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID",
        "/" + name + " r1", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11",
    // "r1.status", "/"+name+" r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "r2.id",
        "/" + coloName + " r2", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22",
    // "r2.status", "/"+coloName+" r2", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRRRLocalQueryingWithIndexOnOnePRRegion() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID",
        "/" + name + " r1", null));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status",
        "/" + name + " r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testRRPRLocalQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(coloName, redundancy, NewPortfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(name, Portfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testRRPRLocalQueryingWithIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(coloName, redundancy, NewPortfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex1", "r2.id",
        "/" + coloName + " r2", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(name, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex2", "r1.ID",
        "/" + name + " r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testRRPRLocalQueryingWithIndexOnOnePRRegion() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(coloName, redundancy, NewPortfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(name, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID",
        "/" + name + " r1", null));


    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name,
        coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRNonLocalQueryException() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName, redundancy, name));
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName, redundancy, name));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(new CacheSerializableRunnable("PRQuery") {
      @Override
      public void run2() throws CacheException {

        Cache cache = getCache();
        // Querying the PR region

        String[] queries = new String[] {"r1.ID = r2.id",};

        Object r[][] = new Object[queries.length][2];
        Region region = null;
        region = cache.getRegion(name);
        assertNotNull(region);
        region = cache.getRegion(coloName);
        assertNotNull(region);
        region = cache.getRegion(localName);
        assertNotNull(region);
        region = cache.getRegion(coloLocalName);
        assertNotNull(region);

        final String[] expectedExceptions =
            new String[] {RegionDestroyedException.class.getName(), ReplyException.class.getName(),
                CacheClosedException.class.getName(), ForceReattemptException.class.getName(),
                QueryInvocationTargetException.class.getName()};

        for (int i = 0; i < expectedExceptions.length; i++) {
          getCache().getLogger().info(
              "<ExpectedException action=add>" + expectedExceptions[i] + "</ExpectedException>");
        }

        QueryService qs = getCache().getQueryService();
        Object[] params;
        try {
          for (int j = 0; j < queries.length; j++) {
            getCache().getLogger().info("About to execute local query: " + queries[j]);
            r[j][1] = qs
                .newQuery("Select " + (queries[j].contains("ORDER BY") ? "DISTINCT" : "")
                    + " * from /" + name + " r1, /" + coloName + " r2 where " + queries[j])
                .execute();
          }
          fail(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Queries Executed successfully on Local region & PR Region");

        } catch (QueryInvocationTargetException e) {
          // throw an unchecked exception so the controller can examine the
          // cause and see whether or not it's okay
          throw new TestException(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Caught unexpected query exception",
              e);
        } catch (QueryException e) {
          LogWriterUtils.getLogWriter().error(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Caught QueryException while querying"
                  + e,
              e);
          throw new TestException(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Caught unexpected query exception",
              e);
        } catch (UnsupportedOperationException uso) {
          LogWriterUtils.getLogWriter().info(uso.getMessage());
          if (!uso.getMessage().equalsIgnoreCase(
              LocalizedStrings.DefaultQuery_A_QUERY_ON_A_PARTITIONED_REGION_0_MAY_NOT_REFERENCE_ANY_OTHER_REGION_1
                  .toLocalizedString(new Object[] {name, "/" + coloName}))) {
            fail(
                "Query did not throw UnsupportedOperationException while using QueryService instead of LocalQueryService");
          } else {
            LogWriterUtils.getLogWriter().info(
                "Query received UnsupportedOperationException successfully while using QueryService.");
          }
        } finally {
          for (int i = 0; i < expectedExceptions.length; i++) {
            getCache().getLogger().info("<ExpectedException action=remove>" + expectedExceptions[i]
                + "</ExpectedException>");
          }
        }
      }
    });

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  @Test
  public void testPRRRLocalQueryingWithHetroIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID",
        "/" + name + " r1", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11",
    // "r1.status", "/"+name+" r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "r2.id",
        "/" + coloName + " r2, r2.positions.values pos2", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22",
    // "r2.status", "/"+coloName+" r2", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAndRRQueryAndCompareResults(name, coloName,
        localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }


  @Test
  @Category(FlakyTest.class) // GEODE-2022
  public void testRRPRLocalQueryingWithHetroIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(coloName, redundancy, NewPortfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex1", "r2.id",
        "/" + coloName + " r2", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11",
    // "r1.status", "/"+name+" r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(name, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex2", "r1.ID",
        "/" + name + " r1, r1.positions.values pos1", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22",
    // "r2.status", "/"+coloName+" r2", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForRRAndPRQueryAndCompareResults(name, coloName,
        localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  @Test
  public void testPRRRCompactRangeAndNestedRangeIndexQuerying() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID",
        "/" + name + " r1", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11",
    // "r1.status", "/"+name+" r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "pos2.id",
        "/" + coloName + " r2, r2.positions.values pos2", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22",
    // "r2.status", "/"+coloName+" r2", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, Portfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final Portfolio[] newPortfolio = createPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForPRAndRRQueryWithCompactAndRangeIndexAndCompareResults(name,
            coloName, localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  @Test
  public void testPRRRIndexQueryWithSameTypeIndexQueryResults() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID",
        "/" + name + " r1", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11",
    // "r1.status", "/"+name+" r1", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionWithAsyncIndexCreation(coloName,
        NewPortfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "r2.id",
        "/" + coloName + " r2", null));
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22",
    // "r2.status", "/"+coloName+" r2", null));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex3", "r1.ID",
        "/" + localName + " r1", null));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex4", "r2.id",
        "/" + coloLocalName + " r2", null));
    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio, cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // Let async index updates be finished.
    Wait.pause(5000);

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAndRRQueryAndCompareResults(name, coloName,
        localName, coloLocalName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1. 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRRRNonLocalQueryingWithNoRROnOneNode() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, 0, Portfolio.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, 0, Portfolio.class));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the RR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(new CacheSerializableRunnable("PRQuery") {
      @Override
      public void run2() throws CacheException {

        // Helper classes and function
        Cache cache = getCache();
        // Querying the PR region

        String[] queries = new String[] {"r1.ID = r2.id",};

        Object r[][] = new Object[queries.length][2];
        Region region = null;
        region = cache.getRegion(name);
        assertNotNull(region);
        region = cache.getRegion(coloName);
        assertNotNull(region);

        QueryService qs = getCache().getQueryService();
        Object[] params;
        try {
          for (int j = 0; j < queries.length; j++) {
            getCache().getLogger().info("About to execute local query: " + queries[j]);
            Function func = new TestQueryFunction("testfunction");

            Object funcResult = FunctionService
                .onRegion((getCache().getRegion(name) instanceof PartitionedRegion)
                    ? getCache().getRegion(name) : getCache().getRegion(coloName))
                .setArguments("Select " + (queries[j].contains("ORDER BY") ? "DISTINCT" : "")
                    + " * from /" + name + " r1, /" + coloName + " r2 where " + queries[j])
                .execute(func).getResult();

            r[j][0] = ((ArrayList) funcResult).get(0);
          }
          fail(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Queries Executed successfully without RR region on one of the nodes");

        } catch (FunctionException e) {
          if (e.getCause() instanceof RegionNotFoundException) {
            LogWriterUtils.getLogWriter()
                .info("Query received FunctionException successfully while using QueryService.");
          } else {
            fail("RegionNotFoundException must be thrown here");
          }
        }
      }
    });

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  // Helper classes and function
  public static class TestQueryFunction extends FunctionAdapter implements DataSerializable {

    public TestQueryFunction() {}

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }

    private String id;

    public TestQueryFunction(String id) {
      super();
      this.id = id;
    }

    @Override
    public void execute(FunctionContext context) {
      Cache cache = CacheFactory.getAnyInstance();
      QueryService queryService = cache.getQueryService();
      ArrayList allQueryResults = new ArrayList();
      String qstr = (String) context.getArguments();
      try {
        Query query = queryService.newQuery(qstr);
        context.getResultSender().sendResult(
            (ArrayList) ((SelectResults) query.execute((RegionFunctionContext) context)).asList());
        context.getResultSender().lastResult(null);
      } catch (Exception e) {
        e.printStackTrace();
        throw new FunctionException(e);
      }
    }

    @Override
    public String getId() {
      return this.id;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeUTF(id);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      id = in.readUTF();
    }
  }

}
