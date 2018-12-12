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
package org.apache.geode.cache.query.partitioned;

import static org.apache.geode.cache.query.Utils.createNewPortfoliosAndPositions;
import static org.apache.geode.cache.query.Utils.createPortfoliosAndPositions;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import parReg.query.unittest.NewPortfolio;

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
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class PRColocatedEquiJoinDUnitTest extends CacheTestCase {

  private static final String name = "Portfolios1";
  private static final String coloName = "Portfolios2";
  private static final String localName = "LocalPortfolios1";
  private static final String coloLocalName = "LocalPortfolios2";

  private static final int cnt = 0;
  private static final int cntDest = 200;
  private static final int redundancy = 1;

  private final PRQueryDUnitHelper prQueryDUnitHelper = new PRQueryDUnitHelper();

  @After
  public void tearDown() {
    disconnectAllFromDS();
    invokeInEveryVM(() -> PRQueryDUnitHelper.setCache(null));
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(SERIALIZABLE_OBJECT_FILTER, "parReg.query.unittest.**");
    return config;
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testPRLocalQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  @Test
  public void testNonColocatedPRLocalQuerying() throws Exception {
    addIgnoredException("UnsupportedOperationException");

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));

    // Create second PR which is not colocated.
    vm0.invoke(() -> {
      Cache cache = getCache();

      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      PartitionAttributes prAttr = paf.setRedundantCopies(redundancy).create();

      AttributesFactory attr = new AttributesFactory();
      attr.setValueConstraint(NewPortfolio.class);
      attr.setPartitionAttributes(prAttr);

      Region partitionedRegion = cache.createRegion(coloName, attr.create());

      assertNotNull(
          "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region "
              + coloName + " not in cache",
          cache.getRegion(coloName));
      assertNotNull(
          "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region ref null",
          partitionedRegion);
      assertTrue(
          "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region ref claims to be destroyed",
          !partitionedRegion.isDestroyed());
    });

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
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
          fail("Expected FunctionException with cause of UnsupportedOperationException");

        } catch (FunctionException e) {
          if (!(e.getCause() instanceof UnsupportedOperationException)) {
            throw e;
          }
        }
      }
    });
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testPRLocalQueryingWithIndexes() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1",
        "r1.ID", "/" + name + " r1", null));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2",
        "r2.id", "/" + coloName + " r2", null));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testPRLocalQueryingWithIndexOnOneRegion() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1",
        "r1.ID", "/" + name + " r1", null));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11",
        "r1.status", "/" + name + " r1", null));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testPRRRLocalQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloName,
        NewPortfolio.class));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testPRRRLocalQueryingWithIndexes() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1",
        "r1.ID", "/" + name + " r1", null));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloName,
        NewPortfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2",
        "r2.id", "/" + coloName + " r2", null));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testPRRRLocalQueryingWithIndexOnOnePRRegion() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1",
        "r1.ID", "/" + name + " r1", null));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11",
        "r1.status", "/" + name + " r1", null));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloName,
        NewPortfolio.class));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testRRPRLocalQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(coloName, redundancy,
        NewPortfolio.class));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(name,
        Portfolio.class));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testRRPRLocalQueryingWithIndexes() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(coloName, redundancy,
        NewPortfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex1",
        "r2.id", "/" + coloName + " r2", null));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(name,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex2",
        "r1.ID", "/" + name + " r1", null));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testRRPRLocalQueryingWithIndexOnOnePRRegion() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(coloName, redundancy,
        NewPortfolio.class));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(name,
        Portfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1",
        "r1.ID", "/" + name + " r1", null));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(
            name, coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testPRNonLocalQueryException() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(() -> {
      Cache cache = getCache();

      String[] queries = new String[] {"r1.ID = r2.id",};

      Object r[][] = new Object[queries.length][2];
      assertNotNull(cache.getRegion(name));
      assertNotNull(cache.getRegion(coloName));
      assertNotNull(cache.getRegion(localName));
      assertNotNull(cache.getRegion(coloLocalName));

      addIgnoredException(CacheClosedException.class.getName());
      addIgnoredException(ForceReattemptException.class.getName());
      addIgnoredException(QueryInvocationTargetException.class.getName());
      addIgnoredException(RegionDestroyedException.class.getName());
      addIgnoredException(ReplyException.class.getName());

      QueryService qs = getCache().getQueryService();
      try {
        for (int i = 0; i < queries.length; i++) {
          getCache().getLogger().info("About to execute local query: " + queries[i]);
          r[i][1] = qs.newQuery("Select " + (queries[i].contains("ORDER BY") ? "DISTINCT" : "")
              + " * from /" + name + " r1, /" + coloName + " r2 where " + queries[i]).execute();
        }
        fail("Expected UnsupportedOperationException");

      } catch (UnsupportedOperationException e) {
        if (!e.getMessage().equalsIgnoreCase(
            String.format(
                "A query on a Partitioned Region ( %s ) may not reference any other region if query is NOT executed within a Function",
                name))) {
          throw e;
        }
      }
    });
  }

  @Test
  public void testPRRRLocalQueryingWithHetroIndexes() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1",
        "r1.ID", "/" + name + " r1", null));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloName,
        NewPortfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2",
        "r2.id", "/" + coloName + " r2, r2.positions.values pos2", null));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAndRRQueryAndCompareResults(name,
        coloName, localName, coloLocalName));
  }

  @Test
  public void testPRRRCompactRangeAndNestedRangeIndexQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1",
        "r1.ID", "/" + name + " r1", null));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2",
        "pos2.id", "/" + coloName + " r2, r2.positions.values pos2", null));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        Portfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    Portfolio[] newPortfolio = createPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForPRAndRRQueryWithCompactAndRangeIndexAndCompareResults(name,
            coloName, localName, coloLocalName));
  }

  @Test
  public void testPRRRIndexQueryWithSameTypeIndexQueryResults() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1",
        "r1.ID", "/" + name + " r1", null));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionWithAsyncIndexCreation(
        coloName, NewPortfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2",
        "r2.id", "/" + coloName + " r2", null));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex3",
        "r1.ID", "/" + localName + " r1", null));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName,
        NewPortfolio.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex4",
        "r2.id", "/" + coloLocalName + " r2", null));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // Let async index updates be finished.
    Wait.pause(5000);

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAndRRQueryAndCompareResults(name,
        coloName, localName, coloLocalName));
  }

  /**
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a LOCAL query on one data store VM and verifies the result.
   */
  @Test
  public void testPRRRNonLocalQueryingWithNoRROnOneNode() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);

    // Creating PR's on the participating VM's
    // Creating DataStore node on the VM0.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, 0, Portfolio.class));
    vm1.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, 0, Portfolio.class));

    // Creating Colocated Region DataStore node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(coloName,
        NewPortfolio.class));

    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(cntDest);

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio, cnt,
        cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(() -> {
      Cache cache = getCache();

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
        fail("Expected RegionNotFoundException wrapped in FunctionException");

      } catch (FunctionException e) {
        if (!(e.getCause() instanceof RegionNotFoundException)) {
          throw e;
        }
      }
    });
  }

  private void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

  public static class TestQueryFunction extends FunctionAdapter implements DataSerializable {

    private String id;

    public TestQueryFunction() {
      // nothing
    }

    public TestQueryFunction(String id) {
      super();
      this.id = id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
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
