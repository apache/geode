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

package org.apache.geode.cache.query.partitioned;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This tests creates partition regions across VM's executes Queries on PR's so
 * that they generate various Exceptions
 */

import static org.apache.geode.cache.query.Utils.createPortfolioData;

import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.internal.cache.PartitionedRegionDUnitTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;

@Category(DistributedTest.class)
public class PRInvalidQueryDUnitTest extends PartitionedRegionDUnitTestCase

{
  /**
   * constructor *
   * 
   * @param name
   */

  public PRInvalidQueryDUnitTest() {

    super();
  }

  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

  int totalNumBuckets = 100;

  PRQueryDUnitHelper prq = new PRQueryDUnitHelper();

  final String name = "Portfolios";

  final int i = 0, step = 20, cnt=0,cntDest = 90;

  final int redundancy = 0;

  /**
   * This test <pr> 1. Creates PR regions across with scope = DACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the data in PR region & the Local Region <br>
   * 4. Queries the PR qith an Invalid Query Syntax <br>
   * 5. Verfies that there is an QueryInavalidException
   * 
   * @throws Exception
   */

  @Test
  public void testPRDAckCreationAndQueryingWithInvalidQuery() throws Exception
  {
    LogWriterUtils.getLogWriter()
        .info(
            "PRInvalidQueryDUnitTest#testPRDAckCreationAndQueryingWithInvalidQuery: Querying PR Test with Expected InvalidQueryException*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);
    // Creting PR's on the participating VM's

    // Creating Accessor node on the VM
    LogWriterUtils.getLogWriter()
        .info(
            "PRInvalidQueryDUnitTest#testPRDAckCreationAndQueryingWithInvalidQuery: Creating the Accessor node in the PR");
    vm0.invoke(prq.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, PortfolioData.class));
    LogWriterUtils.getLogWriter()
        .info(
            "PRInvalidQueryDUnitTest#testPRDAckCreationAndQueryingWithInvalidQuery: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM's
    LogWriterUtils.getLogWriter()
        .info(
            "PRInvalidQueryDUnitTest#testPRDAckCreationAndQueryingWithInvalidQuery: Creating the Datastore node in the PR");
    vm1.invoke(prq.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(prq.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(prq.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    LogWriterUtils.getLogWriter()
        .info(
            "PRInvalidQueryDUnitTest#testPRDAckCreationAndQueryingWithInvalidQuery: Successfully Created the Datastore node in the PR");

    LogWriterUtils.getLogWriter()
        .info(
            "PRInvalidQueryDUnitTest#testPRDAckCreationAndQueryingWithInvalidQuery: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(prq.getCacheSerializableRunnableForPRPuts(name, portfolio, i, i
        + step));
    vm1.invoke(prq.getCacheSerializableRunnableForPRPuts(name, portfolio, i
        + step, i + (2 * step)));
    vm2.invoke(prq.getCacheSerializableRunnableForPRPuts(name, portfolio, i
        + (2 * step), i + (3 * step)));
    vm3.invoke(prq.getCacheSerializableRunnableForPRPuts(name, portfolio, i
        + (3 * step), cntDest));
    LogWriterUtils.getLogWriter()
        .info(
            "PRInvalidQueryDUnitTest#testPRDAckCreationAndQueryingWithInvalidQuery: Successfully Inserted Portfolio data across PR's");

    final String invalidQuery = "Invalid Query";
    // querying the VM for data
    vm0.invoke(prq.getCacheSerializableRunnableForPRInvalidQuery(name
    ));
    LogWriterUtils.getLogWriter()
        .info(
            "PRInvalidQueryDUnitTest#testPRDAckCreationAndQueryingWithInvalidQuery: *****Querying PR's Test with Expected Invalid Query Exception *****");
  }
}
