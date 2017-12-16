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

import static org.apache.geode.cache.query.Utils.createPortfolioData;
import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegionDUnitTestCase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class PRQueryRegionCloseDUnitTest extends PartitionedRegionDUnitTestCase {

  public PRQueryRegionCloseDUnitTest() {
    super();
  }

  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }


  int threadSleepTime = 500;

  int queryTestCycle = 10;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  final String name = "Portfolios";

  final String localName = "LocalPortfolios";

  final int cnt = 0, cntDest = 50;

  final int redundancy = 1;

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache.query.data.**");
    return properties;
  }


  /**
   * This test <br>
   * 1. Creates PR regions across with scope = DACK, one accessor node & 2 datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Also calls Region.close() randomly on one of the datastore VM's with delay <br>
   * 6. then recreates the PR on the same VM <br>
   * 7. Verfies the size , type , contents of both the resultSets Obtained <br>
   */
  @Category(FlakyTest.class) // GEODE-1720
  @Test
  public void testPRWithRegionCloseInOneDatastoreWithoutDelay() throws Exception

  {

    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Querying PR Test with region Close PR operation*****");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    setCacheInVMs(vm0, vm1, vm2);
    List vmList = new LinkedList();
    vmList.add(vm1);
    vmList.add(vm2);

    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Creating Accessor node on VM0");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Created Accessor node on VM0");

    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Creating PR's across all VM1 , VM2");
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Created PR on VM1 , VM2");

    // creating a local region on one of the JVM's
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Creating Local Region on VM0");
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the accessor node
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Inserting Portfolio data through the accessor node");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Inserted Portfolio data through the accessor node");

    // Putting the same data in the local region created
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Inserting Portfolio data on local node  VM0 for result Set Comparison");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Inserted Portfolio data on local node  VM0 for result Set Comparison");

    Random random = new Random();
    AsyncInvocation async0;
    // querying the VM for data
    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Querying on VM0 both on PR Region & local ,also  Comparing the Results sets from both");
    async0 = vm0.invokeAsync(
        PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName));

    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Calling for Region.close() on either of the Datastores VM1 , VM2 at random and then recreating the cache, with a predefined Delay ");
    for (int j = 0; j < queryTestCycle; j++) {
      int k = (random.nextInt(vmList.size()));
      if (0 != k) {
        ((VM) (vmList.get(k))).invoke(PRQHelp.getCacheSerializableRunnableForRegionClose(name,
            redundancy, PortfolioData.class));
        Wait.pause(threadSleepTime);
      }
    }
    ThreadUtils.join(async0, 30 * 1000);

    if (async0.exceptionOccurred()) {
      // for now, certain exceptions when a region is closed are acceptable
      // including ForceReattemptException (e.g. resulting from RegionDestroyed)
      boolean isForceReattempt = false;
      Throwable t = async0.getException();
      do {
        if (t instanceof ForceReattemptException) {
          isForceReattempt = true;
          break;
        }
        t = t.getCause();
      } while (t != null);

      if (!isForceReattempt) {
        Assert.fail("Unexpected exception during query", async0.getException());
      }
    }

    LogWriterUtils.getLogWriter().info(
        "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Querying with PR Operations ENDED*****");
  }



}
