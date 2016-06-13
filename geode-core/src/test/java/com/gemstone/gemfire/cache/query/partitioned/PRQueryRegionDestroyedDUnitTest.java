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

package com.gemstone.gemfire.cache.query.partitioned;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * This test creates partition regions with one Accessor node & two Datastores
 * Calls region.destroy() on one of the data stores while the query is being
 * executed and recreates the PR on the VM and verifies the results.
 * 
 */

import static com.gemstone.gemfire.cache.query.Utils.createPortfolioData;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;

@Category(DistributedTest.class)
public class PRQueryRegionDestroyedDUnitTest extends PartitionedRegionDUnitTestCase
{

  /**
   * constructor *
   * 
   * @param name
   */

  public PRQueryRegionDestroyedDUnitTest() {

    super();
  }
  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }
  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  final String name = "Portfolios";

  final String localName = "LocalPortfolios";

  final int cnt = 0, cntDest = 50;

  final int redundancy = 1;

  /**
   * This test <br>
   * 1. Creates PR regions across with scope = DACK, one accessor node & 2
   * datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Also calls Region.close() randomly on one of the datastore VM's with
   * delay <br>
   * 6. then recreates the PR on the same VM <br>
   * 7. Verfies the size , type , contents of both the resultSets Obtained <br>
   */
  @Test
  public void testPRWithRegionDestroyInOneDatastoreWithDelay()
      throws Exception

  {
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Querying with PR Destroy Region Operation Test Started");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm2,vm3);
    List vmList = new LinkedList();
    vmList.add(vm1);
    vmList.add(vm2);
    vmList.add(vm3);
    
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Creating Accessor node on VM0");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, PortfolioData.class));
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Successfully Created Accessor node on VM0");

    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Creating PR's across all VM1 , VM2, VM3");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    
    
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Successfully Created PR on VM1 , VM2, VM3");

    // creating a local region on one of the JVM's
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Creating Local Region on VM0");
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
 

    // Putting the data into the accessor node
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Inserting Portfolio data through the accessor node");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Successfully Inserted Portfolio data through the accessor node");

    // Putting the same data in the local region created
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Inserting Portfolio data on local node  VM0 for result Set Comparison");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Successfully Inserted Portfolio data on local node  VM0 for result Set Comparison");

    Random random = new Random();
    AsyncInvocation async0;

    // Execute query first time. This is to make sure all the buckets are created 
    // (lazy bucket creation).
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Querying on VM0 First time");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
            name, localName));

    // Now execute the query. And while query execution in process destroy the region 
    // on one of the node.
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Querying on VM0 both on PR Region & local ,also  Comparing the Results sets from both");
    async0 = vm0
        .invokeAsync(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
            name, localName));
    
    Wait.pause(5);
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Calling for Region.destroyRegion() on either of the Datastores VM1 , VM2 at random and then recreating the cache, with a predefined Delay ");
    
      int k = (random.nextInt(vmList.size()));
      
      ((VM)(vmList.get(k))).invoke(PRQHelp.getCacheSerializableRunnableForRegionClose(
          name, redundancy, PortfolioData.class));
    
    
      ThreadUtils.join(async0, 30 * 1000);

    if (async0.exceptionOccurred()) {
      // for Elbe, certain exceptions when a region is destroyed are acceptable
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
    LogWriterUtils.getLogWriter()
        .info(
            "PRQueryRegionDestroyedDUnitTest#testPRWithRegionDestroyInOneDatastoreWithDelay: Querying with PR Destroy Region Operation Test ENDED");
  }

   

}
