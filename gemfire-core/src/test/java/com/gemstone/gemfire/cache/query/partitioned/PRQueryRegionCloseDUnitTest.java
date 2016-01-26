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

/**
 * This test creates partition regions with one Accessor node & two Datastores
 * Calls region.close() on one of the data stores while the query is being
 * executed and recreates the PR on the VM and verifies the results.
 * 
 * @author pbatra
 */

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;

public class PRQueryRegionCloseDUnitTest extends PartitionedRegionDUnitTestCase
{

  /**
   * constructor *
   * 
   * @param name
   */

  public PRQueryRegionCloseDUnitTest(String name) {

    super(name);
  }

  static Properties props = new Properties();

  int totalNumBuckets = 100;

  int threadSleepTime = 500;

  int querySleepTime = 2000;

  int queryTestCycle = 10;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

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
  public void testPRWithRegionCloseInOneDatastoreWithoutDelay()
      throws Exception

  {

    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Querying PR Test with region Close PR operation*****");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    List vmList = new LinkedList();
    vmList.add(vm1);
    vmList.add(vm2);

    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Creating Accessor node on VM0");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy));
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Created Accessor node on VM0");

    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Creating PR's across all VM1 , VM2");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Created PR on VM1 , VM2");

    // creating a local region on one of the JVM's
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Creating Local Region on VM0");
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);

    // Putting the data into the accessor node
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Inserting Portfolio data through the accessor node");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Inserted Portfolio data through the accessor node");

    // Putting the same data in the local region created
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Inserting Portfolio data on local node  VM0 for result Set Comparison");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Successfully Inserted Portfolio data on local node  VM0 for result Set Comparison");

    Random random = new Random();
    AsyncInvocation async0;
    // querying the VM for data
    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Querying on VM0 both on PR Region & local ,also  Comparing the Results sets from both");
    async0 = vm0
        .invokeAsync(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
            name, localName));

    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Calling for Region.close() on either of the Datastores VM1 , VM2 at random and then recreating the cache, with a predefined Delay ");
    for (int j = 0; j < queryTestCycle; j++) {
      int k = (random.nextInt(vmList.size()));
      if( 0 != k ) {
      ((VM)(vmList.get(k))).invoke(PRQHelp.getCacheSerializableRunnableForRegionClose(
          name, redundancy));
      pause(threadSleepTime);
      }
    }
    DistributedTestCase.join(async0, 30 * 1000, getLogWriter());

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
        fail("Unexpected exception during query", async0.getException());
      }
    }

    getLogWriter()
        .info(
            "PRQueryRegionCloseDUnitTest#testPRWithRegionCloseInOneDatastoreWithoutDelay: Querying with PR Operations ENDED*****");
  }

  
  
    
}
