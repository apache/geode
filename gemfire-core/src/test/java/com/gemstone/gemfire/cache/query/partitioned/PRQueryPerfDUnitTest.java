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

import java.io.Serializable;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *This tests executes an array of queries to be executed over the PR ,
 * benchmarking them over the time taken by the same when executed over the
 * Local Region 
 * The performance difference is reported for scenarios 
 * encompassing various permutations of the PR attributes
 * like Redundancy / No. of D.S / No. of Accessors etc 
 *
 * @author pbatra
 */

public class PRQueryPerfDUnitTest extends PartitionedRegionDUnitTestCase {
  public static final int SLEEP = 0;

  /**
   * Constructor
   * 
   * @param name
   */

  public PRQueryPerfDUnitTest(String name) {
    super(name);
  }

  int totalNumBuckets = 100;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  final String name = "Portfolios";

  final String localName = "LocalPortfolios";

  final int cnt = 0, cntDest = 5000;

  /**
   * A nuthing test to make DUnit happy.
   * The rest of these tests shouldn't be run 
   * as part of our CruiseControl or precheckin
   * since they don't assert any behavior.
   */
  public void testNuthin() {}
    
  /**
   * This tests executes an array of queries to be executed over the PR ,
   * benchmarking them over the time taken by the same when executed over the
   * Local Region with
   * One Accessor
   * One Datastore
   * Redundancy =0
   *  
   */
  public void norun_testBenchmarkingQueryingOneAccessorOneDS_Redundancy0()
  throws Exception
 {

    LogWriter log = getLogWriter();
    log.info("BenchMarking PR Querying Test Started*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    int redundancy = 0;
    
    try {
      setVMInfoLogLevel();
  
      // Creating Accessor PR's on the participating VM's
      log.info("Creating Accessor node on VM0");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
          redundancy));
      log.info("Successfully Created Accessor node on VM0");
  
      log.info("Creating Datastores across  VM1");
      vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      log.info("Successfully Created Datastores on VM1");
  
      // creating a local region on one of the JVM's
      log.info("Creating Local Region on VM0");
      vm0.invoke(PRQHelp
          .getCacheSerializableRunnableForLocalRegionCreation(localName));
      log.info("Successfully Created Local Region on VM0");
  
      // Generating portfolio object array to be populated across the PR's & Local
      // Regions
  
      final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
  
      // Putting the data into the accessor node
      log.info("Inserting Portfolio data through the accessor node");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
          cnt, cntDest));
      log.info("Successfully Inserted Portfolio data through the accessor node");
  
      // Putting the same data in the local region created
      log
          .info("Inserting Portfolio data on local node  VM0 for result " +
                        "set comparison");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
          portfolio, cnt, cntDest));
      log
          .info("Successfully Inserted Portfolio data on local node  VM0 for" +
                        " result set comparison");
  
      ResultsObject perfR = new ResultsObject();
      perfR.OperationDescription = "PR with 1 Accessor, 1 D.S., Redundancy =0,";
      perfR.NumberOfAccessors = 1;
      perfR.NumberOfDataStores = 1;
      perfR.redundancy = 0;
      
      if (SLEEP > 0) {
        Thread.sleep(SLEEP);
      }
      
      // querying the VM for data
      log.info("Querying on VM0 both on PR Region & local, also comparing the " +
                "Results sets from both");
      vm0.invoke(PRQHelp.PRQueryingVsLocalQuerying(name, localName, perfR));
  
      log.info("Benchmarking Querying between PR & local  ENDED*****");
    }
    finally {
      resetVMLogLevel();
    }
}
  
  
  /**
   * This tests executes an array of queries to be executed over the PR ,
   * benchmarking them over the time taken by the same when executed over the
   * Local Region with
   * One Accessor
   * Two Datastore
   * Redundancy =0
   *  
   */


  public void norun_testBenchmarkingQueryingOneAccessorTwoDS_Redundancy0()
      throws Exception
  {
    LogWriter log = getLogWriter();

    log.info("BenchMarking PR Querying Test Started*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int redundancy = 0;

    try {
      setVMInfoLogLevel();
      
      // Creating Accessor PR's on the participating VM's
      log.info("Creating Accessor node on VM0");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
          redundancy));
      log.info("Successfully Created Accessor node on VM0");
  
      log.info("Creating Datastores across  VM1 , VM2");
      vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      log.info("Successfully Created Datastores on VM1 , VM2");
  
      // creating a local region on one of the JVM's
      log.info("Creating Local Region on VM0");
      vm0.invoke(PRQHelp
          .getCacheSerializableRunnableForLocalRegionCreation(localName));
      log.info("Successfully Created Local Region on VM0");
  
      // Generating portfolio object array to be populated across the PR's & Local
      // Regions
  
      final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
  
      // Putting the data into the accessor node
      log.info("Inserting Portfolio data through the accessor node");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
          cnt, cntDest));
      log.info("Successfully Inserted Portfolio data through the accessor node");
  
      // Putting the same data in the local region created
      log
          .info("Inserting Portfolio data on local node  VM0 for result set comparison");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
          portfolio, cnt, cntDest));
      log.info("Successfully Inserted Portfolio data on local node  VM0 for " +
                "result set comparison");
  
      ResultsObject perfR = new ResultsObject();
      perfR.OperationDescription = "PR with 1 Accessor, 2 D.S., Redundancy=0";
      perfR.NumberOfAccessors = 1;
      perfR.NumberOfDataStores = 2;
      perfR.redundancy = 0;

      if (SLEEP > 0) {
        Thread.sleep(SLEEP);
      }
      
      
      // querying the VM for data
      log.info("Querying on VM0 both on PR Region & local, also comparing the " +
                "results sets from both");
      vm0.invoke(PRQHelp.PRQueryingVsLocalQuerying(name, localName, perfR));
  
      log.info("Benchmarking Querying between PR & local  ENDED*****");
    }
    finally {
      resetVMLogLevel();
    }
  }
  

  /**
   * This tests executes an array of queries to be executed over the PR ,
   * benchmarking them over the time taken by the same when executed over the
   * Local Region with One Accessor Two Datastore  Redundancy =1
   * 
   */

  public void norun_testBenchmarkingQueryingOneAccessorTwoDS_D_ACK_Redundancy1()
      throws Exception
  {
    LogWriter log = getLogWriter();
    log.info("BenchMarking PR Querying Test Started*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int redundancy = 1;

    try {
      setVMInfoLogLevel();

      // Creating Accessor PR's on the participating VM'sw
      log.info("Creating Accessor node on VM0");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
          redundancy));
      log.info("Successfully Created Accessor node on VM0");
  
      log.info("Creating Datastores across  VM1 , VM2");
      vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      log.info("Successfully Created Datastores on VM1 , VM2");
  
      // creating a local region on one of the JVM's
      log.info("Creating Local Region on VM0");
      vm0.invoke(PRQHelp
          .getCacheSerializableRunnableForLocalRegionCreation(localName));
      log.info("Successfully Created Local Region on VM0");
  
      // Generating portfolio object array to be populated across the PR's & Local
      // Regions
  
      final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
  
      // Putting the data into the accessor node
      log.info("Inserting Portfolio data through the accessor node");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
          cnt, cntDest));
      log.info("Successfully Inserted Portfolio data through the accessor node");
  
      // Putting the same data in the local region created
      log.info("Inserting Portfolio data on local node VM0 for result " +
                "set comparison");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
          portfolio, cnt, cntDest));
      log.info("Successfully Inserted Portfolio data on local node VM0 for " +
                "result set comparison");
  
      ResultsObject perfR = new ResultsObject();
      perfR.OperationDescription = "PR with 1 Accessor, 2 D.S., Redundancy=1";
      perfR.NumberOfAccessors = 1;
      perfR.NumberOfDataStores = 2;
      perfR.redundancy = 1;

      if (SLEEP > 0) {
        Thread.sleep(SLEEP);
      }
      
      // querying the VM for data
      log.info("Querying on VM0 both on PR Region & local, also comparing the " +
                "results sets from both");
      vm0.invoke(PRQHelp.PRQueryingVsLocalQuerying(name, localName, perfR));
  
      log.info("Benchmarking Querying between PR & local  ENDED*****");
    }
    finally {
      resetVMLogLevel();
    }
  }

  /**
   * This tests executes an array of queries to be executed over the PR ,
   * benchmarking them over the time taken by the same when executed over the
   * Local Region with One Accessor Three Datastore Redundancy =1
   * 
   */

  public void norun_testBenchmarkingQueryingOneAccessorThreeDS_Redundancy1()
      throws Exception
  {
    LogWriter log = getLogWriter();
    log.info("BenchMarking PR Querying Test Started*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    int redundancy = 1;

    try {
      setVMInfoLogLevel();

      // Creating Accessor PR's on the participating VM's
      log.info("Creating Accessor node on VM0");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
          redundancy));
      log.info("Successfully Created Accessor node on VM0");
  
      log.info("Creating Datastores across  VM1 , VM2 , VM3");
      vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
  
      log.info("Successfully Created Datastores on VM1 , VM2 ,VM3");
  
      // creating a local region on one of the JVM's
      log.info("Creating Local Region on VM0");
      vm0.invoke(PRQHelp
          .getCacheSerializableRunnableForLocalRegionCreation(localName));
      log.info("Successfully Created Local Region on VM0");
  
      // Generating portfolio object array to be populated across the PR's & Local
      // Regions
  
      final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
  
      // Putting the data into the accessor node
      log.info("Inserting Portfolio data through the accessor node");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
          cnt, cntDest));
      log.info("Successfully Inserted Portfolio data through the accessor node");
  
      // Putting the same data in the local region created
      log.info("Inserting Portfolio data on local node  VM0 for result set comparison");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
          portfolio, cnt, cntDest));
      log.info("Successfully Inserted Portfolio data on local node VM0 for " +
                "result set comparison");
  
      ResultsObject perfR = new ResultsObject();
      perfR.OperationDescription = "PR with 1 Accessor, 3 D.S., Redundancy=1";
      perfR.NumberOfAccessors = 1;
      perfR.NumberOfDataStores = 3;
      perfR.redundancy = 1;
      
      if (SLEEP > 0) {
        Thread.sleep(SLEEP);
      }
      
      
      // querying the VM for data
      log.info("Querying on VM0 both on PR Region & local, also comparing the " +
                "results sets from both");
      vm0.invoke(PRQHelp.PRQueryingVsLocalQuerying(name, localName, perfR));
  
      log.info("Benchmarking Querying between PR & local  ENDED*****");
    }
    finally {
      resetVMLogLevel();
    }
  }
  
  
  /**
   * This tests executes an array of queries to be executed over the PR ,
   * benchmarking them over the time taken by the same when executed over the
   * Local Region with One Accessor Three Datastore  Redundancy =2
   * 
   */

  public void norun_testBenchmarkingQueryingOneAccessorThreeDS_Redundancy2()
      throws Exception
  {
    LogWriter log = getLogWriter();
    log.info("BenchMarking PR Querying Test Started*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    int redundancy = 2;

    try {
      setVMInfoLogLevel();

      // Creating Accessor PR's on the participating VM's
      log.info("Creating Accessor node on VM0");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
          redundancy));
      log.info("Successfully Created Accessor node on VM0");
  
      log.info("Creating Datastores across  VM1 , VM2 , VM3");
      vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
      vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy));
  
      log.info("Successfully Created Datastores on VM1 , VM2 , VM3");
  
      // creating a local region on one of the JVM's
      log.info("Creating Local Region on VM0");
      vm0.invoke(PRQHelp
          .getCacheSerializableRunnableForLocalRegionCreation(localName));
      log.info("Successfully Created Local Region on VM0");
  
      // Generating portfolio object array to be populated across the PR's & Local
      // Regions
  
      final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
  
      // Putting the data into the accessor node
      log.info("Inserting Portfolio data through the accessor node");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
          cnt, cntDest));
      log.info("Successfully Inserted Portfolio data through the accessor node");
  
      // Putting the same data in the local region created
      log.info("Inserting Portfolio data on local node  VM0 for result set comparison");
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
          portfolio, cnt, cntDest));
      log.info("Successfully Inserted Portfolio data on local node VM0 for " +
                "result set comparison");
  
      ResultsObject perfR = new ResultsObject();
      perfR.OperationDescription = "PR with 1 Accessor, 3 D.S., Redundancy=2";
      perfR.NumberOfAccessors = 1;
      perfR.NumberOfDataStores = 3;
      perfR.redundancy = 2;
      
      if (SLEEP > 0) {
        Thread.sleep(SLEEP);
      }
      
      
      // querying the VM for data
      log.info("Querying on VM0 both on PR Region & local, also comparing the " +
                "results sets from both");
      vm0.invoke(PRQHelp.PRQueryingVsLocalQuerying(name, localName, perfR));
  
      log.info("Benchmarking Querying between PR & local  ENDED*****");
    }
    finally {
      resetVMLogLevel();
    }
  }
  
  /*
   * Inner class to for the ResultObject , displaying various attributes of the
   * Performance Report
   */
  class ResultsObject implements Serializable {
    String OperationDescription;
    long QueryingTimeLocal;
    long QueryingTimePR;
    int NumberOfDataStores = 0;
    int NumberOfAccessors = 0;
    int redundancy = 0;
  }

}
