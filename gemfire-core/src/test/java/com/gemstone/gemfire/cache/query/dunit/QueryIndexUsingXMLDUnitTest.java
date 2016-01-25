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
package com.gemstone.gemfire.cache.query.dunit;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheExistsException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.functional.StructSetOrResultsSet;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;
import com.gemstone.gemfire.util.test.TestUtil;

public class QueryIndexUsingXMLDUnitTest extends CacheTestCase {

  static private final String WAIT_PROPERTY = "QueryIndexBuckets.maxWaitTime";

  static private final int WAIT_DEFAULT = (60 * 1000);
  
  public static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, 
      WAIT_DEFAULT).intValue();

  final String name = "PartionedPortfolios";
  final String repRegName = "Portfolios";
  final String persistentRegName = "PersistentPrPortfolios";
  final String nameWithRange = "PartitionedPortfoliosWithRange";
  final String nameWithHash = "PartionedPortfoliosWithHash";
  final String repRegNameWithRange = "PortfoliosWithRange";
  final String repRegNameWithHash = "PortfoliosWithHash";
  final String persistentRegNameWithRange = "PersistentPrPortfoliosWithRange";
  final String persistentRegNameWithHash = "PersistentPrPortfoliosWithHash";
  final String noIndexRepReg = "PortfoliosNoIndex";
  final String statusIndex = "statusIndex";
  final String idIndex = "idIndex";
  
  String queryStr[][] = new String[][]{
      { "Select * from /" + name + " where ID > 10", 
        "Select * from /" + repRegName + " where ID > 10", 
        "Select * from /" + persistentRegName + " where ID > 10",        
      }, 
      { "Select * from /" + name + " where ID = 5", 
        "Select * from /" + repRegName + " where ID = 5", 
        "Select * from /" + persistentRegName + " where ID = 5",
        "Select * from /" + nameWithHash + " where ID = 5", 
        "Select * from /" + repRegNameWithHash + " where ID = 5", 
        "Select * from /" + persistentRegNameWithHash + " where ID = 5"
      }, 
      {"Select * from /" + name + " where status = 'active'", 
        "Select * from /" + repRegName + " where status = 'active'", 
        "Select * from /" + persistentRegName + " where status = 'active'", 
        "Select * from /" + nameWithHash + " where status = 'active'", 
        "Select * from /" + repRegNameWithHash + " where status = 'active'", 
        "Select * from /" + persistentRegNameWithHash + " where status = 'active'",
      },
  };

  String queryStrNoIndex[] = new String[]{ 
      "Select * from /" + noIndexRepReg + " where ID > 10", 
      "Select * from /" + noIndexRepReg + " where ID = 5", 
      "Select * from /" + noIndexRepReg + " where status = 'active'", 
    };
  
  String queryStrValid = "Select * from /" + noIndexRepReg + " where ID > 10";

  private String persistentOverFlowRegName = "PersistentOverflowPortfolios";

  /** Creates a new instance of QueryIndexUsingXMLDUnitTest */
  public QueryIndexUsingXMLDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    //Workaround for #52008
    addExpectedException("Failed to create index");
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    // Get the disk store name.
    GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
    String diskStoreName = cache.getDefaultDiskStoreName();
    
    //reset TestHook
    invokeInEveryVM(resetTestHook());
    // close the cache.
    closeCache();
    disconnectFromDS();
    
    // remove the disk store.
    File diskDir = new File(diskStoreName).getAbsoluteFile();
    com.gemstone.gemfire.internal.FileUtil.delete(diskDir);
  }
  
  /**
   * Creates partitioned index from an xml description.
   */
  public void testCreateIndexThroughXML() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    AsyncInvocation asyInvk0 = vm0.invokeAsync(createIndexThrougXML("vm0testCreateIndexThroughXML", name, fileName));
    
    AsyncInvocation asyInvk1 = vm1.invokeAsync(createIndexThrougXML("vm1testCreateIndexThroughXML", name, fileName));
    
    DistributedTestCase.join(asyInvk1, 30 * 1000, getLogWriter());
    if (asyInvk1.exceptionOccurred()) {
      fail("asyInvk1 failed", asyInvk1.getException());
    }
    DistributedTestCase.join(asyInvk0, 30 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }

    // Check index for PR
    vm0.invoke(prIndexCreationCheck(name, statusIndex, -1));
    vm1.invoke(prIndexCreationCheck(name, statusIndex, -1));
    vm0.invoke(prIndexCreationCheck(name, idIndex, -1));
    vm1.invoke(prIndexCreationCheck(name, idIndex, -1));
    vm0.invoke(prIndexCreationCheck(name, "secIndex", -1));
    vm1.invoke(prIndexCreationCheck(name, "secIndex", -1));

    // Check index for replicated
    vm0.invoke(indexCreationCheck(repRegName, statusIndex));
    vm1.invoke(indexCreationCheck(repRegName, statusIndex));

    // Check index for persistent pr region
    vm0.invoke(prIndexCreationCheck(persistentRegName, statusIndex, -1));
    vm1.invoke(prIndexCreationCheck(persistentRegName, statusIndex, -1));
    
    //check range index creation
    vm0.invoke(prIndexCreationCheck(nameWithRange, statusIndex, -1));
    vm1.invoke(prIndexCreationCheck(nameWithRange, statusIndex, -1));
    vm0.invoke(prIndexCreationCheck(nameWithRange, idIndex, -1));
    vm1.invoke(prIndexCreationCheck(nameWithRange, idIndex, -1));
    vm0.invoke(indexCreationCheck(repRegNameWithRange, statusIndex));
    vm1.invoke(indexCreationCheck(repRegNameWithRange, statusIndex));
    vm0.invoke(prIndexCreationCheck(persistentRegNameWithRange, statusIndex, -1));
    vm1.invoke(prIndexCreationCheck(persistentRegNameWithRange, statusIndex, -1));
    
    //check hash index creation
    vm0.invoke(prIndexCreationCheck(nameWithHash, statusIndex, -1));
    vm1.invoke(prIndexCreationCheck(nameWithHash, statusIndex, -1));
    vm0.invoke(prIndexCreationCheck(nameWithHash, idIndex, -1));
    vm1.invoke(prIndexCreationCheck(nameWithHash, idIndex, -1));
    vm0.invoke(indexCreationCheck(repRegNameWithHash, statusIndex));
    vm1.invoke(indexCreationCheck(repRegNameWithHash, statusIndex));
    vm0.invoke(prIndexCreationCheck(persistentRegNameWithHash, statusIndex, -1));
    vm1.invoke(prIndexCreationCheck(persistentRegNameWithHash, statusIndex, -1));
    
    vm0.invoke(close());
    vm1.invoke(close());
  }

  /**
   * Creates partitioned index from an xml description.
   */
  public void testCreateIndexWhileDoingGII() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    vm0.invoke(createIndexThrougXML("vm0testCreateIndexWhileDoingGII", name, fileName));
    // LoadRegion
    vm0.invoke(loadRegion(name));
    vm0.invoke(loadRegion(nameWithHash));
    vm0.invoke(loadRegion(nameWithRange));
    vm0.invoke(prIndexCreationCheck(name, statusIndex, -1));
    vm0.invoke(prIndexCreationCheck(nameWithHash, statusIndex, -1));
    vm0.invoke(prIndexCreationCheck(nameWithRange, statusIndex, -1));
    
    vm1.invoke(setTestHook());    
    vm1.invoke(createIndexThrougXML("vm1testCreateIndexWhileDoingGII", name, fileName));
    
    vm0.invoke(prIndexCreationCheck(name, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(name, statusIndex, 50));
    vm0.invoke(prIndexCreationCheck(name, idIndex, 50));
    vm1.invoke(prIndexCreationCheck(name, idIndex, 50));
    vm0.invoke(prIndexCreationCheck(name, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(name, "secIndex", 50));
    
    //check range index creation
    vm0.invoke(prIndexCreationCheck(nameWithRange, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(nameWithRange, statusIndex, 50));
    vm0.invoke(prIndexCreationCheck(nameWithRange, idIndex, 50));
    vm1.invoke(prIndexCreationCheck(nameWithRange, idIndex, 50));
    
    //check hash index creation
    vm0.invoke(prIndexCreationCheck(nameWithHash, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(nameWithHash, statusIndex, 50));
    vm0.invoke(prIndexCreationCheck(nameWithHash, idIndex, 50));
    vm1.invoke(prIndexCreationCheck(nameWithHash, idIndex, 50));
    
    // Execute query and verify index usage    
    vm0.invoke(executeQuery(name));
    vm1.invoke(executeQuery(name));

    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }

 
  /**
   * Creates partitioned index from an xml description.
   */
  public void testReplicatedRegionCreateIndexWhileDoingGII() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    vm0.invoke(createIndexThrougXML("vm0testRRegionCreateIndexWhileDoingGII", repRegName, fileName));
    // LoadRegion
    vm0.invoke(loadRegion(repRegName));
    vm0.invoke(loadRegion(repRegNameWithHash));
    vm0.invoke(loadRegion(noIndexRepReg));
    vm0.invoke(indexCreationCheck(repRegName, statusIndex));
    vm0.invoke(indexCreationCheck(repRegNameWithHash, statusIndex));
    
    vm1.invoke(setTestHook());    
    vm1.invoke(createIndexThrougXML("vm1testRRegionCreateIndexWhileDoingGII", repRegName, fileName));
    
    vm0.invoke(indexCreationCheck(repRegName, statusIndex));
    vm1.invoke(indexCreationCheck(repRegName, statusIndex));
    vm0.invoke(indexCreationCheck(repRegName, idIndex));
    vm1.invoke(indexCreationCheck(repRegName, idIndex));
    vm0.invoke(indexCreationCheck(repRegName, "secIndex"));
    vm1.invoke(indexCreationCheck(repRegName, "secIndex"));
    
    //check hash index creation
    vm0.invoke(indexCreationCheck(repRegNameWithHash, statusIndex));
    vm1.invoke(indexCreationCheck(repRegNameWithHash, statusIndex));
    vm0.invoke(indexCreationCheck(repRegNameWithHash, idIndex));
    vm1.invoke(indexCreationCheck(repRegNameWithHash, idIndex));
    vm0.invoke(indexCreationCheck(repRegNameWithHash, "secIndex"));
    vm1.invoke(indexCreationCheck(repRegNameWithHash, "secIndex"));
    
    // Execute query and verify index usage    
    vm0.invoke(executeQuery(repRegName));
    vm1.invoke(executeQuery(repRegName));
    
    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }
  /**
   * Creates persistent partitioned index from an xml description.
   */
  public void testPersistentPRRegionCreateIndexWhileDoingGII() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    vm0.invoke(createIndexThrougXML("vm0testPersistentPRRegion", persistentRegName, fileName));
    // LoadRegion
    vm0.invoke(loadRegion(this.persistentRegName));
    vm0.invoke(loadRegion(noIndexRepReg));
    vm0.invoke(loadRegion(persistentRegNameWithHash));
    vm0.invoke(prIndexCreationCheck(persistentRegName, statusIndex, -1));
    vm0.invoke(prIndexCreationCheck(persistentRegNameWithHash, statusIndex, -1));
    
    vm1.invoke(setTestHook());    
    vm1.invoke(createIndexThrougXML("vm1testPersistentPRRegion", persistentRegName, fileName));
    
    vm0.invoke(prIndexCreationCheck(persistentRegName, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(persistentRegName, statusIndex, 50));
    vm0.invoke(prIndexCreationCheck(persistentRegName, idIndex, 50));
    vm1.invoke(prIndexCreationCheck(persistentRegName, idIndex, 50));
    vm0.invoke(prIndexCreationCheck(persistentRegName, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(persistentRegName, "secIndex", 50));
    
  //check hash index creation
    vm0.invoke(prIndexCreationCheck(persistentRegNameWithHash, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(persistentRegNameWithHash, statusIndex, 50));
    vm0.invoke(prIndexCreationCheck(persistentRegNameWithHash, idIndex, 50));
    vm1.invoke(prIndexCreationCheck(persistentRegNameWithHash, idIndex, 50));
    vm0.invoke(prIndexCreationCheck(persistentRegNameWithHash, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(persistentRegNameWithHash, "secIndex", 50));
    
    // Execute query and verify index usage    
    vm0.invoke(executeQuery(persistentRegName));
    vm1.invoke(executeQuery(persistentRegName));
    
    // close one vm cache
    vm1.invoke(resetTestHook());
    vm1.invoke(new SerializableRunnable() {
      
      @Override
      public void run() {
        closeCache();
      }
    });
    
    // restart
    vm1.invoke(setTestHook());    
    vm1.invoke(createIndexThrougXML("vm1testPersistentPRRegion", persistentRegName, fileName));
    vm1.invoke(prIndexCreationCheck(persistentRegName, statusIndex, 50));

    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }


  /**
   * Creates partitioned index from an xml description.
   */
  public void testCreateIndexWhileDoingGIIWithEmptyPRRegion() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info("### in testCreateIndexWhileDoingGIIWithEmptyPRRegion.");
    

    vm0.invoke(createIndexThrougXML("vm0testGIIWithEmptyPRRegion", name, fileName));
    vm0.invoke(prIndexCreationCheck(name, statusIndex, -1));
    vm0.invoke(prIndexCreationCheck(nameWithHash, statusIndex, -1));
    
    vm1.invoke(setTestHook());    
    vm1.invoke(createIndexThrougXML("vm1testGIIWithEmptyPRRegion", name, fileName));
    vm1.invoke(prIndexCreationCheck(name, statusIndex, -1));
    vm1.invoke(prIndexCreationCheck(nameWithHash, statusIndex, -1));

    // LoadRegion
    vm0.invoke(loadRegion(name));
    vm0.invoke(loadRegion(nameWithHash));

    vm0.invoke(prIndexCreationCheck(name, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(name, statusIndex, 50));
    vm0.invoke(prIndexCreationCheck(nameWithHash, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(nameWithHash, statusIndex, 50));
    
    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }

  /**
   * Creats partitioned index from an xml discription.
   */
  public void testCreateAsyncIndexWhileDoingGII() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    AsyncInvocation asyInvk0 = vm0.invokeAsync(createIndexThrougXML("vm0testAsyncIndexWhileDoingGII", name, fileName));
    
    DistributedTestCase.join(asyInvk0, 30 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    // LoadRegion
    asyInvk0 = vm0.invokeAsync(loadRegion(name));
    
    vm1.invoke(setTestHook());    
    AsyncInvocation asyInvk1 = vm1.invokeAsync(createIndexThrougXML("vm1testAsyncIndexWhileDoingGII", name, fileName));
    
    vm0.invoke(prIndexCreationCheck(name, statusIndex, 50));

    DistributedTestCase.join(asyInvk1, 30 * 1000, getLogWriter());
    if (asyInvk1.exceptionOccurred()) {
      fail("asyInvk1 failed", asyInvk1.getException());
    }
    
    vm1.invoke(prIndexCreationCheck(name, statusIndex, 50));

    DistributedTestCase.join(asyInvk0, 30 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }

  /**
   * Creates indexes and compares the results between index and non-index results.
   */
  public void testCreateIndexWhileDoingGIIAndCompareQueryResults() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    vm0.invoke(createIndexThrougXML("vm0testIndexCompareQResults", name, fileName));
    // LoadRegion
    vm0.invoke(loadRegion(name));
    vm0.invoke(loadRegion(repRegName));
    vm0.invoke(loadRegion(persistentRegName));
    vm0.invoke(loadRegion(noIndexRepReg));
    vm0.invoke(loadRegion(nameWithHash));
    vm0.invoke(loadRegion(repRegNameWithHash));
    vm0.invoke(loadRegion(persistentRegNameWithHash));
    vm0.invoke(prIndexCreationCheck(name, statusIndex, -1));
    
    vm1.invoke(setTestHook());    
    vm1.invoke(createIndexThrougXML("vm1testIndexCompareQResults", name, fileName));
    
    vm0.invoke(prIndexCreationCheck(name, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(name, statusIndex, 50));
    vm0.invoke(prIndexCreationCheck(name, idIndex, 50));
    vm1.invoke(prIndexCreationCheck(name, idIndex, 50));
    vm0.invoke(prIndexCreationCheck(name, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(name, "secIndex", 50));
    
    vm0.invoke(prIndexCreationCheck(nameWithHash, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(nameWithHash, statusIndex, 50));
    vm0.invoke(prIndexCreationCheck(nameWithHash, idIndex, 50));
    vm1.invoke(prIndexCreationCheck(nameWithHash, idIndex, 50));
    vm0.invoke(prIndexCreationCheck(nameWithHash, "secIndex", 50));
    vm1.invoke(prIndexCreationCheck(nameWithHash, "secIndex", 50));

    vm0.invoke(prIndexCreationCheck(persistentRegName, "secIndex", 50));
    vm0.invoke(indexCreationCheck(repRegName, "secIndex"));
    vm0.invoke(prIndexCreationCheck(persistentRegNameWithHash, "secIndex", 50));
    vm0.invoke(indexCreationCheck(repRegNameWithHash, "secIndex"));

    vm1.invoke(prIndexCreationCheck(persistentRegName, "secIndex", 50));
    vm1.invoke(indexCreationCheck(repRegName, "secIndex"));
    vm1.invoke(prIndexCreationCheck(persistentRegNameWithHash, "secIndex", 50));
    vm1.invoke(indexCreationCheck(repRegNameWithHash, "secIndex"));
   
   
 
    // Execute query and verify index usage    
    vm0.invoke(executeQueryAndCompareResult(name, true));
    vm1.invoke(executeQueryAndCompareResult(name, true));
    
    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }
  
  /**
   * Creates async partitioned index from an xml description.
   */
  public void testCreateAsyncIndexWhileDoingGIIAndQuery() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    AsyncInvocation asyInvk0 = vm0.invokeAsync(createIndexThrougXML("vm0testCreateAsyncIndexGIIAndQuery", name, fileName));
    DistributedTestCase.join(asyInvk0, 30 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    // LoadRegion
    asyInvk0 = vm0.invokeAsync(loadRegion(name));
    
    vm1.invoke(setTestHook());    
    AsyncInvocation asyInvk1 = vm1.invokeAsync(createIndexThrougXML("vm1testCreateAsyncIndexGIIAndQuery", name, fileName));
 
    
    DistributedTestCase.join(asyInvk1, 30 * 1000, getLogWriter());  
    if (asyInvk1.exceptionOccurred()) {
      fail("asyInvk1 failed", asyInvk1.getException());
    }
    DistributedTestCase.join(asyInvk0, 30 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }
 
    vm0.invoke(prIndexCreationCheck(name, statusIndex, 50));
    vm1.invoke(prIndexCreationCheck(name, statusIndex, 50));
    
    // Execute query and verify index usage    
    vm0.invoke(executeQuery(name));
    vm1.invoke(executeQuery(name));
    
    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }
  
  /**
   * Creates asynch indexes and compares the results between index and non-index results.
   * <p>
   * DISABLED.  This test is disabled due to a high rate of failure.  See ticket #52167
   */
  public void disabled_testCreateAsyncIndexWhileDoingGIIAndCompareQueryResults() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    vm0.invoke(createIndexThrougXML("vm0testAsyncIndexAndCompareQResults", name, fileName));
    // LoadRegion
    vm0.invoke(loadRegion(name));
    vm0.invoke(loadRegion(repRegName));
    vm0.invoke(loadRegion(persistentRegName));
    vm0.invoke(loadRegion(noIndexRepReg));
    
    // Start async update
    vm0.invokeAsync(loadRegion(name, 500));
    vm0.invokeAsync(loadRegion(repRegName, 500));
    AsyncInvocation asyInvk0 = vm0.invokeAsync(loadRegion(persistentRegName, 500));
    vm0.invokeAsync(loadRegion(noIndexRepReg, 500));
    
    vm1.invoke(setTestHook());    
    vm1.invoke(createIndexThrougXML("vm1testAsyncIndexAndCompareQResults", name, fileName));
    
    DistributedTestCase.join(asyInvk0, 30 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }
     
    vm1.invoke(prIndexCreationCheck(persistentRegName, "secIndex", 50));
    vm1.invoke(indexCreationCheck(repRegName, "secIndex"));

    // Execute query and verify index usage    
    vm0.invoke(executeQueryAndCompareResult(name, false));
    vm1.invoke(executeQueryAndCompareResult(name, false));
    
    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }
  
  public void testIndexCreationForReplicatedPersistentOverFlowRegionOnRestart() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    //create index using xml
    vm0.invoke(createIndexThrougXML("vm0testIndexCreationForReplicatedPersistentOverFlowRegionOnRestart", persistentOverFlowRegName, fileName));
    //verify index creation
    vm0.invoke(indexCreationCheck(persistentOverFlowRegName, statusIndex));
    //LoadRegion
    vm0.invoke(loadRegion(persistentOverFlowRegName));
    //close cache without deleting diskstore
    vm0.invoke(closeWithoutDeletingDiskStore());
    //start cache by recovering data from diskstore
    vm0.invoke(createIndexThrougXML("vm0testIndexCreationForReplicatedPersistentOverFlowRegionOnRestart", persistentOverFlowRegName, fileName));
    //verify index creation on restart
    vm0.invoke(indexCreationCheck(persistentOverFlowRegName, statusIndex));
    //close cache and delete diskstore 
    vm0.invoke(close());
    
  }
  
  
  public CacheSerializableRunnable setTestHook()
  {
    SerializableRunnable sr = new CacheSerializableRunnable("TestHook") {
      public void run2()
      {
        class IndexTestHook implements IndexManager.TestHook {
          public boolean indexCreatedAsPartOfGII;
          
          public void hook(int spot) throws RuntimeException {
            GemFireCacheImpl.getInstance().getLogger().fine(
                "In IndexTestHook.hook(). hook() argument value is : " + spot);
            if (spot == 1) {
              throw new RuntimeException("Index is not created as part of Region GII.");
            }
          }
        };
        /*
        try {
          ClassLoader.getSystemClassLoader().loadClass(IndexManager.class.getName());
        } catch (Exception ex) {
          fail("Failed to load IndexManager.class");
        }
        */
        IndexManager.testHook = new IndexTestHook();
      }
    };
    return (CacheSerializableRunnable)sr;
  }


  public CacheSerializableRunnable resetTestHook()
  {
    SerializableRunnable sr = new CacheSerializableRunnable("TestHook") {
      public void run2()
      {
        IndexManager.testHook = null;
      }
    };
    return (CacheSerializableRunnable)sr;
  }
  public CacheSerializableRunnable createIndexThrougXML(final String vmid,
      final String regionName, final String xmlFileName)
  {
    SerializableRunnable sr = new CacheSerializableRunnable("RegionCreator") {
      public void run2()
      {
        try {
          //closeCache();
          File file = findFile(xmlFileName);
          GemFireCacheImpl.testCacheXml = file;
          //DistributedTestCase.diskStore = vmid;
          getSystem();
          Cache cache = getCache();
          Region region = cache.getRegion(regionName);
          if (region == null){
            fail("Region not found." + regionName);
          }
        }
        finally {
          GemFireCacheImpl.testCacheXml = null;
          //DistributedTestCase.diskStore = null;
        }   
      }
    };
    return (CacheSerializableRunnable)sr;
  }
  
  public CacheSerializableRunnable prIndexCreationCheck(
      final String regionName, final String indexName, final int bucketCount)
  {
    CacheSerializableRunnable sr = new CacheSerializableRunnable("pr IndexCreationCheck" + 
        regionName + " indexName :" + indexName) {
      public void run2()
      {
        //closeCache();
        Cache cache = getCache();
        LogWriter logger = cache.getLogger();
        PartitionedRegion region = (PartitionedRegion)cache.getRegion(regionName);
        Map indexMap = region.getIndex();
        PartitionedIndex index = (PartitionedIndex)region.getIndex().get(indexName);
        if (index == null) {
          fail("Index " + indexName + " Not Found for region " + regionName);
        }
        logger.info("Current number of buckets indexed : " + ""
            + ((PartitionedIndex)index).getNumberOfIndexedBuckets());        
        if (bucketCount >= 0) {
          waitForIndexedBuckets((PartitionedIndex)index, bucketCount);
        }
        if (!index.isPopulated()) {
          fail("Index isPopulatedFlag is not set to true");
        }
      }
    };
    return sr;
  }

  public CacheSerializableRunnable indexCreationCheck(
      final String regionName, final String indexName)
  {
    CacheSerializableRunnable sr = new CacheSerializableRunnable("IndexCreationCheck region: " + 
        regionName + " indexName :" + indexName) {
      public void run2()
      {
        //closeCache();
        Cache cache = getCache();
        LogWriter logger = cache.getLogger();
        LocalRegion region = (LocalRegion)cache.getRegion(regionName);
        Index index = region.getIndexManager().getIndex(indexName);
        if (index == null) {
          fail("Index " + indexName + " Not Found for region name:" + regionName);
        }
      }
    };
    return sr;
  }

  public boolean waitForIndexedBuckets(final PartitionedIndex index, final int bucketCount) {
    
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (index.getNumberOfIndexedBuckets() >= bucketCount);
      }
      public String description() {
        return "Number of Indexed Bucket is less than the expected number. "+ bucketCount + ", " + index.getNumberOfIndexedBuckets();
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public CacheSerializableRunnable loadRegion(
      final String name)
  {
    CacheSerializableRunnable sr = new CacheSerializableRunnable("load region on " + name) {
      public void run2()
      {
        Cache cache = getCache();
        LogWriter logger = cache.getLogger();
        Region region = cache.getRegion(name);
        for (int i=0; i < 100; i++){
          region.put("" +i, new Portfolio(i));
        }        
      }
    };
    return sr;
  }

  public CacheSerializableRunnable loadRegion(
      final String name, final int size)
  {
    CacheSerializableRunnable sr = new CacheSerializableRunnable("LoadRegion: " + name + " size :" + size) {
      public void run2()
      {
        Cache cache = getCache();
        LogWriter logger = cache.getLogger();
        Region region = cache.getRegion(name);
        for (int i=0; i < size; i++){
          region.put("" +i, new Portfolio(i));
        }        
      }
    };
    return sr;
  }

  public CacheSerializableRunnable executeQuery(final String rname)
  {
    CacheSerializableRunnable sr = new CacheSerializableRunnable("execute query on " + rname) {
      public void run2()
      {
        QueryService qs = getCache().getQueryService();
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        String queryStr = "Select * from /" + rname + " where ID > 10";
        Query query = qs.newQuery(queryStr);
        try {
          query.execute();
        } catch (Exception ex) {
          fail("Failed to execute the query.");
        }
        if(!observer.isIndexesUsed) {
          fail("Index not used for query. " + queryStr);
        }
      }
    };
    return sr;
  }

  public CacheSerializableRunnable executeQueryAndCompareResult(final String rname, final boolean compareHash)
  {
    CacheSerializableRunnable sr = new CacheSerializableRunnable("execute query and compare results.") {
      public void run2()
      {
        QueryService qs = getCache().getQueryService();
        
        StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];
        String s[] = new String[2];
        for (int j=0; j < queryStr.length; j++) {  
          String[] queryArray = queryStr[j];
          int numQueriesToCheck = compareHash ? queryArray.length: 3;
          for (int i=0; i < numQueriesToCheck; i++) {
            QueryObserverImpl observer = new QueryObserverImpl();
            QueryObserverHolder.setInstance(observer);
            // Query using index.
            s[0] = queryStr[j][i];
            // Execute query with index.
            Query query = qs.newQuery(s[0]);
            
            try {
              sr[0][0] = (SelectResults)query.execute();
            } catch (Exception ex) {
              fail("Failed to execute the query.");
            }
            if(!observer.isIndexesUsed) {
              fail("Index not used for query. " + s[0]);
            }
            
            // Query using no index.
            s[1] = queryStrNoIndex[j];
            try {
              query = qs.newQuery(s[1]);
              sr[0][1] = (SelectResults)query.execute();
            } catch (Exception ex) {
              fail("Failed to execute the query on no index region.");
            }

            // compare.
            getLogWriter().info("Execute query : \n queryStr with index: " + s[0]  + " \n queryStr without index: " + s[1]);
            ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1, s);
          }
        }
      }
    };
    return sr;
  }

  public CacheSerializableRunnable closeWithoutDeletingDiskStore()
  {
    CacheSerializableRunnable sr = new CacheSerializableRunnable("close") {
      public void run2()
      {
        IndexManager.testHook = null;
        // close the cache.
        closeCache();
        disconnectFromDS();
      }
    };
    return sr;
  }
  
  public CacheSerializableRunnable close()
  {
    CacheSerializableRunnable sr = new CacheSerializableRunnable("close") {
      public void run2()
      {
        IndexManager.testHook = null;
        
        // Get the disk store name.
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        String diskStoreName = cache.getDefaultDiskStoreName();
        
        // close the cache.
        closeCache();
        disconnectFromDS();
        
        // remove the disk store.
        File diskDir = new File(diskStoreName).getAbsoluteFile();
        try {
          com.gemstone.gemfire.internal.FileUtil.delete(diskDir);
        } catch (Exception ex) {
          fail("Failed to delete the disDir");
        }
      }
    };
    return sr;
  }
  
  protected File findFile(String fileName)
  {
    String path = TestUtil.getResourcePath(getClass(), fileName);
    return new File(path);
  }
  
  public final InternalDistributedSystem getSystem(String diskStoreId) {
    new Exception("TEST DEBUG###" + diskStoreId).printStackTrace();
    if (system == null || !system.isConnected()) {
      // Figure out our distributed system properties
      Properties p = getAllDistributedSystemProperties(getDistributedSystemProperties());
      system = (InternalDistributedSystem)DistributedSystem.connect(p);
    } 
    return system;
  }
  
  
  private Cache getCache(InternalDistributedSystem system) {
    Cache cache = basicGetCache();
    if (cache == null) {
      try {
        System.setProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        cache = CacheFactory.create(system); 
      } catch (CacheExistsException e) {
        fail("the cache already exists", e);

      } catch (RuntimeException ex) {
        throw ex;

      } catch (Exception ex) {
        fail("Checked exception while initializing cache??", ex);
      } finally {
        System.clearProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
      }      
    }
    return cache;
  }
  
  public static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }
}

