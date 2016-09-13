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
package org.apache.geode.cache.query.dunit;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
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
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.util.test.TestUtil;

@Category(DistributedTest.class)
public class QueryIndexUsingXMLDUnitTest extends JUnit4CacheTestCase {

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

  @Override
  public final void postSetUp() throws Exception {
    //Workaround for #52008
    IgnoredException.addIgnoredException("Failed to create index");
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    // avoid creating a new cache just to get the diskstore name
    Invoke.invokeInEveryVM(resetTestHook());
    disconnectFromDS();
    FileUtil.delete(new File(GemFireCacheImpl.DEFAULT_DS_NAME).getAbsoluteFile());
  }
  
  /**
   * Creates partitioned index from an xml description.
   */
  @Test
  public void testCreateIndexThroughXML() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    AsyncInvocation asyInvk0 = vm0.invokeAsync(createIndexThrougXML("vm0testCreateIndexThroughXML", name, fileName));
    
    AsyncInvocation asyInvk1 = vm1.invokeAsync(createIndexThrougXML("vm1testCreateIndexThroughXML", name, fileName));
    
    ThreadUtils.join(asyInvk1, 30 * 1000);
    if (asyInvk1.exceptionOccurred()) {
      Assert.fail("asyInvk1 failed", asyInvk1.getException());
    }
    ThreadUtils.join(asyInvk0, 30 * 1000);
    if (asyInvk0.exceptionOccurred()) {
      Assert.fail("asyInvk0 failed", asyInvk0.getException());
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
  @Test
  public void testCreateIndexWhileDoingGII() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
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
  @Test
  public void testReplicatedRegionCreateIndexWhileDoingGII() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
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
  @Test
  public void testPersistentPRRegionCreateIndexWhileDoingGII() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
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
  @Test
  public void testCreateIndexWhileDoingGIIWithEmptyPRRegion() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("### in testCreateIndexWhileDoingGIIWithEmptyPRRegion.");
    

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
   * Creates partitioned index from an xml description.
   */
  @Test
  public void testCreateAsyncIndexWhileDoingGII() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    AsyncInvocation asyInvk0 = vm0.invokeAsync(createIndexThrougXML("vm0testAsyncIndexWhileDoingGII", name, fileName));
    
    ThreadUtils.join(asyInvk0, 30 * 1000);
    if (asyInvk0.exceptionOccurred()) {
      Assert.fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    // LoadRegion
    asyInvk0 = vm0.invokeAsync(loadRegion(name));
    
    vm1.invoke(setTestHook());    
    AsyncInvocation asyInvk1 = vm1.invokeAsync(createIndexThrougXML("vm1testAsyncIndexWhileDoingGII", name, fileName));
    
    vm0.invoke(prIndexCreationCheck(name, statusIndex, 50));

    ThreadUtils.join(asyInvk1, 30 * 1000);
    if (asyInvk1.exceptionOccurred()) {
      Assert.fail("asyInvk1 failed", asyInvk1.getException());
    }
    
    vm1.invoke(prIndexCreationCheck(name, statusIndex, 50));

    ThreadUtils.join(asyInvk0, 30 * 1000);
    if (asyInvk0.exceptionOccurred()) {
      Assert.fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    vm1.invoke(resetTestHook());
    vm0.invoke(close());
    vm1.invoke(close());
  }

  /**
   * Creates indexes and compares the results between index and non-index results.
   */
  @Test
  public void testCreateIndexWhileDoingGIIAndCompareQueryResults() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
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
  @Test
  public void testCreateAsyncIndexWhileDoingGIIAndQuery() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
        "Creating index using an xml file name : " + fileName);
    
    AsyncInvocation asyInvk0 = vm0.invokeAsync(createIndexThrougXML("vm0testCreateAsyncIndexGIIAndQuery", name, fileName));
    ThreadUtils.join(asyInvk0, 30 * 1000);
    if (asyInvk0.exceptionOccurred()) {
      Assert.fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    // LoadRegion
    asyInvk0 = vm0.invokeAsync(loadRegion(name));
    
    vm1.invoke(setTestHook());    
    AsyncInvocation asyInvk1 = vm1.invokeAsync(createIndexThrougXML("vm1testCreateAsyncIndexGIIAndQuery", name, fileName));
 
    
    ThreadUtils.join(asyInvk1, 30 * 1000);  
    if (asyInvk1.exceptionOccurred()) {
      Assert.fail("asyInvk1 failed", asyInvk1.getException());
    }
    ThreadUtils.join(asyInvk0, 30 * 1000);
    if (asyInvk0.exceptionOccurred()) {
      Assert.fail("asyInvk0 failed", asyInvk0.getException());
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
  @Ignore("TODO: test is disabled because of #52167")
  @Test
  public void testCreateAsyncIndexWhileDoingGIIAndCompareQueryResults() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
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
    
    ThreadUtils.join(asyInvk0, 30 * 1000);
    if (asyInvk0.exceptionOccurred()) {
      Assert.fail("asyInvk0 failed", asyInvk0.getException());
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
  
  @Test
  public void testIndexCreationForReplicatedPersistentOverFlowRegionOnRestart() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String fileName = "IndexCreation.xml";
    
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
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
    Wait.waitForCriterion(ev, MAX_TIME, 200, true);
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
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("Execute query : \n queryStr with index: " + s[0]  + " \n queryStr without index: " + s[1]);
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
          org.apache.geode.internal.FileUtil.delete(diskDir);
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
    if (basicGetSystem() == null || !basicGetSystem().isConnected()) {
      // Figure out our distributed system properties
      Properties p = DistributedTestUtils.getAllDistributedSystemProperties(getDistributedSystemProperties());
      getSystem(p);
    } 
    return basicGetSystem();
  }

  private Cache getCache(InternalDistributedSystem system) {
    Cache cache = basicGetCache();
    if (cache == null) {
      try {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        cache = CacheFactory.create(system); 
      } catch (CacheExistsException e) {
        Assert.fail("the cache already exists", e);

      } catch (RuntimeException ex) {
        throw ex;

      } catch (Exception ex) {
        Assert.fail("Checked exception while initializing cache??", ex);
      } finally {
        System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
      }      
    }
    return cache;
  }
  
  public static class QueryObserverImpl extends QueryObserverAdapter {

    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }
}

