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

import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Basic funtional test for removing index from a partitioned region system.
 * @author rdubey
 * 
 */
public class PRBasicRemoveIndexDUnitTest extends PartitionedRegionDUnitTestCase
{
  /**
   * Constructor
   * @param name
   */  
  public PRBasicRemoveIndexDUnitTest (String name) {
    super(name);
  }
  
  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");
  
  /**
   * Name of the partitioned region for the test.
   */
  final String name = "PartionedPortfolios";
  
  
  final int start = 0;
  
  final int end = 1003;

  /**
   * Reduncancy level for the pr.
   */
  final int redundancy = 0;

  
  /**
   * Remove index test to remove all the indexes in a given partitioned region
   * 
   * @throws Exception
   *           if the test fails
   */
  public void testPRBasicIndexRemove() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    LogWriterUtils.getLogWriter().info(
        "PRBasicRemoveIndexDUnitTest.testPRBasicIndexCreate test now starts ....");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(start, end);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        start, end));
    
    // create all the indexes.
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid",null, "p"));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    
    //remove indexes
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForRemoveIndex(name, false));
    
    LogWriterUtils.getLogWriter().info(
    "PRBasicRemoveIndexDUnitTest.testPRBasicRemoveIndex test now  ends sucessfully");

  }
  
  /**
   * Test removing single index on a pr.
   */
  public void testPRBasicRemoveParticularIndex() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    LogWriterUtils.getLogWriter().info(
        "PRBasicRemoveIndexDUnitTest.testPRBasicIndexCreate test now starts ....");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(start, end);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        start, end));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid",null, "p"));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    
//  remove indexes
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForRemoveIndex(name, true));
    
    
  }
  
}
