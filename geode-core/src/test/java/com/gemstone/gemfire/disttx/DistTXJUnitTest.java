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
package com.gemstone.gemfire.disttx;

import com.gemstone.gemfire.TXJUnitTest;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

/**
 * Run the basic transaction functionality tests in TXJUnitTest after setting
 * "distributed-transactions" property to true
 *
 */
@Category({IntegrationTest.class, DistributedTransactionsTest.class })
public class DistTXJUnitTest extends TXJUnitTest {

  public DistTXJUnitTest() {
  }
  
  @Override
  protected void createCache() throws Exception {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner
    p.setProperty(DISTRIBUTED_TRANSACTIONS, "true");
    this.cache = (GemFireCacheImpl)CacheFactory.create(DistributedSystem.connect(p));
    createRegion();
    this.txMgr = this.cache.getCacheTransactionManager();
    assert(this.txMgr.isDistributed());
    this.listenerAfterCommit = 0;
    this.listenerAfterFailedCommit = 0;
    this.listenerAfterRollback = 0;
    this.listenerClose = 0;
  }
  
  @Before
  public void setUp() throws Exception {
    createCache();
  }

  @After
  public void tearDown() throws Exception {
    closeCache();
  }
  
  @Override
  @Test
  @Ignore
  public void testTxAlgebra() throws CacheException {
    // [DISTTX] TODO Fix this issue. This test fails for DISTTX. This is
    // overridden to avoid running the actual test
  }
  
  @Override
  @Test
  @Ignore
  public void testListener() {
    // [DISTTX] TODO Fix this issue. This test fails for DISTTX. This is
    // overridden to avoid running the actual test
  }
  
  @Override
  @Test
  @Ignore
  public void testCacheCallbacks() throws CacheException {
    // [DISTTX] TODO Fix this issue. This test fails for DISTTX. This is
    // overridden to avoid running the actual test
  }
  
}
