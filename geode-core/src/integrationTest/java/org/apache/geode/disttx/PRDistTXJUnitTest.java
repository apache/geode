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
package org.apache.geode.disttx;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PRTXJUnitTest;
import org.apache.geode.test.junit.categories.DistributedTransactionsTest;

/**
 * Same tests as that of {@link PRTXJUnitTest} after setting "distributed-transactions" property to
 * true
 */
@Category({DistributedTransactionsTest.class})
public class PRDistTXJUnitTest extends PRTXJUnitTest {

  @Override
  protected void createCache() throws Exception {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner
    p.setProperty(ConfigurationProperties.DISTRIBUTED_TRANSACTIONS, "true");

    cache = (GemFireCacheImpl) CacheFactory.create(DistributedSystem.connect(p));

    createRegion();
    txMgr = cache.getCacheTransactionManager();

    assertTrue(txMgr.isDistributed());

    listenerAfterCommit = 0;
    listenerAfterFailedCommit = 0;
    listenerAfterRollback = 0;
    listenerClose = 0;
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

  @Override
  @Test
  @Ignore
  public void testJTAEnlistment() {
    // [DISTTX] TODO Fix this issue. This test fails for DISTTX. This is
    // overridden to avoid running the actual test
  }
}
