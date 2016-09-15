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
package org.apache.geode.disttx;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static junit.framework.TestCase.*;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXManagerImplJUnitTest;
import org.apache.geode.test.junit.categories.DistributedTransactionsTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Same tests as that of {@link TXManagerImplJUnitTest} after setting
 * "distributed-transactions" property to true
 */
@Category({IntegrationTest.class, DistributedTransactionsTest.class })
public class DistTXManagerImplJUnitTest extends TXManagerImplJUnitTest {

  @Override
  protected void createCache() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    props.put(ConfigurationProperties.DISTRIBUTED_TRANSACTIONS, "true");
    cache = new CacheFactory(props).create();
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("testRegion");
    CacheTransactionManager txmgr = cache.getCacheTransactionManager();
    assert(txmgr.isDistributed());
  }

  @Override
  protected void callIsDistributed(TXManagerImpl txMgr) {
    assertTrue(txMgr.isDistributed());
  }
}
