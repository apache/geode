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

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.offheap.TxReleasesOffHeapOnCloseJUnitTest;
import org.apache.geode.test.junit.categories.DistributedTransactionsTest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;


/**
 * Same tests as that of {@link TxReleasesOffHeapOnCloseJUnitTest} after setting
 * "distributed-transactions" property to true
 */
@Category({IntegrationTest.class, DistributedTransactionsTest.class})
public class DistTXReleasesOffHeapOnCloseJUnitTest extends
    TxReleasesOffHeapOnCloseJUnitTest {

  public DistTXReleasesOffHeapOnCloseJUnitTest() {
  }
  
  @Override
  protected void createCache() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, "1m");
    props.put(ConfigurationProperties.DISTRIBUTED_TRANSACTIONS, "true");
    cache = new CacheFactory(props).create();
    CacheTransactionManager txmgr = cache.getCacheTransactionManager();
    assert(txmgr.isDistributed());
  }

}
