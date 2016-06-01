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

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.distributed.DistributedSystemConfigProperties;
import com.gemstone.gemfire.internal.offheap.TxReleasesOffHeapOnCloseJUnitTest;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;


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
    props.setProperty(DistributedSystemConfigProperties.OFF_HEAP_MEMORY_SIZE, "1m");
    props.put(DistributedSystemConfigProperties.DISTRIBUTED_TRANSACTIONS, "true");
    cache = new CacheFactory(props).create();
    CacheTransactionManager txmgr = cache.getCacheTransactionManager();
    assert(txmgr.isDistributed());
  }

}
