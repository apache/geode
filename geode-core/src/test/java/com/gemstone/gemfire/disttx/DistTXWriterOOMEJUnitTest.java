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

import com.gemstone.gemfire.TXWriterOOMEJUnitTest;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.ConfigurationProperties;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;

/**
 * Same tests as that of {@link TXWriterOOMEJUnitTest} after setting
 * "distributed-transactions" property to true
 */
@Category({ IntegrationTest.class, DistributedTransactionsTest.class })
public class DistTXWriterOOMEJUnitTest extends TXWriterOOMEJUnitTest {

  public DistTXWriterOOMEJUnitTest() {
  }

  protected void createCache() throws CacheException {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner
    p.setProperty(ConfigurationProperties.DISTRIBUTED_TRANSACTIONS, "true");
    this.cache = (GemFireCacheImpl) CacheFactory.create(DistributedSystem
        .connect(p));
    AttributesFactory<?, ?> af = new AttributesFactory<String, String>();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setIndexMaintenanceSynchronous(true);
    this.region = this.cache.createRegion("TXTest", af.create());
    this.txMgr = this.cache.getCacheTransactionManager();
    assert (this.txMgr.isDistributed());
  }

}
