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
package com.gemstone.gemfire;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;

/**
 * Extracted from TXWriterJUnitTest to share with TXWriterOOMEJUnitTest.
 * 
 */
@SuppressWarnings("deprecation")
public class TXWriterTestCase {

  protected int cbCount;
  protected int failedCommits = 0;
  protected int afterCommits = 0;
  protected int afterRollbacks = 0;
  
  protected GemFireCacheImpl cache;
  protected CacheTransactionManager txMgr;
  protected Region<String, String> region;

  protected void createCache() throws CacheException {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner
    this.cache = (GemFireCacheImpl)CacheFactory.create(DistributedSystem.connect(p));
    AttributesFactory<?, ?> af = new AttributesFactory<String, String>();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setIndexMaintenanceSynchronous(true);
    this.region = this.cache.createRegion("TXTest", af.create());
    this.txMgr = this.cache.getCacheTransactionManager();
  }
  private void closeCache() {
    if (this.cache != null) {
      if (this.txMgr != null) {
        try {
          this.txMgr.rollback();
        } catch (IllegalStateException ignore) {
        }
      }
      this.region = null;
      this.txMgr = null;
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }
  
  @Before
  public void setUp() {
    createCache();
  }

  @After
  public void tearDown() {
    try {
      if (this.txMgr != null) {
        ((CacheTransactionManager)this.txMgr).setWriter(null);
        ((CacheTransactionManager)this.txMgr).setListener(null);
      }
    } finally {
      closeCache();
    }
  }
  
  @AfterClass
  public static void afterClass() {
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }

  protected void installCacheListenerAndWriter() {
    AttributesMutator<String, String> mutator = this.region.getAttributesMutator();
    mutator.setCacheListener(new CacheListenerAdapter<String, String>() {
        public void close() {cbCount++;}
        public void afterCreate(EntryEvent<String, String> event) {cbCount++;}
        public void afterUpdate(EntryEvent<String, String> event) {cbCount++;}
        public void afterInvalidate(EntryEvent<String, String> event) {cbCount++;}
        public void afterDestroy(EntryEvent<String, String> event) {cbCount++;}
        public void afterRegionInvalidate(RegionEvent<String, String> event) {cbCount++;}
        public void afterRegionDestroy(RegionEvent<String, String> event) {cbCount++;}
      });
    mutator.setCacheWriter(new CacheWriter<String, String>() {
        public void close() {cbCount++;}
        public void beforeUpdate(EntryEvent<String, String> event)
          throws CacheWriterException {cbCount++;}
        public void beforeCreate(EntryEvent<String, String> event)
          throws CacheWriterException {cbCount++;}
        public void beforeDestroy(EntryEvent<String, String> event)
          throws CacheWriterException {cbCount++;}
        public void beforeRegionDestroy(RegionEvent<String, String> event)
          throws CacheWriterException {cbCount++;}
        public void beforeRegionClear(RegionEvent<String, String> event)
          throws CacheWriterException {cbCount++;}
      });
  }
  
  protected void installTransactionListener() {
    ((CacheTransactionManager)this.txMgr).setListener(new TransactionListener() {
      public void afterFailedCommit(TransactionEvent event) {
        failedCommits++;
      }
      public void afterCommit(TransactionEvent event) {
        afterCommits++;
      }
      public void afterRollback(TransactionEvent event) {
        afterRollbacks++;
      }
      public void close() {}
    });
  }
}
