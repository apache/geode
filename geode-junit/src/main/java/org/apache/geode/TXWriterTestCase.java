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
package org.apache.geode;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;

/**
 * Extracted from TXWriterJUnitTest to share with TXWriterOOMEJUnitTest.
 */
@SuppressWarnings("deprecation")
public class TXWriterTestCase {

  int cbCount;
  int failedCommits = 0;
  int afterCommits = 0;
  int afterRollbacks = 0;

  protected GemFireCacheImpl cache;
  protected CacheTransactionManager txMgr;
  protected Region<String, String> region;

  protected void createCache() throws CacheException {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner

    this.cache = (GemFireCacheImpl) CacheFactory.create(DistributedSystem.connect(p));

    AttributesFactory<String, String> af = new AttributesFactory<>();
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
        ((CacheTransactionManager) this.txMgr).setWriter(null);
        ((CacheTransactionManager) this.txMgr).setListener(null);
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

  void installCacheListenerAndWriter() {
    AttributesMutator<String, String> mutator = this.region.getAttributesMutator();
    mutator.addCacheListener(new CacheListenerAdapter<String, String>() {
      @Override
      public void close() {
        cbCount++;
      }

      @Override
      public void afterCreate(EntryEvent<String, String> event) {
        cbCount++;
      }

      @Override
      public void afterUpdate(EntryEvent<String, String> event) {
        cbCount++;
      }

      @Override
      public void afterInvalidate(EntryEvent<String, String> event) {
        cbCount++;
      }

      @Override
      public void afterDestroy(EntryEvent<String, String> event) {
        cbCount++;
      }

      @Override
      public void afterRegionInvalidate(RegionEvent<String, String> event) {
        cbCount++;
      }

      @Override
      public void afterRegionDestroy(RegionEvent<String, String> event) {
        cbCount++;
      }
    });
    mutator.setCacheWriter(new CacheWriter<String, String>() {
      @Override
      public void close() {
        cbCount++;
      }

      @Override
      public void beforeUpdate(EntryEvent<String, String> event) throws CacheWriterException {
        cbCount++;
      }

      @Override
      public void beforeCreate(EntryEvent<String, String> event) throws CacheWriterException {
        cbCount++;
      }

      @Override
      public void beforeDestroy(EntryEvent<String, String> event) throws CacheWriterException {
        cbCount++;
      }

      @Override
      public void beforeRegionDestroy(RegionEvent<String, String> event)
          throws CacheWriterException {
        cbCount++;
      }

      @Override
      public void beforeRegionClear(RegionEvent<String, String> event) throws CacheWriterException {
        cbCount++;
      }
    });
  }

  void installTransactionListener() {
    this.txMgr.setListener(new TransactionListener() {
      @Override
      public void afterFailedCommit(TransactionEvent event) {
        failedCommits++;
      }

      @Override
      public void afterCommit(TransactionEvent event) {
        afterCommits++;
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        afterRollbacks++;
      }

      @Override
      public void close() {}
    });
  }
}
