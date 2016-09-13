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
package org.apache.geode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.transaction.RollbackException;
import javax.transaction.UserTransaction;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.TransactionWriterException;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class TXWriterJUnitTest extends TXWriterTestCase {
  
  /**
   *  make sure standard Cache(Listener,Writer)
   *  are not called during rollback due to transaction writer throw
   */
  @Test
  public void testNoCallbacksOnTransactionWriterThrow() throws Exception {
    installCacheListenerAndWriter();

    ((CacheTransactionManager)this.txMgr).setWriter(new TransactionWriter() {
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new TransactionWriterException("Rollback now!");
      }
      public void close() {}
    });
    
    installTransactionListener();
    
    this.txMgr.begin();
    this.region.create("key1", "value1");
    this.cbCount = 0;
    try {
      this.txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch(CommitConflictException cce) {
      assertNotNull(cce.getCause());
      assertTrue(cce.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, this.cbCount);

    this.cbCount = 0;
    this.region.create("key1", "value1");
    // do a sanity check to make sure callbacks are installed
    assertEquals(2, this.cbCount); // 2 -> 1writer + 1listener

    this.txMgr.begin();
    this.region.put("key1", "value2");
    this.cbCount = 0;
    try {
      this.txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch(CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, this.cbCount);
    this.region.localDestroy("key1");

    this.region.create("key1", "value1");
    this.txMgr.begin();
    this.region.localDestroy("key1");
    this.cbCount = 0;
    try {
      this.txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch(CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, this.cbCount);

    this.region.put("key1", "value1");
    this.txMgr.begin();
    this.region.destroy("key1");
    this.cbCount = 0;
    try {
      this.txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch(CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, this.cbCount);

    this.region.put("key1", "value1");
    this.txMgr.begin();
    this.region.localInvalidate("key1");
    this.cbCount = 0;
    try {
      this.txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch(CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, this.cbCount);
    this.region.localDestroy("key1");

    this.region.put("key1", "value1");
    this.txMgr.begin();
    this.region.invalidate("key1");
    this.cbCount = 0;
    try {
      this.txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch(CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, this.cbCount);

    this.region.localDestroy("key1");
  }
  
  /**
   * make sure standard Cache(Listener,Writer)
   * are not called during rollback due to transaction writer throw
   */
  @Test
  public void testAfterCommitFailedOnTransactionWriterThrow() throws Exception {
    installCacheListenerAndWriter();

    ((CacheTransactionManager)this.txMgr).setWriter(new TransactionWriter() {
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new TransactionWriterException("Rollback now!");
      }
      public void close() {}
    });
    
    installTransactionListener();
    
    this.txMgr.begin();
    this.region.create("key1", "value1");
    this.cbCount = 0;
    try {
      this.txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch(CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, this.cbCount);
    assertEquals(1, this.failedCommits);
    assertEquals(0, this.afterCommits);
    assertEquals(0, this.afterRollbacks);
  }
  
  /**
   * make sure standard Cache(Listener,Writer)
   * are not called during rollback due to transaction writer throw
   */
  @Test
  public void testAfterCommitFailedOnTransactionWriterThrowWithJTA() throws Exception {
    installCacheListenerAndWriter();

    ((CacheTransactionManager)this.txMgr).setWriter(new TransactionWriter() {
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new TransactionWriterException("Rollback now!");
      }
      public void close() {}
    });
    
    installTransactionListener();
    
    UserTransaction userTx = (UserTransaction)this.cache.getJNDIContext().lookup("java:/UserTransaction");
    
    userTx.begin();
    this.region.create("key1", "value1");
    this.cbCount = 0;
    try {
      userTx.commit();
      fail("Commit should have thrown RollbackException");
    } catch(RollbackException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() + " is not a SynchronizationCommitConflictException", 
          expected.getCause() instanceof SynchronizationCommitConflictException);
    }
    assertEquals(0, this.cbCount);
    assertEquals(1, this.failedCommits);
    assertEquals(0, this.afterCommits);
    assertEquals(1, this.afterRollbacks);
  }
  
  @Test
  public void testAfterCommitFailedOnThrowNPE() throws Exception {
    installCacheListenerAndWriter();

    ((CacheTransactionManager)this.txMgr).setWriter(new TransactionWriter() {
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new NullPointerException("this is expected!");
      }
      public void close() {}
    });
    
    installTransactionListener();
    
    this.txMgr.begin();
    this.region.create("key1", "value1");
    this.cbCount = 0;
    try {
      this.txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch(CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof NullPointerException);
    }
    assertEquals(0, this.cbCount);
    assertEquals(1, this.failedCommits);
    assertEquals(0, this.afterCommits);
    assertEquals(0, this.afterRollbacks);
  }
}
