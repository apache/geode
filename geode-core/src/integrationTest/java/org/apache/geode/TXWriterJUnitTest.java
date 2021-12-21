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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.transaction.RollbackException;
import javax.transaction.UserTransaction;

import org.junit.Test;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.TransactionWriterException;

public class TXWriterJUnitTest extends TXWriterTestCase {

  /**
   * make sure standard Cache(Listener,Writer) are not called during rollback due to transaction
   * writer throw
   */
  @Test
  public void testNoCallbacksOnTransactionWriterThrow() throws Exception {
    installCacheListenerAndWriter();

    txMgr.setWriter(new TransactionWriter() {
      @Override
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new TransactionWriterException("Rollback now!");
      }

      @Override
      public void close() {}
    });

    installTransactionListener();

    txMgr.begin();
    region.create("key1", "value1");
    cbCount = 0;
    try {
      txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch (CommitConflictException cce) {
      assertNotNull(cce.getCause());
      assertTrue(cce.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, cbCount);

    cbCount = 0;
    region.create("key1", "value1");
    // do a sanity check to make sure callbacks are installed
    assertEquals(2, cbCount); // 2 -> 1writer + 1listener

    txMgr.begin();
    region.put("key1", "value2");
    cbCount = 0;
    try {
      txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch (CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, cbCount);
    region.localDestroy("key1");

    region.create("key1", "value1");
    txMgr.begin();
    region.localDestroy("key1");
    cbCount = 0;
    try {
      txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch (CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, cbCount);

    region.put("key1", "value1");
    txMgr.begin();
    region.destroy("key1");
    cbCount = 0;
    try {
      txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch (CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, cbCount);

    region.put("key1", "value1");
    txMgr.begin();
    region.localInvalidate("key1");
    cbCount = 0;
    try {
      txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch (CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, cbCount);
    region.localDestroy("key1");

    region.put("key1", "value1");
    txMgr.begin();
    region.invalidate("key1");
    cbCount = 0;
    try {
      txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch (CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, cbCount);

    region.localDestroy("key1");
  }

  /**
   * make sure standard Cache(Listener,Writer) are not called during rollback due to transaction
   * writer throw
   */
  @Test
  public void testAfterCommitFailedOnTransactionWriterThrow() throws Exception {
    installCacheListenerAndWriter();

    txMgr.setWriter(new TransactionWriter() {
      @Override
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new TransactionWriterException("Rollback now!");
      }

      @Override
      public void close() {}
    });

    installTransactionListener();

    txMgr.begin();
    region.create("key1", "value1");
    cbCount = 0;
    try {
      txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch (CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof TransactionWriterException);
    }
    assertEquals(0, cbCount);
    assertEquals(1, failedCommits);
    assertEquals(0, afterCommits);
    assertEquals(0, afterRollbacks);
  }

  /**
   * make sure standard Cache(Listener,Writer) are not called during rollback due to transaction
   * writer throw
   */
  @Test
  public void testAfterCommitFailedOnTransactionWriterThrowWithJTA() throws Exception {
    installCacheListenerAndWriter();

    txMgr.setWriter(new TransactionWriter() {
      @Override
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new TransactionWriterException("Rollback now!");
      }

      @Override
      public void close() {}
    });

    installTransactionListener();

    UserTransaction userTx =
        (UserTransaction) cache.getJNDIContext().lookup("java:/UserTransaction");

    userTx.begin();
    region.create("key1", "value1");
    cbCount = 0;
    try {
      userTx.commit();
      fail("Commit should have thrown RollbackException");
    } catch (RollbackException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() + " is not a SynchronizationCommitConflictException",
          expected.getCause() instanceof SynchronizationCommitConflictException);
    }
    assertEquals(0, cbCount);
    assertEquals(1, failedCommits);
    assertEquals(0, afterCommits);
    assertEquals(1, afterRollbacks);
  }

  @Test
  public void testAfterCommitFailedOnThrowNPE() throws Exception {
    installCacheListenerAndWriter();

    txMgr.setWriter(new TransactionWriter() {
      @Override
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new NullPointerException("this is expected!");
      }

      @Override
      public void close() {}
    });

    installTransactionListener();

    txMgr.begin();
    region.create("key1", "value1");
    cbCount = 0;
    try {
      txMgr.commit();
      fail("Commit should have thrown CommitConflictException");
    } catch (CommitConflictException expected) {
      assertNotNull(expected.getCause());
      assertTrue(expected.getCause() instanceof NullPointerException);
    }
    assertEquals(0, cbCount);
    assertEquals(1, failedCommits);
    assertEquals(0, afterCommits);
    assertEquals(0, afterRollbacks);
  }
}
