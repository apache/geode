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
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.TransactionWriterException;
import org.apache.geode.internal.SystemFailureTestHook;

/**
 * Extracted from TXWriterJUnitTest. The OOME is problematic and causes the test suite to fail due
 * to suspect strings from SystemFailure Watchdog sending output to STDERR.
 *
 */
public class TXWriterOOMEJUnitTest extends TXWriterTestCase {

  @Test
  public void testAfterCommitFailedOnThrowOOM() throws Exception {
    installCacheListenerAndWriter();

    // install TransactionWriter
    txMgr.setWriter(new TransactionWriter() {
      @Override
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new OutOfMemoryError("this is expected!");
      }

      @Override
      public void close() {}
    });

    installTransactionListener();

    try {
      SystemFailureTestHook.setExpectedFailureClass(OutOfMemoryError.class);

      txMgr.begin();
      region.create("key1", "value1");
      cbCount = 0;
      try {
        txMgr.commit();
        fail("Commit should have thrown OOME");
      } catch (OutOfMemoryError expected) {
        // this is what we expect
      }

      // no callbacks were invoked
      assertEquals(0, cbCount);
      assertEquals(0, failedCommits);
      assertEquals(0, afterCommits);
      assertEquals(0, afterRollbacks);
    } finally {
      SystemFailureTestHook.setExpectedFailureClass(null);
    }
  }
}
