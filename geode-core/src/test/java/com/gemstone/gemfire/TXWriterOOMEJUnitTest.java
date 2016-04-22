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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.TransactionEvent;
import com.gemstone.gemfire.cache.TransactionWriter;
import com.gemstone.gemfire.cache.TransactionWriterException;
import com.gemstone.gemfire.internal.SystemFailureTestHook;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Extracted from TXWriterJUnitTest. The OOME is problematic and causes the
 * test suite to fail due to suspect strings from SystemFailure Watchdog sending
 * output to STDERR.
 * 
 */
@Category(IntegrationTest.class)
public class TXWriterOOMEJUnitTest extends TXWriterTestCase {

  @Test
  public void testAfterCommitFailedOnThrowOOM() throws Exception {
    installCacheListenerAndWriter();
    
    // install TransactionWriter
    ((CacheTransactionManager)this.txMgr).setWriter(new TransactionWriter() {
      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        throw new OutOfMemoryError("this is expected!");
      }
      public void close() {}
    });
    
    installTransactionListener();
    
    try {
      SystemFailureTestHook.setExpectedFailureClass(OutOfMemoryError.class);
      
      this.txMgr.begin();
      this.region.create("key1", "value1");
      this.cbCount = 0;
      try {
        this.txMgr.commit();
        fail("Commit should have thrown OOME");
      } catch(OutOfMemoryError expected) {
        // this is what we expect
      }
      
      // no callbacks were invoked
      assertEquals(0, this.cbCount);
      assertEquals(0, this.failedCommits);
      assertEquals(0, this.afterCommits);
      assertEquals(0, this.afterRollbacks);
    } finally {
      SystemFailureTestHook.setExpectedFailureClass(null);
    }
  }
}
