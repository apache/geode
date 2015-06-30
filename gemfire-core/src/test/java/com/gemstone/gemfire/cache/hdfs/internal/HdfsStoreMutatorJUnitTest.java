/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSCompactionConfigMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSEventQueueAttributesMutator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.BaseHoplogTestCase;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

@Category({IntegrationTest.class, HoplogTest.class})
public class HdfsStoreMutatorJUnitTest extends BaseHoplogTestCase {
  public void testMutatorInitialState() {
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    assertEquals(-1, mutator.getFileRolloverInterval());
    assertEquals(-1, mutator.getMaxFileSize());
    
    HDFSCompactionConfigMutator compMutator = mutator.getCompactionConfigMutator();
    assertEquals(-1, compMutator.getMaxInputFileCount());
    assertEquals(-1, compMutator.getMaxInputFileSizeMB());
    assertEquals(-1, compMutator.getMinInputFileCount());
    assertEquals(-1, compMutator.getMaxThreads());
    assertNull(mutator.getMinorCompaction());
    
    assertEquals(-1, compMutator.getMajorCompactionIntervalMins());
    assertEquals(-1, compMutator.getMajorCompactionMaxThreads());
    assertNull(compMutator.getAutoMajorCompaction());
    
    assertEquals(-1, compMutator.getOldFilesCleanupIntervalMins());
    
    HDFSEventQueueAttributesMutator qMutator = mutator.getHDFSEventQueueAttributesMutator();
    assertEquals(-1, qMutator.getBatchSizeMB());
    assertEquals(-1, qMutator.getBatchTimeInterval());
  }
  
  public void testMutatorSetInvalidValue() {
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    HDFSCompactionConfigMutator compMutator = mutator.getCompactionConfigMutator();
    HDFSEventQueueAttributesMutator qMutator = mutator.getHDFSEventQueueAttributesMutator();

    try {
      mutator.setFileRolloverInterval(-3);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      mutator.setMaxFileSize(-5);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    
    try {
      compMutator.setMinInputFileCount(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      compMutator.setMaxInputFileCount(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      compMutator.setMaxInputFileSizeMB(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      compMutator.setMaxThreads(-9);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      compMutator.setMajorCompactionIntervalMins(-6);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      compMutator.setMajorCompactionMaxThreads(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      compMutator.setOldFilesCleanupIntervalMins(-4);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
/*    try {
      qMutator.setBatchSizeMB(-985);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      qMutator.setBatchTimeInterval(-695);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
*/    
    try {
      compMutator.setMinInputFileCount(10);
      compMutator.setMaxInputFileCount(5);
      hdfsStore.alter(mutator);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public void testMutatorReturnsUpdatedValues() {
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    HDFSCompactionConfigMutator compMutator = mutator.getCompactionConfigMutator();
    HDFSEventQueueAttributesMutator qMutator = mutator.getHDFSEventQueueAttributesMutator();
    
    mutator.setFileRolloverInterval(121);
    mutator.setMaxFileSize(234);
    
    compMutator.setMaxInputFileCount(87);
    compMutator.setMaxInputFileSizeMB(45);
    compMutator.setMinInputFileCount(34);
    compMutator.setMaxThreads(843);
    mutator.setMinorCompaction(false);

    compMutator.setMajorCompactionIntervalMins(26);
    compMutator.setMajorCompactionMaxThreads(92);
    compMutator.setAutoMajorCompaction(false);
    
    compMutator.setOldFilesCleanupIntervalMins(328);
    
    qMutator.setBatchSizeMB(985);
    qMutator.setBatchTimeInterval(695);
    
    assertEquals(121, mutator.getFileRolloverInterval());
    assertEquals(234, mutator.getMaxFileSize());
    
    assertEquals(87, compMutator.getMaxInputFileCount());
    assertEquals(45, compMutator.getMaxInputFileSizeMB());
    assertEquals(34, compMutator.getMinInputFileCount());
    assertEquals(843, compMutator.getMaxThreads());
    assertFalse(mutator.getMinorCompaction());
    
    assertEquals(26, compMutator.getMajorCompactionIntervalMins());
    assertEquals(92, compMutator.getMajorCompactionMaxThreads());
    assertFalse(compMutator.getAutoMajorCompaction());
    
    assertEquals(328, compMutator.getOldFilesCleanupIntervalMins());
    
    assertEquals(985, qMutator.getBatchSizeMB());
    assertEquals(695, qMutator.getBatchTimeInterval());
    
    // repeat the cycle once more
    mutator.setFileRolloverInterval(14);
    mutator.setMaxFileSize(56);
    
    compMutator.setMaxInputFileCount(93);
    compMutator.setMaxInputFileSizeMB(85);
    compMutator.setMinInputFileCount(64);
    compMutator.setMaxThreads(59);
    mutator.setMinorCompaction(true);
    
    compMutator.setMajorCompactionIntervalMins(26);
    compMutator.setMajorCompactionMaxThreads(92);
    compMutator.setAutoMajorCompaction(false);
    
    compMutator.setOldFilesCleanupIntervalMins(328);
    
    assertEquals(14, mutator.getFileRolloverInterval());
    assertEquals(56, mutator.getMaxFileSize());
    
    assertEquals(93, compMutator.getMaxInputFileCount());
    assertEquals(85, compMutator.getMaxInputFileSizeMB());
    assertEquals(64, compMutator.getMinInputFileCount());
    assertEquals(59, compMutator.getMaxThreads());
    assertTrue(mutator.getMinorCompaction());
    
    assertEquals(26, compMutator.getMajorCompactionIntervalMins());
    assertEquals(92, compMutator.getMajorCompactionMaxThreads());
    assertFalse(compMutator.getAutoMajorCompaction());
    
    assertEquals(328, compMutator.getOldFilesCleanupIntervalMins());
  }
}
