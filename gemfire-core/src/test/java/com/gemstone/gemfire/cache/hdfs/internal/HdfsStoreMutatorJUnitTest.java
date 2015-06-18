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
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.BaseHoplogTestCase;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category({IntegrationTest.class, HoplogTest.class})
public class HdfsStoreMutatorJUnitTest extends BaseHoplogTestCase {
  public void testMutatorInitialState() {
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    assertEquals(-1, mutator.getWriteOnlyFileRolloverInterval());
    assertEquals(-1, mutator.getMaxWriteOnlyFileSize());
    
    assertEquals(-1, mutator.getMaxInputFileCount());
    assertEquals(-1, mutator.getMaxInputFileSizeMB());
    assertEquals(-1, mutator.getMinInputFileCount());
    assertEquals(-1, mutator.getMinorCompactionThreads());
    assertNull(mutator.getMinorCompaction());
    
    assertEquals(-1, mutator.getMajorCompactionInterval());
    assertEquals(-1, mutator.getMajorCompactionThreads());
    assertNull(mutator.getMajorCompaction());
    
    assertEquals(-1, mutator.getPurgeInterval());
    
    assertEquals(-1, mutator.getBatchSize());
    assertEquals(-1, mutator.getBatchInterval());
  }
  
  public void testMutatorSetInvalidValue() {
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();

    try {
      mutator.setWriteOnlyFileRolloverInterval(-3);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      mutator.setMaxWriteOnlyFileSize(-5);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    
    try {
      mutator.setMinInputFileCount(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      mutator.setMaxInputFileCount(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      mutator.setMaxInputFileSizeMB(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      mutator.setMinorCompactionThreads(-9);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      mutator.setMajorCompactionInterval(-6);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      mutator.setMajorCompactionThreads(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      mutator.setPurgeInterval(-4);
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
      mutator.setMinInputFileCount(10);
      mutator.setMaxInputFileCount(5);
      hdfsStore.alter(mutator);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public void testMutatorReturnsUpdatedValues() {
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    
    mutator.setWriteOnlyFileRolloverInterval(121);
    mutator.setMaxWriteOnlyFileSize(234);
    
    mutator.setMaxInputFileCount(87);
    mutator.setMaxInputFileSizeMB(45);
    mutator.setMinInputFileCount(34);
    mutator.setMinorCompactionThreads(843);
    mutator.setMinorCompaction(false);

    mutator.setMajorCompactionInterval(26);
    mutator.setMajorCompactionThreads(92);
    mutator.setMajorCompaction(false);
    
    mutator.setPurgeInterval(328);
    
    mutator.setBatchSize(985);
    mutator.setBatchInterval(695);
    
    assertEquals(121, mutator.getWriteOnlyFileRolloverInterval());
    assertEquals(234, mutator.getMaxWriteOnlyFileSize());
    
    assertEquals(87, mutator.getMaxInputFileCount());
    assertEquals(45, mutator.getMaxInputFileSizeMB());
    assertEquals(34, mutator.getMinInputFileCount());
    assertEquals(843, mutator.getMinorCompactionThreads());
    assertFalse(mutator.getMinorCompaction());
    
    assertEquals(26, mutator.getMajorCompactionInterval());
    assertEquals(92, mutator.getMajorCompactionThreads());
    assertFalse(mutator.getMajorCompaction());
    
    assertEquals(328, mutator.getPurgeInterval());
    
    assertEquals(985, mutator.getBatchSize());
    assertEquals(695, mutator.getBatchInterval());
    
    // repeat the cycle once more
    mutator.setWriteOnlyFileRolloverInterval(14);
    mutator.setMaxWriteOnlyFileSize(56);
    
    mutator.setMaxInputFileCount(93);
    mutator.setMaxInputFileSizeMB(85);
    mutator.setMinInputFileCount(64);
    mutator.setMinorCompactionThreads(59);
    mutator.setMinorCompaction(true);
    
    mutator.setMajorCompactionInterval(26);
    mutator.setMajorCompactionThreads(92);
    mutator.setMajorCompaction(false);
    
    mutator.setPurgeInterval(328);
    
    assertEquals(14, mutator.getWriteOnlyFileRolloverInterval());
    assertEquals(56, mutator.getMaxWriteOnlyFileSize());
    
    assertEquals(93, mutator.getMaxInputFileCount());
    assertEquals(85, mutator.getMaxInputFileSizeMB());
    assertEquals(64, mutator.getMinInputFileCount());
    assertEquals(59, mutator.getMinorCompactionThreads());
    assertTrue(mutator.getMinorCompaction());
    
    assertEquals(26, mutator.getMajorCompactionInterval());
    assertEquals(92, mutator.getMajorCompactionThreads());
    assertFalse(mutator.getMajorCompaction());
    
    assertEquals(328, mutator.getPurgeInterval());
  }
}
