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
package com.gemstone.gemfire.internal.cache.versions;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionVersionHolder2JUnitTest {

  @Test
  public void testCreateHolderWithBitSet() {
    createHolderTest(true);
  }
  
  @Test
  public void testCreateHolderWithoutBitSet() {
    createHolderTest(false);
  }
  
  private void createHolderTest(boolean useBitSet) {
    RegionVersionHolder h = createHolder(useBitSet);
    assertEquals(0, h.getExceptionCount());
  }

  @Test
  public void testRecordZeroDoesNothingWithBitSet() {
    recordZeroDoesNothing(true);
  }
  
  @Test
  public void testRecordZeroDoesNothingWithoutBitSet() {
    recordZeroDoesNothing(false);
  }
  
  private void recordZeroDoesNothing(boolean useBitSet) {
    RegionVersionHolder h = createHolder(useBitSet);
    h.recordVersion(0l);
    assertEquals(0, h.getExceptionCount());
    assertEquals(0l, h.getVersion());
  }
  
  @Test
  public void testRecordSequentialVersionsWithBitSet() {
    recordSequentialVersions(true);
  }

  @Test
  public void testRecordSequentialVersionsWithoutBitSet() {
    recordSequentialVersions(false);
  }

  private void recordSequentialVersions(boolean useBitSet) {
    RegionVersionHolder h = createHolder(useBitSet);
    h.recordVersion(1l);
    h.recordVersion(2l);
    h.recordVersion(3l);
    assertEquals(0, h.getExceptionCount());
    assertEquals(3l, h.getVersion());
  }

  @Test
  public void testSkippedVersionCreatesExceptionWithBitSet() {
    skippedVersionCreatesException(true);
  }

  @Test
  public void testSkippedVersionCreatesExceptionWithoutBitSet() {
    skippedVersionCreatesException(false);
  }

  private void skippedVersionCreatesException(boolean useBitSet) {
    RegionVersionHolder h = createHolder(useBitSet);
    h.recordVersion(2l);
    assertEquals(1, h.getExceptionCount());
    assertEquals(2l, h.getVersion());
  }

  @Test
  public void testFillExceptionWithBitSet() {
    fillException(true);
  }

  @Test
  public void testFillExceptionWithoutBitSet() {
    fillException(false);
  }

  private void fillException(boolean useBitSet) {
    RegionVersionHolder h = createHolder(useBitSet);
    h.recordVersion(3l);
    h.recordVersion(1l);
    h.recordVersion(2l);
    h.recordVersion(2l);
    assertEquals(0, h.getExceptionCount());
    assertEquals(3l, h.getVersion());
  }

  @Test
  public void testFillLargeExceptionWithBitSet() {
    fillLargeException(true);
  }

  @Test
  public void testFillLargeExceptionWithoutBitSet() {
    fillLargeException(false);
  }

  private void fillLargeException(boolean useBitSet) {
    RegionVersionHolder h = createHolder(useBitSet);
    long bigVersion = RegionVersionHolder.BIT_SET_WIDTH + 1;
    h.recordVersion(bigVersion);
    for (long i=0l; i<bigVersion; i++) {
      h.recordVersion(i);
    }
    assertEquals("expected no exceptions in " + h, 0, h.getExceptionCount());
    assertEquals(bigVersion, h.getVersion());
  }

  @Test
  public void testReceiveDuplicateAfterBitSetFlushWithBitSet() {
    RegionVersionHolder h = createHolder(true);
    long bigVersion = RegionVersionHolder.BIT_SET_WIDTH + 1;
    h.recordVersion(bigVersion);
    for (long i=0l; i<bigVersion; i++) {
      h.recordVersion(i);
    }
    h.recordVersion(bigVersion);
    assertEquals("expected no exceptions in " + h, 0, h.getExceptionCount());
    assertEquals(bigVersion, h.getVersion());
  }
  
  @Test
  public void testFillSpecialExceptionWithBitSet() {
    RegionVersionHolder h = createHolder(true);
    h.recordVersion(1l);
    createSpecialException(h);
    assertEquals(1, h.getExceptionCount());
    RVVException e = (RVVException)h.getExceptionForTest().iterator().next();
    assertTrue(h.isSpecialException(e, h));
    h.recordVersion(2l);
    // BUG: the exception is not removed
//    assertIndexDetailsEquals("unexpected RVV exception : " + h, 0, h.getExceptionCount());
  }
  
  private void createSpecialException(RegionVersionHolder h) {
    h.addException(h.getVersion()-1, h.getVersion()+1);
  }

  private RegionVersionHolder createHolder(boolean useBitSet) {
    if (useBitSet) {
      return new RegionVersionHolder("id");
    }
    return new RegionVersionHolder(0l);
  }
  
}
