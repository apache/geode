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
package org.apache.geode.internal.offheap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class FragmentJUnitTest {

  private SlabImpl[] slabs;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public JUnitSoftAssertions softly = new JUnitSoftAssertions();

  @Before
  public void setUp() throws Exception {
    SlabImpl slab1 = new SlabImpl((int) OffHeapStorage.MIN_SLAB_SIZE);
    SlabImpl slab2 = new SlabImpl((int) OffHeapStorage.MIN_SLAB_SIZE);
    slabs = new SlabImpl[] {slab1, slab2};
  }

  @After
  public void tearDown() throws Exception {
    for (int i = 0; i < slabs.length; i++) {
      slabs[i].free();
    }
  }

  @Test
  public void fragmentConstructorThrowsExceptionForNon8ByteAlignedAddress() {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("address was not 8 byte aligned");

    new Fragment(slabs[0].getMemoryAddress() + 2, 0);
    fail("Constructor failed to throw exception for non-8-byte alignment");
  }

  @Test
  public void zeroSizeFragmentHasNoFreeSpace() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), 0);
    assertThat(fragment.freeSpace(), is(0));
  }

  @Test
  public void unallocatedFragmentHasFreeSpaceEqualToFragmentSize() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.getSize()).isEqualTo((int) OffHeapStorage.MIN_SLAB_SIZE);
    softly.assertThat(fragment.freeSpace()).isEqualTo((int) OffHeapStorage.MIN_SLAB_SIZE);
  }

  @Test
  public void allocatingFromFragmentReducesFreeSpace() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256))
        .isEqualTo(true);
    softly.assertThat(fragment.freeSpace()).isEqualTo(768);
    softly.assertThat(fragment.getFreeIndex()).isEqualTo(256);
  }

  @Test
  public void fragementAllocationIsUnsafeWithRespectToAllocationSize() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.allocate(fragment.getFreeIndex(),
        fragment.getFreeIndex() + (int) OffHeapStorage.MIN_SLAB_SIZE + 8)).isEqualTo(true);
    softly.assertThat(fragment.freeSpace()).isEqualTo(-8);
  }

  @Test
  public void getBlockSizeReturnsFreeSpace() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256))
        .isEqualTo(true);
    softly.assertThat(fragment.getBlockSize()).isEqualTo(fragment.freeSpace());
  }

  @Test
  public void getMemoryAdressIsAlwaysFragmentBaseAddress() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.getAddress()).isEqualTo(slabs[0].getMemoryAddress());
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    softly.assertThat(fragment.getAddress()).isEqualTo(slabs[0].getMemoryAddress());
  }

  @Test
  public void getStateIsAlwaysStateUNUSED() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.getState()).isEqualTo(MemoryBlock.State.UNUSED);
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    softly.assertThat(fragment.getState()).isEqualTo(MemoryBlock.State.UNUSED);
  }

  @Test
  public void getFreeListIdIsAlwaysMinus1() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.getFreeListId()).isEqualTo(-1);
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    softly.assertThat(fragment.getFreeListId()).isEqualTo(-1);
  }

  @Test
  public void getRefCountIsAlwaysZero() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.getRefCount()).isEqualTo(0);
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    softly.assertThat(fragment.getRefCount()).isEqualTo(0);
  }

  @Test
  public void getDataTypeIsAlwaysNA() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.getDataType()).isEqualTo("N/A");
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    softly.assertThat(fragment.getDataType()).isEqualTo("N/A");
  }

  @Test
  public void isSerializedIsAlwaysFalse() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.isSerialized()).isEqualTo(false);
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    softly.assertThat(fragment.isSerialized()).isEqualTo(false);
  }

  @Test
  public void isCompressedIsAlwaysFalse() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.isCompressed()).isEqualTo(false);
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    softly.assertThat(fragment.isCompressed()).isEqualTo(false);
  }

  @Test
  public void getDataValueIsAlwaysNull() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.getDataValue()).isNull();
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    softly.assertThat(fragment.getDataValue()).isNull();
  }

  @Test
  public void fragmentEqualsComparesMemoryBlockAddresses() {
    Fragment fragment0 = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Fragment sameFragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Fragment fragment1 = new Fragment(slabs[1].getMemoryAddress(), slabs[1].getSize());
    softly.assertThat(fragment0.equals(sameFragment)).isEqualTo(true);
    softly.assertThat(fragment0.equals(fragment1)).isEqualTo(false);
  }

  @Test
  public void fragmentEqualsIsFalseForNonFragmentObjects() {
    Fragment fragment0 = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment0.equals(slabs[0]), is(false));
  }

  @Test
  public void fragmentHashCodeIsHashCodeOfItsMemoryAddress() {
    Fragment fragment0 = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Fragment fragment1 = new Fragment(slabs[1].getMemoryAddress(), slabs[1].getSize());
    Long fragmentAddress = fragment0.getAddress();
    softly.assertThat(fragment0.hashCode()).isEqualTo(fragmentAddress.hashCode())
        .isNotEqualTo(fragment1.hashCode());
  }

  @Test
  public void fragmentFillSetsAllBytesToTheSameConstantValue() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Long fragmentAddress = fragment.getAddress();
    byte[] bytes = new byte[(int) OffHeapStorage.MIN_SLAB_SIZE];
    byte[] expectedBytes = new byte[(int) OffHeapStorage.MIN_SLAB_SIZE];
    Arrays.fill(expectedBytes, OffHeapStoredObject.FILL_BYTE);
    fragment.fill();
    AddressableMemoryManager.readBytes(fragmentAddress, bytes, 0,
        (int) OffHeapStorage.MIN_SLAB_SIZE);
    assertThat(bytes, is(equalTo(expectedBytes)));
  }

  @Test
  public void getNextBlockThrowsExceptionForFragment() {
    expectedException.expect(UnsupportedOperationException.class);

    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    fragment.getNextBlock();
    fail("getNextBlock failed to throw UnsupportedOperationException");
  }

  @Test
  public void getSlabIdThrowsExceptionForFragment() {
    expectedException.expect(UnsupportedOperationException.class);

    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    fragment.getSlabId();
    fail("getSlabId failed to throw UnsupportedOperationException");
  }

}
