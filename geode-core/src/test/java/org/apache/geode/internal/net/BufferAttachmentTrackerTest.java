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

package org.apache.geode.internal.net;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Test;

/**
 * Unit tests for BufferAttachmentTracker.
 */
public class BufferAttachmentTrackerTest {

  @After
  public void tearDown() {
    // Clean up after each test
    BufferAttachmentTracker.clearTracking();
  }

  @Test
  public void getOriginal_returnsOriginalBufferForSlice() {
    ByteBuffer original = ByteBuffer.allocateDirect(1024);
    original.position(0).limit(512);
    ByteBuffer slice = original.slice();

    BufferAttachmentTracker.recordSlice(slice, original);

    ByteBuffer result = BufferAttachmentTracker.getOriginal(slice);

    assertThat(result).isSameAs(original);
  }

  @Test
  public void getOriginal_returnsBufferItselfWhenNotTracked() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

    ByteBuffer result = BufferAttachmentTracker.getOriginal(buffer);

    assertThat(result).isSameAs(buffer);
  }

  @Test
  public void removeTracking_removesSliceMapping() {
    ByteBuffer original = ByteBuffer.allocateDirect(1024);
    original.position(0).limit(512);
    ByteBuffer slice = original.slice();

    BufferAttachmentTracker.recordSlice(slice, original);
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(1);

    BufferAttachmentTracker.removeTracking(slice);

    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(0);
    assertThat(BufferAttachmentTracker.getOriginal(slice)).isSameAs(slice);
  }

  @Test
  public void trackingMapSize_reflectsCurrentMappings() {
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(0);

    ByteBuffer original1 = ByteBuffer.allocateDirect(1024);
    ByteBuffer slice1 = original1.slice();
    BufferAttachmentTracker.recordSlice(slice1, original1);
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(1);

    ByteBuffer original2 = ByteBuffer.allocateDirect(2048);
    ByteBuffer slice2 = original2.slice();
    BufferAttachmentTracker.recordSlice(slice2, original2);
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(2);
  }

  @Test
  public void clearTracking_removesAllMappings() {
    ByteBuffer original1 = ByteBuffer.allocateDirect(1024);
    ByteBuffer slice1 = original1.slice();
    BufferAttachmentTracker.recordSlice(slice1, original1);

    ByteBuffer original2 = ByteBuffer.allocateDirect(2048);
    ByteBuffer slice2 = original2.slice();
    BufferAttachmentTracker.recordSlice(slice2, original2);

    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(2);

    BufferAttachmentTracker.clearTracking();

    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(0);
  }

  @Test
  public void recordSlice_canOverwriteExistingMapping() {
    ByteBuffer original1 = ByteBuffer.allocateDirect(1024);
    ByteBuffer original2 = ByteBuffer.allocateDirect(2048);
    ByteBuffer slice = original1.slice();

    BufferAttachmentTracker.recordSlice(slice, original1);
    assertThat(BufferAttachmentTracker.getOriginal(slice)).isSameAs(original1);

    BufferAttachmentTracker.recordSlice(slice, original2);
    assertThat(BufferAttachmentTracker.getOriginal(slice)).isSameAs(original2);
  }

  @Test
  public void worksWithHeapBuffers() {
    ByteBuffer original = ByteBuffer.allocate(1024);
    original.position(0).limit(512);
    ByteBuffer slice = original.slice();

    BufferAttachmentTracker.recordSlice(slice, original);

    ByteBuffer result = BufferAttachmentTracker.getOriginal(slice);

    assertThat(result).isSameAs(original);
  }
}
