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
package org.apache.geode.internal.cache.entries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.InternalRegion;

public class DiskEntryHelperTest {


  private InternalRegion internalRegion = mock(InternalRegion.class);

  private DiskRegion diskRegion = mock(DiskRegion.class);

  private boolean callDoSynchronousWrite() {
    return DiskEntry.Helper.doSynchronousWrite(internalRegion, diskRegion);
  }

  @Test
  public void doSynchronousWriteReturnsTrueWhenDiskRegionIsSync() {
    when(diskRegion.isSync()).thenReturn(true);
    boolean result = callDoSynchronousWrite();
    assertThat(result).isTrue();
  }

  @Test
  public void doSynchronousWriteReturnsTrueWhenPersistentRegionIsInitializing() {
    when(diskRegion.isSync()).thenReturn(false);
    when(diskRegion.isBackup()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(false);

    boolean result = callDoSynchronousWrite();

    assertThat(result).isTrue();
  }

  @Test
  public void doSynchronousWriteReturnsFalseWhenOverflowOnly() {
    when(diskRegion.isSync()).thenReturn(false);
    when(diskRegion.isBackup()).thenReturn(false);
    when(internalRegion.isInitialized()).thenReturn(false);

    boolean result = callDoSynchronousWrite();

    assertThat(result).isFalse();
  }

  @Test
  public void doSynchronousWriteReturnsFalseWhenPersistentRegionIsInitialized() {
    when(diskRegion.isSync()).thenReturn(false);
    when(diskRegion.isBackup()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(true);

    boolean result = callDoSynchronousWrite();

    assertThat(result).isFalse();
  }

}
