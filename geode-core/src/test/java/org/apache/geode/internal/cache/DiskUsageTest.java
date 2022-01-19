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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Test;

import org.apache.geode.internal.cache.DiskStoreMonitor.DiskState;
import org.apache.geode.internal.cache.DiskStoreMonitor.DiskUsage;

public class DiskUsageTest {

  @Test
  public void defaultStateIsNormal() {
    DiskUsage diskUsage = new TestableDiskUsage();
    assertThat(diskUsage.getState()).isEqualTo(DiskState.NORMAL);
  }

  @Test
  public void updateReturnsCurrentStateIfThresholdsDisabled() {
    DiskUsage diskUsage = new TestableDiskUsage();
    assertThat(diskUsage.update(0.0f, 0.0f)).isEqualTo(DiskState.NORMAL);
  }

  @Test
  public void updateReturnsCurrentStateIfDirectoryDoesNotExist() {
    File dir = mock(File.class);
    DiskUsage diskUsage = new TestableDiskUsage(dir);
    assertThat(diskUsage.update(1.0f, 0.0f)).isEqualTo(DiskState.NORMAL);
    verify(dir, times(1)).exists();
  }

  @Test
  public void updateSendsTotalToRecordsStats() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    final long expectedTotal = 123;
    when(dir.getTotalSpace()).thenReturn(expectedTotal);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir);
    diskUsage.update(0.0f, 1.0f);
    assertThat(diskUsage.getTotal()).isEqualTo(expectedTotal);
  }

  @Test
  public void updateSendsUsableSpaceToRecordsStats() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    final long expectedFree = 456;
    when(dir.getUsableSpace()).thenReturn(expectedFree);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir);
    diskUsage.update(0.0f, 1.0f);
    assertThat(diskUsage.getFree()).isEqualTo(expectedFree);
  }

  @Test
  public void updateChangesStateToWarn() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    // File indicates 2% of disk used
    when(dir.getTotalSpace()).thenReturn(100L);
    when(dir.getUsableSpace()).thenReturn(98L);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir);
    // warn if over 1.9% used
    assertThat(diskUsage.update(1.9f, 0.0f)).isEqualTo(DiskState.WARN);
    assertThat(diskUsage.getNext()).isEqualTo(DiskState.WARN);
    assertThat(diskUsage.getPct()).isEqualTo("2%");
    assertThat(diskUsage.getCriticalMessage()).isNull();
  }

  @Test
  public void updateStaysNormalIfBelowWarnThreshold() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    // File indicates 2% of disk used
    when(dir.getTotalSpace()).thenReturn(100L);
    when(dir.getUsableSpace()).thenReturn(98L);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir);
    // warn if over 2.1% used
    assertThat(diskUsage.update(2.1f, 0.0f)).isEqualTo(DiskState.NORMAL);
    assertThat(diskUsage.getNext()).isNull();
    assertThat(diskUsage.getPct()).isNull();
    assertThat(diskUsage.getCriticalMessage()).isNull();
  }

  @Test
  public void updateWarnThresholdIgnoresMinimum() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    // File indicates 2% of disk used
    when(dir.getTotalSpace()).thenReturn(100L);
    when(dir.getUsableSpace()).thenReturn(98L);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir, 1);
    // warn if over 2.1% used
    assertThat(diskUsage.update(2.1f, 0.0f)).isEqualTo(DiskState.NORMAL);
    assertThat(diskUsage.getNext()).isNull();
    assertThat(diskUsage.getPct()).isNull();
    assertThat(diskUsage.getCriticalMessage()).isNull();
  }

  @Test
  public void updateChangesStateToCritical() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    // File indicates 2% of disk used
    when(dir.getTotalSpace()).thenReturn(100L);
    when(dir.getUsableSpace()).thenReturn(98L);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir);
    // critical if over 1.9% used
    assertThat(diskUsage.update(0.0f, 1.9f)).isEqualTo(DiskState.CRITICAL);
    assertThat(diskUsage.getNext()).isEqualTo(DiskState.CRITICAL);
    assertThat(diskUsage.getPct()).isEqualTo("2%");
    assertThat(diskUsage.getCriticalMessage())
        .isEqualTo("the file system is 2% full, which reached the critical threshold of 1.9%.");
  }

  @Test
  public void updateStaysNormalIfBelowCriticalThreshold() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    // File indicates 2% of disk used
    when(dir.getTotalSpace()).thenReturn(100L);
    when(dir.getUsableSpace()).thenReturn(98L);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir);
    // critical if over 2.1% used
    assertThat(diskUsage.update(0.0f, 2.1f)).isEqualTo(DiskState.NORMAL);
    assertThat(diskUsage.getNext()).isNull();
    assertThat(diskUsage.getPct()).isNull();
    assertThat(diskUsage.getCriticalMessage()).isNull();
  }

  @Test
  public void updateCriticalThresholdGoesCriticalIfBelowMinimum() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    // File indicates 2% of disk used
    when(dir.getTotalSpace()).thenReturn(100L);
    when(dir.getUsableSpace()).thenReturn(98L);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir, 1/* one megabyte */);
    // critical if over 2.1% used
    assertThat(diskUsage.update(0.0f, 2.1f)).isEqualTo(DiskState.CRITICAL);
    assertThat(diskUsage.getNext()).isEqualTo(DiskState.CRITICAL);
    assertThat(diskUsage.getPct()).isEqualTo("2%");
    assertThat(diskUsage.getCriticalMessage())
        .isEqualTo("the file system only has 98 bytes free which is below the minimum of 1048576.");
  }

  @Test
  public void criticalMessageStatesUsageExceedsCritical() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    when(dir.getTotalSpace()).thenReturn(1024L * 1024L * 3L);
    when(dir.getUsableSpace()).thenReturn(1024L * 1024L * 2L);

    TestableDiskUsage diskUsage = new TestableDiskUsage(dir, 1/* one megabyte */);

    assertThat(diskUsage.update(0.0f, 33.2f)).isEqualTo(DiskState.CRITICAL);
    assertThat(diskUsage.getNext()).isEqualTo(DiskState.CRITICAL);
    assertThat(diskUsage.getPct()).isEqualTo("33.3%");
    assertThat(diskUsage.getCriticalMessage())
        .isEqualTo("the file system is 33.3% full, which reached the critical threshold of 33.2%.");
  }

  @Test
  public void criticalMessageStatesUsageExceedsCriticalWithManyDigits() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    when(dir.getTotalSpace()).thenReturn(1024L * 1024L * 3L);
    when(dir.getUsableSpace()).thenReturn(1024L * 1024L * 2L);

    TestableDiskUsage diskUsage = new TestableDiskUsage(dir, 1/* one megabyte */);

    assertThat(diskUsage.update(0.0f, 33.25783495783648593746336f)).isEqualTo(DiskState.CRITICAL);
    assertThat(diskUsage.getNext()).isEqualTo(DiskState.CRITICAL);
    assertThat(diskUsage.getPct()).isEqualTo("33.3%");
    assertThat(diskUsage.getCriticalMessage())
        .isEqualTo("the file system is 33.3% full, which reached the critical threshold of 33.3%.");
  }

  @Test
  public void updateCriticalThresholdStaysNormalIfFreeSpaceAboveMinimum() {
    File dir = mock(File.class);
    when(dir.exists()).thenReturn(true);
    // File indicates 98% of disk used
    when(dir.getTotalSpace()).thenReturn(100L * 1024 * 1024);
    when(dir.getUsableSpace()).thenReturn(2L * 1024 * 1024);
    TestableDiskUsage diskUsage = new TestableDiskUsage(dir, 1/* one megabyte */);
    // critical if over 98.1% used
    assertThat(diskUsage.update(0.0f, 98.1f)).isEqualTo(DiskState.NORMAL);
    assertThat(diskUsage.getNext()).isNull();
    assertThat(diskUsage.getPct()).isNull();
    assertThat(diskUsage.getCriticalMessage()).isNull();
  }

  private static class TestableDiskUsage extends DiskUsage {

    private final File dir;
    private final long minimum;

    private long total;
    private long free;
    private long elapsed;
    private DiskState next;
    private String pct;
    private String criticalMessage;

    public TestableDiskUsage(File dir, long min) {
      this.dir = dir;
      minimum = min;
    }

    public TestableDiskUsage(File dir) {
      this.dir = dir;
      minimum = 0L;
    }

    public TestableDiskUsage() {
      dir = null;
      minimum = 0L;
    }

    @Override
    protected File dir() {
      return dir;
    }

    @Override
    protected long getMinimumSpace() {
      return minimum;
    }

    @Override
    protected void recordStats(long total, long free, long elapsed) {
      this.total = total;
      this.free = free;
      this.elapsed = elapsed;
    }

    @Override
    protected void handleStateChange(DiskState next, String pct, String criticalMessage) {
      this.next = next;
      this.pct = pct;
      this.criticalMessage = criticalMessage;
    }

    public long getTotal() {
      return total;
    }

    public long getFree() {
      return free;
    }

    public DiskState getNext() {
      return next;
    }

    public String getPct() {
      return pct;
    }

    public String getCriticalMessage() {
      return criticalMessage;
    }
  }
}
