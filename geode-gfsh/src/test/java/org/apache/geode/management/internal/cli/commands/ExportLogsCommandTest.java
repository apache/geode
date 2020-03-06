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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.GIGABYTE;
import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.KILOBYTE;
import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.MEGABYTE;
import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.TERABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.SizeExportLogsFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.BytesToString;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class ExportLogsCommandTest {

  @Test
  public void parseSize_sizeWithUnit_shouldReturnSize() {
    assertThat(ExportLogsCommand.parseSize("1000m")).isEqualTo(1000);
  }

  @Test
  public void parseSize_sizeWithoutUnit_shouldReturnSize() {
    assertThat(ExportLogsCommand.parseSize("1000")).isEqualTo(1000);
  }

  @Test
  public void parseByteMultiplier_sizeWithoutUnit_shouldReturnDefaultUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000")).isEqualTo(MEGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_k_shouldReturnUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000k")).isEqualTo(KILOBYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_m_shouldReturnUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000m")).isEqualTo(MEGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_g_shouldReturnUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000g")).isEqualTo(GIGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_t_shouldReturnUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000t")).isEqualTo(TERABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_K_shouldReturnUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000K")).isEqualTo(KILOBYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_M_shouldReturnUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000M")).isEqualTo(MEGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_G_shouldReturnUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000G")).isEqualTo(GIGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_T_shouldReturnUnit() {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000T")).isEqualTo(TERABYTE);
  }

  @Test
  public void parseByteMultiplier_illegalUnit_shouldThrow() {
    assertThatThrownBy(() -> ExportLogsCommand.parseByteMultiplier("1000q"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void parseSize_garbage_shouldThrow() {
    assertThatThrownBy(() -> ExportLogsCommand.parseSize("bizbap"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void parseByteMultiplier_garbage_shouldThrow() {
    assertThatThrownBy(() -> ExportLogsCommand.parseByteMultiplier("bizbap"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void parseSizeLimit_sizeWithoutUnit_shouldReturnMegabytesSize() {
    ExportLogsCommand exportCmd = new ExportLogsCommand();
    assertThat(exportCmd.parseFileSizeLimit("1000")).isEqualTo(1000 * MEGABYTE);
  }

  @Test
  public void parseSizeLimit_sizeWith_K_shouldReturnKilobytesSize() {
    ExportLogsCommand exportCmd = new ExportLogsCommand();
    assertThat(exportCmd.parseFileSizeLimit("1000k")).isEqualTo(1000 * KILOBYTE);
  }

  @Test
  public void parseSizeLimit_sizeWith_M_shouldReturnMegabytesSize() {
    ExportLogsCommand exportCmd = new ExportLogsCommand();
    assertThat(exportCmd.parseFileSizeLimit("1000m")).isEqualTo(1000 * MEGABYTE);
  }

  @Test
  public void parseSizeLimit_sizeWith_G_shouldReturnMegabytesSize() {
    ExportLogsCommand exportCmd = new ExportLogsCommand();
    assertThat(exportCmd.parseFileSizeLimit("1000g")).isEqualTo(1000 * GIGABYTE);
  }

  @Test
  public void parseSizeLimit_sizeWith_T_shouldReturnMegabytesSize() {
    ExportLogsCommand exportCmd = new ExportLogsCommand();
    assertThat(exportCmd.parseFileSizeLimit("1000t")).isEqualTo(1000 * TERABYTE);
  }

  @Test
  public void testTotalEstimateSizeExceedsLocatorAvailableDisk() throws Exception {
    final InternalCache mockCache = mock(InternalCache.class);
    final InternalCacheForClientAccess mockCacheFilter = mock(InternalCacheForClientAccess.class);
    when(mockCache.getCacheForProcessingClientRequests()).thenReturn(mockCacheFilter);
    final ExportLogsCommand realCmd = new ExportLogsCommand();
    ExportLogsCommand spyCmd = spy(realCmd);

    String start = null;
    String end = null;
    String logLevel = null;
    boolean onlyLogLevel = false;
    boolean logsOnly = false;
    boolean statsOnly = false;

    InternalDistributedMember member1 = new InternalDistributedMember("member1", 12345);
    InternalDistributedMember member2 = new InternalDistributedMember("member2", 98765);
    member1.setName("member1");
    member2.setName("member2");
    Set<DistributedMember> testMembers = new HashSet<>();
    testMembers.add(member1);
    testMembers.add(member2);

    CustomCollector testResults1 = new CustomCollector();
    testResults1.addResult(member1, 75 * MEGABYTE);
    CustomCollector testResults2 = new CustomCollector();
    testResults2.addResult(member2, 60 * MEGABYTE);

    doReturn(mockCache).when(spyCmd).getCache();
    doReturn(testMembers).when(spyCmd).getMembersIncludingLocators(null, null);
    doReturn(testResults1).when(spyCmd)
        .estimateLogSize(any(SizeExportLogsFunction.Args.class), eq(member1));
    doReturn(testResults2).when(spyCmd)
        .estimateLogSize(any(SizeExportLogsFunction.Args.class), eq(member2));
    doReturn(10 * MEGABYTE).when(spyCmd).getLocalDiskAvailable();

    ResultModel res = spyCmd.exportLogs("working dir", null, null, logLevel,
        onlyLogLevel, false, start, end, logsOnly, statsOnly, "125m");
    assertThat(res.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(res.toJson())
        .contains("Estimated logs size will exceed the available disk space on the locator");
  }

  @Test
  public void testTotalEstimateSizeExceedsUserSpecifiedValue() throws Exception {
    final InternalCache mockCache = mock(InternalCache.class);
    final InternalCacheForClientAccess mockCacheFilter = mock(InternalCacheForClientAccess.class);
    when(mockCache.getCacheForProcessingClientRequests()).thenReturn(mockCacheFilter);
    final ExportLogsCommand realCmd = new ExportLogsCommand();
    ExportLogsCommand spyCmd = spy(realCmd);

    String start = null;
    String end = null;
    String logLevel = null;
    boolean onlyLogLevel = false;
    boolean logsOnly = false;
    boolean statsOnly = false;

    InternalDistributedMember member1 = new InternalDistributedMember("member1", 12345);
    InternalDistributedMember member2 = new InternalDistributedMember("member2", 98765);
    member1.setName("member1");
    member2.setName("member2");
    Set<DistributedMember> testMembers = new HashSet<>();
    testMembers.add(member1);
    testMembers.add(member2);

    CustomCollector testResults1 = new CustomCollector();
    testResults1.addResult(member1, 75 * MEGABYTE);
    CustomCollector testResults2 = new CustomCollector();
    testResults2.addResult(member2, 60 * MEGABYTE);

    doReturn(mockCache).when(spyCmd).getCache();
    doReturn(testMembers).when(spyCmd).getMembersIncludingLocators(null, null);
    doReturn(testResults1).when(spyCmd)
        .estimateLogSize(any(SizeExportLogsFunction.Args.class), eq(member1));
    doReturn(testResults2).when(spyCmd)
        .estimateLogSize(any(SizeExportLogsFunction.Args.class), eq(member2));
    doReturn(GIGABYTE).when(spyCmd).getLocalDiskAvailable();

    ResultModel res = spyCmd.exportLogs("working dir", null, null, logLevel,
        onlyLogLevel, false, start, end, logsOnly, statsOnly, "125m");
    assertThat(res.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(res.toJson()).contains(
        "Estimated exported logs expanded file size = 141557760, file-size-limit = 131072000");
  }

  @Test
  public void estimateLogSizeExceedsServerDisk() throws Exception {
    final InternalCache mockCache = mock(InternalCache.class);
    final InternalCacheForClientAccess mockCacheFilter = mock(InternalCacheForClientAccess.class);
    when(mockCache.getCacheForProcessingClientRequests()).thenReturn(mockCacheFilter);
    final ExportLogsCommand realCmd = new ExportLogsCommand();
    ExportLogsCommand spyCmd = spy(realCmd);

    String start = null;
    String end = null;
    String logLevel = null;
    boolean onlyLogLevel = false;
    boolean logsOnly = false;
    boolean statsOnly = false;

    InternalDistributedMember member1 = new InternalDistributedMember("member1", 12345);
    member1.setName("member1");
    Set<DistributedMember> testMembers = new HashSet<>();
    testMembers.add(member1);

    BytesToString bytesToString = new BytesToString();
    CustomCollector testResults1 = new CustomCollector();
    String sb = "Estimated disk space required (" +
        bytesToString.of(GIGABYTE) + ") to consolidate logs on member " +
        member1.getName() + " will exceed available disk space (" +
        bytesToString.of(500 * MEGABYTE) + ")";
    testResults1.addResult(member1, new ManagementException(sb));

    doReturn(mockCache).when(spyCmd).getCache();
    doReturn(testMembers).when(spyCmd).getMembersIncludingLocators(null, null);
    doReturn(testResults1).when(spyCmd)
        .estimateLogSize(any(SizeExportLogsFunction.Args.class), eq(member1));

    ResultModel res = spyCmd.exportLogs("working dir", null, null, logLevel,
        onlyLogLevel, false, start, end, logsOnly, statsOnly, "125m");
    assertThat(res.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(res.toJson()).contains(
        "Estimated disk space required (1 GB) to consolidate logs on member member1 will exceed available disk space (500 MB)");
  }

  private static class CustomCollector implements ResultCollector<Object, List<Object>> {

    private final ArrayList<Object> results = new ArrayList<>();

    @Override
    public List<Object> getResult() throws FunctionException {
      return results;
    }

    @Override
    public List<Object> getResult(final long timeout, final TimeUnit unit)
        throws FunctionException {
      return results;
    }

    @Override
    public void addResult(final DistributedMember memberID, final Object resultOfSingleExecution) {
      results.add(resultOfSingleExecution);
    }

    @Override
    public void endResults() {}

    @Override
    public void clearResults() {
      results.clear();
    }
  }
}
