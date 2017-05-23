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

import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.SizeExportLogsFunction;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Category(UnitTest.class)
public class ExportLogsCommandTest {

  @Test
  public void parseSize_sizeWithUnit_shouldReturnSize() throws Exception {
    assertThat(ExportLogsCommand.parseSize("1000m")).isEqualTo(1000);
  }

  @Test
  public void parseSize_sizeWithoutUnit_shouldReturnSize() throws Exception {
    assertThat(ExportLogsCommand.parseSize("1000")).isEqualTo(1000);
  }

  @Test
  public void parseByteMultiplier_sizeWithoutUnit_shouldReturnDefaultUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000")).isEqualTo(MEGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_k_shouldReturnUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000k")).isEqualTo(KILOBYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_m_shouldReturnUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000m")).isEqualTo(MEGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_g_shouldReturnUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000g")).isEqualTo(GIGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_t_shouldReturnUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000t")).isEqualTo(TERABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_K_shouldReturnUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000K")).isEqualTo(KILOBYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_M_shouldReturnUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000M")).isEqualTo(MEGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_G_shouldReturnUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000G")).isEqualTo(GIGABYTE);
  }

  @Test
  public void parseByteMultiplier_sizeWith_T_shouldReturnUnit() throws Exception {
    assertThat(ExportLogsCommand.parseByteMultiplier("1000T")).isEqualTo(TERABYTE);
  }

  @Test
  public void parseByteMultiplier_illegalUnit_shouldThrow() throws Exception {
    assertThatThrownBy(() -> ExportLogsCommand.parseByteMultiplier("1000q"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void parseSize_garbage_shouldThrow() throws Exception {
    assertThatThrownBy(() -> ExportLogsCommand.parseSize("bizbap"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void parseByteMultiplier_garbage_shouldThrow() throws Exception {
    assertThatThrownBy(() -> ExportLogsCommand.parseByteMultiplier("bizbap"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void sizeTooBigOnMember_sizeChecksDisabled_returnsFalse() throws Exception {
    final Cache mockCache = mock(Cache.class);
    final DistributedMember mockDistributedMember = mock(DistributedMember.class);
    final Execution mockFunctionExecutor = mock(Execution.class);
    final ExportLogsCommand cmd =
        createExportLogsCommand(mockCache, mockDistributedMember, mockFunctionExecutor);
    cmd.checkIfExportLogsOverflowsDisk("clusterMember", 0, MEGABYTE + 1024, MEGABYTE);
  }

  @Test
  public void sizeOKOnMember_sizeChecksEnabled_doesNotThrow() throws Exception {
    final Cache mockCache = mock(Cache.class);
    final DistributedMember mockDistributedMember = mock(DistributedMember.class);
    final Execution mockFunctionExecutor = mock(Execution.class);
    final ExportLogsCommand cmd =
        createExportLogsCommand(mockCache, mockDistributedMember, mockFunctionExecutor);
    cmd.checkIfExportLogsOverflowsDisk("clusterMember", 10 * MEGABYTE, MEGABYTE - 1024, MEGABYTE);
  }

  @Test
  public void sizeTooBigOnMember_sizeChecksEnabled_shouldThrow() throws Exception {
    final Cache mockCache = mock(Cache.class);
    final DistributedMember mockDistributedMember = mock(DistributedMember.class);
    final Execution mockFunctionExecutor = mock(Execution.class);
    final ExportLogsCommand cmd =
        createExportLogsCommand(mockCache, mockDistributedMember, mockFunctionExecutor);
    assertThatThrownBy(() -> cmd.checkIfExportLogsOverflowsDisk("clusterMember", 10 * MEGABYTE,
        MEGABYTE + 1024, MEGABYTE)).isInstanceOf(ManagementException.class);
  }

  @Test
  public void sizeFromAllMembers_greaterThanLocalDiskAvailable_shouldReturnErrorResult()
      throws Exception {
    final InternalCache mockCache = mock(InternalCache.class);
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
    member1.getNetMember().setName("member1");
    member2.getNetMember().setName("member2");
    Set<DistributedMember> testMembers = new HashSet<>();
    testMembers.add(member1);
    testMembers.add(member2);

    ResultCollector testResults1 = new CustomCollector();
    testResults1.addResult(member1, Arrays.asList(75 * MEGABYTE));
    ResultCollector testResults2 = new CustomCollector();
    testResults2.addResult(member2, Arrays.asList(60 * MEGABYTE));

    doReturn(mockCache).when(spyCmd).getCache();
    doReturn(testMembers).when(spyCmd).getMembers(null, null);
    doReturn(testResults1).when(spyCmd)
        .estimateLogSize(Matchers.any(SizeExportLogsFunction.Args.class), eq(member1));
    doReturn(testResults2).when(spyCmd)
        .estimateLogSize(Matchers.any(SizeExportLogsFunction.Args.class), eq(member2));
    doReturn(125 * MEGABYTE).when(spyCmd).getLocalDiskAvailable();
    doReturn(GIGABYTE).when(spyCmd).getLocalDiskSize();

    Result res = spyCmd.exportLogs("working dir", null, null, logLevel, onlyLogLevel, false, start,
        end, logsOnly, statsOnly, "125m");
    assertThat(res.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  private ExportLogsCommand createExportLogsCommand(final Cache cache,
      final DistributedMember distributedMember, final Execution functionExecutor) {
    return new TestExportLogsCommand(cache, distributedMember, functionExecutor);
  }

  private static class TestExportLogsCommand extends ExportLogsCommand {

    private final Cache cache;
    private final DistributedMember distributedMember;
    private final Execution functionExecutor;

    TestExportLogsCommand(final Cache cache, final DistributedMember distributedMember,
        final Execution functionExecutor) {
      assert cache != null;
      this.cache = cache;
      this.distributedMember = distributedMember;
      this.functionExecutor = functionExecutor;
    }
  }

  public static class CustomCollector implements ResultCollector<Object, List<Object>> {
    private ArrayList<Object> results = new ArrayList<>();

    @Override
    public List<Object> getResult() throws FunctionException {
      return results;
    }

    @Override
    public List<Object> getResult(final long timeout, final TimeUnit unit)
        throws FunctionException, InterruptedException {
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
