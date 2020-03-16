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
package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.management.ManagementException;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({GfshTest.class, LoggingTest.class})
public class SizeExportLogsFunctionIntegrationTest {

  private SizeExportLogsFunction.Args nonFilteringArgs;
  private File logFile;
  private File statFile;
  private Properties config;
  private TestResultSender resultSender;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  @Before
  public void before() {
    String name = testName.getMethodName();
    config = new Properties();
    config.setProperty(NAME, name);

    File dir = temporaryFolder.getRoot();
    logFile = new File(dir, name + ".log");
    statFile = new File(dir, name + ".gfs");

    resultSender = new TestResultSender();
    nonFilteringArgs = new SizeExportLogsFunction.Args(null, null, null, false, false, false);
  }

  @Test
  public void withFiles_returnsCombinedSizeResult() throws Throwable {
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());
    config.setProperty(STATISTIC_ARCHIVE_FILE, statFile.getAbsolutePath());

    server.withProperties(config).startServer();

    @SuppressWarnings("unchecked")
    FunctionContext<ExportLogsFunction.Args> context =
        new FunctionContextImpl(server.getCache(), "functionId", nonFilteringArgs, resultSender);

    // log and stat files sizes are not constant with a real cache running, so check for the sizer
    // estimate within a range
    long initialFileSizes = FileUtils.sizeOf(logFile) + FileUtils.sizeOf(statFile);
    new SizeExportLogsFunction().execute(context);
    long finalFileSizes = FileUtils.sizeOf(logFile) + FileUtils.sizeOf(statFile);
    getAndVerifySizeEstimate(resultSender, initialFileSizes, finalFileSizes);
  }

  @Test
  public void noFiles_returnsZeroResult() throws Throwable {
    config.setProperty(LOG_FILE, "");
    config.setProperty(STATISTIC_ARCHIVE_FILE, "");

    server.withProperties(config).startServer();

    @SuppressWarnings("unchecked")
    FunctionContext<ExportLogsFunction.Args> context =
        new FunctionContextImpl(server.getCache(), "functionId", nonFilteringArgs, resultSender);
    new SizeExportLogsFunction().execute(context);
    getAndVerifySizeEstimate(resultSender, 0L);
  }

  @Test
  public void withFunctionError_shouldThrow() {
    server.withProperties(config).startServer();

    @SuppressWarnings("unchecked")
    FunctionContext<ExportLogsFunction.Args> context =
        new FunctionContextImpl(server.getCache(), "functionId", null, resultSender);
    new SizeExportLogsFunction().execute(context);
    assertThatThrownBy(resultSender::getResults).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void sizeGreaterThanDiskAvailable_sendsErrorResult() {
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());
    config.setProperty(STATISTIC_ARCHIVE_FILE, statFile.getAbsolutePath());
    server.withProperties(config).startServer();

    @SuppressWarnings("unchecked")
    FunctionContext<ExportLogsFunction.Args> context =
        new FunctionContextImpl(server.getCache(), "functionId", nonFilteringArgs, resultSender);
    SizeExportLogsFunction testFunction = new SizeExportLogsFunction();
    SizeExportLogsFunction spyFunction = spy(testFunction);
    long fakeDiskAvailable = 1024;
    doReturn(fakeDiskAvailable).when(spyFunction)
        .getDiskAvailable(any(DistributionConfig.class));

    spyFunction.execute(context);
    assertThatThrownBy(resultSender::getResults).isInstanceOf(ManagementException.class);
  }

  private void getAndVerifySizeEstimate(TestResultSender resultSender, long expectedSize)
      throws Throwable {
    getAndVerifySizeEstimate(resultSender, expectedSize, expectedSize);
  }

  private void getAndVerifySizeEstimate(TestResultSender resultSender, long minExpected,
      long maxExpected) throws Throwable {
    List<?> results = resultSender.getResults();

    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);
    Object result = results.get(0);
    assertThat(result).isNotNull();
    assertThat(result).isInstanceOf(Long.class);
    if (minExpected == maxExpected) {
      assertThat(((Long) result)).isEqualTo(minExpected);
    }
    assertThat(((Long) result)).isGreaterThanOrEqualTo(minExpected)
        .isLessThanOrEqualTo(maxExpected);
  }

  private static class TestResultSender implements ResultSender<Object> {

    private final List<Object> results = new LinkedList<>();

    private Throwable throwable;

    List<Object> getResults() throws Throwable {
      if (throwable != null) {
        throw throwable;
      }
      return Collections.unmodifiableList(results);
    }

    @Override
    public void lastResult(final Object lastResult) {
      results.add(lastResult);
    }

    @Override
    public void sendResult(final Object oneResult) {
      results.add(oneResult);
    }

    @Override
    public void sendException(final Throwable t) {
      throwable = t;
    }
  }
}
