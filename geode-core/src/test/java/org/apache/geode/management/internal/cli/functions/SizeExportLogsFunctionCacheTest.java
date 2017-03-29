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

import static org.assertj.core.api.Assertions.*;
import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.io.FileUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

@Category(IntegrationTest.class)
public class SizeExportLogsFunctionCacheTest {

  private Cache cache;

  private SizeExportLogsFunction.Args nonFilteringArgs;
  private TestResultSender resultSender;
  private FunctionContext functionContext;
  private File dir;
  private DistributedMember member;
  File logFile;
  File statFile;
  String name;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void before() throws Throwable {
    name = testName.getMethodName();

    this.dir = this.temporaryFolder.getRoot();
    logFile = new File(dir, name + ".log");
    statFile = new File(dir, name + ".gfs");

    this.nonFilteringArgs = new ExportLogsFunction.Args(null, null, null, false, false, false);
    functionContext = new FunctionContextImpl("functionId", nonFilteringArgs, resultSender);

  }

  @After
  public void after() throws Exception {
    if (this.cache != null) {
      this.cache.close();
    }
    FileUtils.deleteDirectory(dir);
  }

  @Test
  public void withFiles_returnsCombinedSizeResult() throws Throwable {
    Properties config = new Properties();
    config.setProperty(NAME, name);
    config.setProperty(LOCATORS, "");
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());
    config.setProperty(STATISTIC_ARCHIVE_FILE, statFile.getAbsolutePath());

    this.cache = new CacheFactory(config).create();
    TestResultSender resultSender = new TestResultSender();
    FunctionContext context = new FunctionContextImpl("functionId", nonFilteringArgs, resultSender);

    // log and stat files sizes are not constant with a real cache running, so check for the sizer
    // estimate within a range
    long initalFileSizes = FileUtils.sizeOf(logFile) + FileUtils.sizeOf(statFile);
    new SizeExportLogsFunction().execute(context);
    long finalFileSizes = FileUtils.sizeOf(logFile) + FileUtils.sizeOf(statFile);
    getAndVerifySizeEstimate(resultSender, initalFileSizes, finalFileSizes);
  }

  @Test
  public void noFiles_returnsZeroResult() throws Throwable {
    Properties config = new Properties();
    config.setProperty(NAME, name);
    config.setProperty(LOCATORS, "");
    config.setProperty(MCAST_PORT, "0");

    this.cache = new CacheFactory(config).create();

    TestResultSender resultSender = new TestResultSender();
    FunctionContext context = new FunctionContextImpl("functionId", nonFilteringArgs, resultSender);

    new SizeExportLogsFunction().execute(context);
    getAndVerifySizeEstimate(resultSender, 0L);
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
    List<?> result = (List<?>) results.get(0);
    assertThat(result).isNotNull();
    assertThat(((ExportedLogsSizeInfo) result.get(0)).getLogsSize())
        .isGreaterThanOrEqualTo(minExpected).isLessThanOrEqualTo(maxExpected);
  }

  @Test
  public void withFunctionError_shouldThrow() throws Throwable {
    Properties config = new Properties();
    config.setProperty(NAME, name);
    config.setProperty(LOCATORS, "");
    config.setProperty(MCAST_PORT, "0");

    this.cache = new CacheFactory().create();

    TestResultSender resultSender = new TestResultSender();
    FunctionContext context = new FunctionContextImpl("functionId", null, resultSender);

    new SizeExportLogsFunction().execute(context);
    assertThatThrownBy(() -> resultSender.getResults()).isInstanceOf(NullPointerException.class);
  }

  private static class TestResultSender implements ResultSender {

    private final List<Object> results = new LinkedList<Object>();

    private Throwable t;

    protected List<Object> getResults() throws Throwable {
      if (t != null) {
        throw t;
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
      this.t = t;
    }
  }
}
