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
 *
 */

package org.apache.geode.management.internal.cli.functions;


import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.io.FileUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

@Category(IntegrationTest.class)
public class ExportLogsFunctionIntegrationTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule();

  private File serverWorkingDir;

  @Before
  public void setup() throws Exception {
    serverWorkingDir = temporaryFolder.newFolder("serverWorkingDir");
    System.setProperty("user.dir", serverWorkingDir.getCanonicalPath());

    serverStarterRule.startServer();
  }

  @After
  public void teardown() {
    serverStarterRule.after();
  }

  @Test
  public void exportLogsFunctionDoesNotBlowUp() throws Throwable {
    File logFile1 = new File(serverWorkingDir, "server1.log");
    FileUtils.writeStringToFile(logFile1, "some log for server1 \n some other log line");
    File logFile2 = new File(serverWorkingDir, "server2.log");
    FileUtils.writeStringToFile(logFile2, "some log for server2 \n some other log line");

    File notALogFile = new File(serverWorkingDir, "foo.txt");
    FileUtils.writeStringToFile(notALogFile, "some text");

    ExportLogsFunction.Args args = new ExportLogsFunction.Args(null, null, "info", false);

    CapturingResultSender resultSender = new CapturingResultSender();
    FunctionContext context = new FunctionContextImpl("functionId", args, resultSender);

    new ExportLogsFunction().execute(context);

    if (resultSender.getThrowable() != null) {
      throw resultSender.getThrowable();
    }

    Cache cache = GemFireCacheImpl.getInstance();
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNull();
  }

  @Test
  public void createOrGetExistingExportLogsRegionDoesNotBlowUp() throws Exception {
    ExportLogsFunction.createOrGetExistingExportLogsRegion(false);

    Cache cache = GemFireCacheImpl.getInstance();
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNotNull();
  }

  @Test
  public void destroyExportLogsRegionWorksAsExpectedForInitiatingMember() throws IOException, ClassNotFoundException {
    ExportLogsFunction.createOrGetExistingExportLogsRegion(true);
    Cache cache = GemFireCacheImpl.getInstance();
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNotNull();

    ExportLogsFunction.destroyExportLogsRegion();
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNull();
  }


  @Test
  public void argsCorrectlyBuildALogLevelFilter() {
    ExportLogsFunction.Args args = new ExportLogsFunction.Args(null, null, "info", false);

    assertThat(args.getPermittedLogLevels()).contains("info");
    assertThat(args.getPermittedLogLevels()).contains("error");
    assertThat(args.getPermittedLogLevels()).doesNotContain("fine");
  }

  private static class CapturingResultSender implements ResultSender {
    private Throwable t;

    public Throwable getThrowable() {
      return t;
    }

    @Override
    public void sendResult(Object oneResult) {

    }

    @Override
    public void lastResult(Object lastResult) {

    }

    @Override
    public void sendException(Throwable t) {
      this.t = t;
    }
  }
}
