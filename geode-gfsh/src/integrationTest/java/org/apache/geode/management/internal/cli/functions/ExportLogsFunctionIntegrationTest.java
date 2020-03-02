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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({GfshTest.class, LoggingTest.class})
public class ExportLogsFunctionIntegrationTest {

  private File serverWorkingDir;

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule().withAutoStart();

  @Before
  public void setup() throws Exception {
    serverWorkingDir = serverStarterRule.getWorkingDir();
  }

  @Test
  public void exportLogsFunctionDoesNotBlowUp() throws Throwable {
    File logFile1 = new File(serverWorkingDir, "server1.log");
    FileUtils.writeStringToFile(logFile1, "some log for server1 \n some other log line");
    File logFile2 = new File(serverWorkingDir, "server2.log");
    FileUtils.writeStringToFile(logFile2, "some log for server2 \n some other log line");

    File notALogFile = new File(serverWorkingDir, "foo.txt");
    FileUtils.writeStringToFile(notALogFile, "some text");

    verifyExportLogsFunctionDoesNotBlowUp(serverStarterRule.getCache());

    Cache cache = serverStarterRule.getCache();
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isEmpty();
  }

  @Test
  public void createOrGetExistingExportLogsRegionDoesNotBlowUp() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    ExportLogsFunction.createOrGetExistingExportLogsRegion(false, cache);
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNotNull();
  }

  @Test
  public void destroyExportLogsRegionWorksAsExpectedForInitiatingMember() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    ExportLogsFunction.createOrGetExistingExportLogsRegion(true, cache);
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNotNull();

    ExportLogsFunction.destroyExportLogsRegion(cache);
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNull();
  }

  private static void verifyExportLogsFunctionDoesNotBlowUp(Cache cache) throws Throwable {
    ExportLogsFunction.Args args =
        new ExportLogsFunction.Args(null, null, "info", false, false, false);
    CapturingResultSender resultSender = new CapturingResultSender();
    FunctionContext context = new FunctionContextImpl(cache, "functionId", args, resultSender);
    new ExportLogsFunction().execute(context);
    if (resultSender.getThrowable() != null) {
      throw resultSender.getThrowable();
    }
  }

  private static class CapturingResultSender implements ResultSender {
    private Throwable throwable;

    public Throwable getThrowable() {
      return throwable;
    }

    @Override
    public void sendResult(Object oneResult) {
      // nothing
    }

    @Override
    public void lastResult(Object lastResult) {
      // nothing
    }

    @Override
    public void sendException(Throwable t) {
      throwable = t;
    }
  }
}
