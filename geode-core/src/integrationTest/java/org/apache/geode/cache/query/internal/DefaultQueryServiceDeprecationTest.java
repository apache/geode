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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY;
import static org.apache.geode.distributed.internal.DistributionConfig.LOG_FILE_NAME;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class DefaultQueryServiceDeprecationTest implements Serializable {

  @ClassRule
  public static TemporaryFolder folderRule = new TemporaryFolder();

  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  @Test
  @SuppressWarnings("deprecation")
  public void warningMessageIsOnlyLoggedOnceWhenDeprecatedPropertyUsed() throws IOException {
    File logFile = folderRule.newFile("customLog1.log");
    System.setProperty(ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY, "true");
    server.withProperty(LOG_FILE_NAME, logFile.getAbsolutePath()).startServer();
    server.getCache().getQueryService();
    server.getCache().getQueryService();
    LogFileAssert.assertThat(logFile)
        .containsOnlyOnce(QueryConfigurationServiceImpl.DEPRECATION_WARNING);
  }

  @Test
  public void warningMessageIsNotLoggedWhenDeprecatedPropertyIsNotUsed() throws IOException {
    File logFile = folderRule.newFile("customLog2.log");
    server.withProperty(LOG_FILE_NAME, logFile.getAbsolutePath()).startServer();
    server.getCache().getQueryService();
    LogFileAssert.assertThat(logFile)
        .doesNotContain(QueryConfigurationServiceImpl.DEPRECATION_WARNING);
  }
}
