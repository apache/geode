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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(OQLQueryTest.class)
public class DefaultQueryServiceIntegrationTest {
  private File logFile;
  private InternalCache spiedCache;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");
    server.withProperty("log-file", logFile.getAbsolutePath())
        .withRegion(RegionShortcut.LOCAL, testName.getMethodName())
        .startServer();

    spiedCache = spy(server.getCache());
  }

  @Test
  public void constructorShouldThrowExceptionWhenCacheIsNull() {
    assertThatThrownBy(() -> new DefaultQueryService(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cache must not be null");
  }

  @Test
  public void constructorShouldLogWarningWhenMethodAuthorizerIsNull() {
    QueryConfigurationService mockService = mock(QueryConfigurationService.class);
    when(mockService.getMethodAuthorizer()).thenReturn(null);
    when(spiedCache.getService(QueryConfigurationService.class)).thenReturn(mockService);

    assertThatCode(() -> new DefaultQueryService(spiedCache)).doesNotThrowAnyException();
    LogFileAssert.assertThat(logFile).contains(
        "MethodInvocationAuthorizer returned by the QueryConfigurationService is null, problems might arise if there are queries using method invocations.");
  }
}
