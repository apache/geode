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

package org.apache.geode.management.internal.cli.remote;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OnlineCommandProcessorTest {

  Properties properties;
  SecurityService securityService;
  CommandExecutor executor;
  OnlineCommandProcessor onlineCommandProcessor;
  Result result;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() {
    properties = new Properties();
    securityService = mock(SecurityService.class);
    executor = mock(CommandExecutor.class);
    result = mock(Result.class);
    when(executor.execute(any())).thenReturn(result);

    onlineCommandProcessor = new OnlineCommandProcessor(properties, securityService, executor);
  }


  @Test
  public void executeWithNullThrowsNPE() throws Exception {
    assertThatThrownBy(() -> onlineCommandProcessor.executeCommand(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void executeWithEmpty() throws Exception {
    assertThat(onlineCommandProcessor.executeCommand("")).isNull();
  }

  @Test
  public void executeStripsComments() throws Exception {
    Result commandResult = onlineCommandProcessor.executeCommand("/*comment*/");
    assertThat(commandResult).isNull();
  }

  @Test
  public void executeReturnsExecutorResult() throws Exception {
    Result commandResult = onlineCommandProcessor.executeCommand("start locator");
    assertThat(commandResult).isSameAs(result);
  }

  @Test
  public void handlesNotAuthorizedException() throws Exception {
    when(executor.execute(any())).thenThrow(new NotAuthorizedException("not authorized"));
    assertThatThrownBy(() -> onlineCommandProcessor.executeCommand("start locator"))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void handlesParsingError() throws Exception {
    Result commandResult = onlineCommandProcessor.executeCommand("foo --bar");
    assertThat(commandResult).isInstanceOf(CommandResult.class);
    assertThat(commandResult.toString()).contains("Could not parse command string. foo --bar");
  }
}
