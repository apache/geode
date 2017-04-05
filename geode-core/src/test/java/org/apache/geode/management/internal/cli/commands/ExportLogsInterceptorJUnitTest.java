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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

@Category(UnitTest.class)
public class ExportLogsInterceptorJUnitTest {
  private ExportLogsInterceptor interceptor;
  private GfshParseResult parseResult;
  private Map<String, String> arguments;
  private Result result;

  @Before
  public void before() {
    interceptor = new ExportLogsInterceptor();
    parseResult = Mockito.mock(GfshParseResult.class);
    arguments = new HashMap<>();
    arguments.put("log-level", "info");
    when(parseResult.getParamValueStrings()).thenReturn(arguments);
  }

  @Test
  public void testGroupAndMember() {
    arguments.put("group", "group");
    arguments.put("member", "member");
    result = interceptor.preExecution(parseResult);
    assertThat(result.nextLine()).contains("Can't specify both group and member");
  }

  @Test
  public void testStartEnd() {
    arguments.put("start-time", "2000/01/01");
    arguments.put("end-time", "2000/01/02");
    result = interceptor.preExecution(parseResult);
    assertThat(result.nextLine()).isEmpty();

    arguments.put("start-time", "2000/01/02");
    arguments.put("end-time", "2000/01/01");
    result = interceptor.preExecution(parseResult);
    assertThat(result.nextLine()).contains("start-time has to be earlier than end-time");
  }

  @Test
  public void testInclideStats() {
    arguments.put("logs-only", "true");
    arguments.put("stats-only", "false");
    result = interceptor.preExecution(parseResult);
    assertThat(result.nextLine()).isEmpty();

    arguments.put("logs-only", "true");
    arguments.put("stats-only", "true");
    result = interceptor.preExecution(parseResult);
    assertThat(result.nextLine()).contains("logs-only and stats-only can't both be true");

  }
}
