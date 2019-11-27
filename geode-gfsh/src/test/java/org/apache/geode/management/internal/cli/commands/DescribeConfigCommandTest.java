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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.domain.MemberConfigurationInfo;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class DescribeConfigCommandTest {

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  private DescribeConfigCommand command;

  @Before
  public void setUp() throws Exception {
    command = spy(DescribeConfigCommand.class);
  }

  @Test
  public void describeConfigWithoutMemberName() throws Exception {
    assertThat(parser.parse("describe config")).isNull();
  }

  @Test
  public void passwordShouldBeRedacted() {
    MemberConfigurationInfo info = new MemberConfigurationInfo();
    Map<String, String> properties = new HashMap<>();
    properties.put(ConfigurationProperties.SSL_KEYSTORE, "somewhere/something");
    properties.put(ConfigurationProperties.SSL_KEYSTORE_PASSWORD, "mySecretPassword");

    info.setGfePropsSetFromFile(properties);
    CliFunctionResult functionResult = mock(CliFunctionResult.class);
    when(functionResult.getResultObject()).thenReturn(info);
    doReturn(mock(DistributedMember.class)).when(command).getMember(any());
    doReturn(functionResult).when(command).executeFunctionAndGetFunctionResult(any(), any(), any());

    parser.executeAndAssertThat(command, "describe config --member=test").statusIsSuccess()
        .hasDataSection("file-properties")
        .hasContent()
        .doesNotContainValue("mySecretPassword")
        .containsValue("********");
  }
}
