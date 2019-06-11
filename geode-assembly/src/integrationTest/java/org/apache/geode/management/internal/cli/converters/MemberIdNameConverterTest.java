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

package org.apache.geode.management.internal.cli.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class MemberIdNameConverterTest {
  @ClassRule
  public static LocatorStarterRule locator =
      new LocatorStarterRule().withHttpService().withAutoStart();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private MemberIdNameConverter converter;

  @Before
  public void name() throws Exception {
    converter = spy(MemberIdNameConverter.class);
    doReturn(gfsh.getGfsh()).when(converter).getGfsh();
  }

  @Test
  public void completeMemberWhenConnectedWithJmx() throws Exception {
    gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    Set<String> values = converter.getCompletionValues();
    assertThat(values).hasSize(0);
    gfsh.disconnect();
  }

  @Test
  public void completeMembersWhenConnectedWithHttp() throws Exception {
    gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    Set<String> values = converter.getCompletionValues();
    assertThat(values).hasSize(0);
    gfsh.disconnect();
  }
}
