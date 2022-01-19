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
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.test.junit.rules.GfshParserRule;
import org.apache.geode.test.junit.rules.GfshParserRule.CommandCandidate;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class BaseStringConverterJUnitTest {

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  private static final String[] allMemberNames = {"candidate1", "candidate2"};

  private BaseStringConverter converter;

  @Parameterized.Parameter(0)
  public Class<BaseStringConverter> converterClass;

  @Parameterized.Parameter(1)
  public String gfshCommand;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {{MemberGroupConverter.class, "start server --group="},
        {ClusterMemberIdNameConverter.class, "describe member --name="},
        {MemberIdNameConverter.class, "status server --name="},
        {LocatorIdNameConverter.class, "status locator --name="},
        {LocatorDiscoveryConfigConverter.class, "start server --locators="},
        {GatewaySenderIdConverter.class, "start gateway-sender --id="},});
  }

  @Before
  public void before() {
    // this will let the parser use the spied converter instead of creating its own
    converter = parser.spyConverter(converterClass);
    when(converter.getCompletionValues())
        .thenReturn(Arrays.stream(allMemberNames).collect(Collectors.toSet()));
  }

  @Test
  public void convert() throws Exception {
    assertThat(converter.convertFromText("value123", String.class, "")).isEqualTo("value123");
  }

  @Test
  public void complete() throws Exception {
    CommandCandidate candidate = parser.complete(gfshCommand);
    assertThat(candidate.size()).isEqualTo(allMemberNames.length);
    assertThat(candidate.getFirstCandidate()).isEqualTo(gfshCommand + allMemberNames[0]);
  }
}
