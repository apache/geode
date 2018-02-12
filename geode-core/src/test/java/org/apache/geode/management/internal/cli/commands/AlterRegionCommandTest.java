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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.domain.ClassName;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;


@Category(UnitTest.class)
public class AlterRegionCommandTest {

  @Rule
  public GfshParserRule parser = new GfshParserRule();

  private AlterRegionCommand command;
  private InternalCache cache;

  @Before
  public void before() throws Exception {
    command = spy(AlterRegionCommand.class);
    cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();
  }

  @Test
  public void cacheWriterEmpty() throws Exception {
    String command = "alter region --name=/Person --cache-writer='' --cache-loader=' '";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValue("cache-writer")).isEqualTo(ClassName.EMPTY);
    assertThat(result.getParamValue("cache-listener")).isNull();
    assertThat(result.getParamValue("cache-loader")).isEqualTo(ClassName.EMPTY);
    assertThat(result.getParamValue("entry-idle-time-custom-expiry")).isNull();
  }

  @Test
  public void cacheWriterInvalid() throws Exception {
    String command = "alter region --name=/Person --cache-writer='1abc'";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNull();
  }

  @Test
  public void emptyCustomExpiry() {
    String command = "alter region --name=/Person --entry-idle-time-custom-expiry=''";
    GfshParseResult result = parser.parse(command);
    assertThat(result.getParamValue("entry-idle-time-custom-expiry")).isEqualTo(ClassName.EMPTY);
  }
}
