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

package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.management.internal.cli.commands.IndexDefinition;

public class ManageIndexDefinitionFunctionTest {
  private ManageIndexDefinitionFunction function;
  private FunctionContext<RegionConfig.Index> context;

  @Before
  @SuppressWarnings({"unchecked"})
  public void before() {
    function = new ManageIndexDefinitionFunction();
    context = mock(FunctionContext.class);
    IndexDefinition.indexDefinitions.clear();
  }

  @After
  public void after() {
    IndexDefinition.indexDefinitions.clear();
  }

  @Test
  public void addToIndexDefinition() throws Exception {
    RegionConfig.Index index1 = new RegionConfig.Index();
    when(context.getArguments()).thenReturn(index1);
    function.executeFunction(context);
    assertThat(IndexDefinition.indexDefinitions).containsExactly(index1);

    RegionConfig.Index index2 = new RegionConfig.Index();
    when(context.getArguments()).thenReturn(index2);
    function.executeFunction(context);
    assertThat(IndexDefinition.indexDefinitions)
        .containsExactlyInAnyOrder(index1, index2);
  }

  @Test
  public void clearIndexWhenArgumentIsNull() throws Exception {
    IndexDefinition.indexDefinitions.add(new RegionConfig.Index());
    when(context.getArguments()).thenReturn(null);
    function.executeFunction(context);
    assertThat(IndexDefinition.indexDefinitions).isEmpty();
  }
}
