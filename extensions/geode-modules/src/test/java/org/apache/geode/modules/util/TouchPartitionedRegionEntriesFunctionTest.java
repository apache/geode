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
package org.apache.geode.modules.util;

import static java.util.Collections.addAll;
import static java.util.Collections.emptySet;
import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;
import static org.assertj.core.internal.bytebuddy.matcher.ElementMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;

public class TouchPartitionedRegionEntriesFunctionTest {

  private RegionFunctionContext context;
  private Region primaryDataSet;
  private ResultSender resultSender;

  private TouchPartitionedRegionEntriesFunction touchPartitionedRegionEntriesFunction;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    context = mock(RegionFunctionContext.class);
    primaryDataSet = mock(Region.class);
    resultSender = mock(ResultSender.class);

    when(cast(context.getResultSender())).thenReturn(cast(resultSender));

    touchPartitionedRegionEntriesFunction = spy(new TouchPartitionedRegionEntriesFunction());

    doReturn(primaryDataSet)
        .when(touchPartitionedRegionEntriesFunction)
        .getLocalDataForContextViaRegionHelper(context);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContext() {
    doReturn(emptySet()).when(context).getFilter();

    touchPartitionedRegionEntriesFunction.execute(context);

    verify(primaryDataSet, never()).get(any());
    verify(resultSender).lastResult(true);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContextAndMultipleKeys() {
    Set<String> keys = set("Key1", "Key2");
    doReturn(keys).when(context).getFilter();

    touchPartitionedRegionEntriesFunction.execute(context);

    verify(primaryDataSet, times(keys.size())).get(anyString());
    verify(resultSender).lastResult(true);
  }

  private static Set<String> set(String... values) {
    Set<String> set = new HashSet<>();
    addAll(set, values);
    return set;
  }
}
