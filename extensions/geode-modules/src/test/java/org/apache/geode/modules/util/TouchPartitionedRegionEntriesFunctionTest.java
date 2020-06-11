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


import static org.assertj.core.internal.bytebuddy.matcher.ElementMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;

public class TouchPartitionedRegionEntriesFunctionTest {

  private final TouchPartitionedRegionEntriesFunction function =
      spy(new TouchPartitionedRegionEntriesFunction());
  private final FunctionContext context = mock(RegionFunctionContext.class);
  private final Cache cache = mock(Cache.class);
  private final LogWriter logger = mock(LogWriter.class);
  private final Region primaryDataSet = mock(Region.class);
  private final ResultSender resultSender = mock(ResultSender.class);

  @Before
  public void setUp() {
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(cache.getLogger()).thenReturn(logger);
    when(logger.fineEnabled()).thenReturn(false);
    doReturn(primaryDataSet).when(function)
        .getLocalDataForContextViaRegionHelper((RegionFunctionContext) context);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContext() {
    doReturn(new HashSet() {}).when((RegionFunctionContext) context).getFilter();

    function.execute(context);

    verify(primaryDataSet, times(0)).get(any());
    verify(resultSender).lastResult(true);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContextAndMultipleKeys() {
    final HashSet<String> keys = new HashSet();
    keys.add("Key1");
    keys.add("Key2");

    doReturn(keys).when((RegionFunctionContext) context).getFilter();

    function.execute(context);

    verify(primaryDataSet, times(keys.size())).get(anyString());
    verify(resultSender).lastResult(true);
  }
}
