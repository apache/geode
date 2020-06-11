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

public class TouchReplicatedRegionEntriesFunctionTest {
  private final TouchReplicatedRegionEntriesFunction function =
      spy(new TouchReplicatedRegionEntriesFunction());
  private final FunctionContext context = mock(RegionFunctionContext.class);
  private final Cache cache = mock(Cache.class);
  private final LogWriter logger = mock(LogWriter.class);
  private final Region region = mock(Region.class);
  private final ResultSender resultSender = mock(ResultSender.class);
  private final String regionName = "regionName";
  private final HashSet<String> keys = new HashSet<>();
  private final Object[] arguments = new Object[] {regionName, keys};

  @Before
  public void setUp() {
    when(context.getArguments()).thenReturn(arguments);
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(cache.getLogger()).thenReturn(logger);
    when(logger.fineEnabled()).thenReturn(false);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContext() {
    when(cache.getRegion(regionName)).thenReturn(region);

    function.execute(context);

    verify(region).getAll(keys);
    verify(resultSender).lastResult(true);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContextAndNullRegion() {
    when(cache.getRegion(regionName)).thenReturn(null);

    function.execute(context);

    verify(region, times(0)).getAll(keys);
    verify(resultSender).lastResult(true);
  }
}
