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
 *
 */

package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.management.internal.configuration.realizers.RegionConfigRealizer;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class RegionCreateFunctionJUnitTest {

  @Test
  public void testSkipIfExists() {
    RegionCreateFunction function = spy(new RegionCreateFunction());
    @SuppressWarnings("unchecked")
    FunctionContext<CreateRegionFunctionArgs> context = mock(FunctionContext.class);
    InternalCache internalCache = mock(InternalCache.class);
    InternalCacheForClientAccess cache = mock(InternalCacheForClientAccess.class);
    @SuppressWarnings("unchecked")
    ResultSender<Object> resultSender = mock(ResultSender.class);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(internalCache);
    when(internalCache.getCacheForProcessingClientRequests()).thenReturn(cache);
    when(context.getMemberName()).thenReturn("member");

    CreateRegionFunctionArgs args = new CreateRegionFunctionArgs("/REGION",
        new RegionConfig(), true);
    when(context.getArguments()).thenReturn(args);

    RegionConfigRealizer realizer = mock(RegionConfigRealizer.class);

    Region<?, ?> region = mock(Region.class);
    when(function.getRealizer()).thenReturn(realizer);
    doThrow(new RegionExistsException(region))
        .when(realizer).create(any(), any(), any());

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> captor = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(captor.capture());

    assertThat(captor.getValue().getStatusMessage())
        .isEqualTo("Skipping \"member\". Region \"/REGION\" already exists.");
  }
}
