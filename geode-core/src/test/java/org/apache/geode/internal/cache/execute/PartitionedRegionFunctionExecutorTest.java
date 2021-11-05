/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.execute;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.PartitionedRegion;

public class PartitionedRegionFunctionExecutorTest {

  @Test
  public void executeThatSetsNetworkHopClearsItAtExit() {
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(region.getNetworkHopType()).thenReturn((byte) 1);
    PartitionedRegionFunctionExecutor executor = new PartitionedRegionFunctionExecutor(region);
    executor.execute(new TestFunction());
    verify(region, times(1)).clearNetworkHopData();
  }

  @Test
  public void executeThatDoesNotSetNetworkHopDoesNotClearItAtExit() {
    PartitionedRegion region = mock(PartitionedRegion.class);
    when(region.getNetworkHopType()).thenReturn((byte) 0);
    PartitionedRegionFunctionExecutor executor = new PartitionedRegionFunctionExecutor(region);
    executor.execute(new TestFunction());
    verify(region, never()).clearNetworkHopData();
  }

  private static class TestFunction implements Function {
    @Override
    public void execute(FunctionContext context) {}
  }
}
