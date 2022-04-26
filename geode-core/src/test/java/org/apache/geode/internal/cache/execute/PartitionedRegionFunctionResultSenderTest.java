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

import static org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultSenderTest.MethodToInvoke.LAST_RESULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.runner.RunWith;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.execute.metrics.FunctionStats;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class PartitionedRegionFunctionResultSenderTest {
  private DistributionManager dm = mock(DistributionManager.class);
  private PartitionedRegion region = mock(PartitionedRegion.class);
  private PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
  private ATestResultCollector rc = new ATestResultCollector();
  private ServerToClientFunctionResultSender serverToClientFunctionResultSender =
      mock(ServerToClientFunctionResultSender.class);
  private FunctionStats functionStats = mock(FunctionStats.class);

  enum MethodToInvoke {
    SEND_EXCEPTION, LAST_RESULT
  }

  @BeforeEach
  public void setUp() {
    when(region.getDataStore()).thenReturn(dataStore);
    when(dataStore.areAllBucketsHosted(any())).thenReturn(true);
  }

  @Test
  public void whenResponseToClientInLastResultFailsEndResultsIsCalled_NotOnlyLocal_OnlyRemote() {
    doThrow(new FunctionException()).when(serverToClientFunctionResultSender)
        .lastResult(any(), any());
    PartitionedRegionFunctionResultSender sender =
        new PartitionedRegionFunctionResultSender(dm,
            region, 1, rc, serverToClientFunctionResultSender, false, true, true,
            new TestFunction(), new int[2], null, (x, y) -> functionStats);

    sender.lastResult(new Object(), true, rc, null);

    assertThat(rc.isEndResultsCalled()).isEqualTo(true);
  }

  @ParameterizedTest(name = "{displayName} with {arguments}")
  @EnumSource(MethodToInvoke.class)
  public void whenResponseToClientInLastResultFailsEndResultsIsCalled_OnlyLocal_NotOnlyRemote(
      MethodToInvoke methodToInvoke) {
    doThrow(new FunctionException("IOException")).when(serverToClientFunctionResultSender)
        .lastResult(any(), any());

    PartitionedRegionFunctionResultSender sender =
        new PartitionedRegionFunctionResultSender(dm,
            region, 1, rc, serverToClientFunctionResultSender, true, false, true,
            new TestFunction(), new int[2]);

    if (methodToInvoke == LAST_RESULT) {
      sender.lastResult(new Object());
    } else {
      sender.sendException(new Exception());
    }

    assertThat(rc.isEndResultsCalled()).isEqualTo(true);
  }

  @ParameterizedTest(name = "{displayName} with {arguments}")
  @EnumSource(MethodToInvoke.class)
  public void whenResponseToClientInSendResultFailsEndResultsIsCalled_NotOnlyLocal_OnlyRemote(
      MethodToInvoke methodToInvoke) {
    doThrow(new FunctionException("IOException")).when(serverToClientFunctionResultSender)
        .sendResult(any(), any());
    PartitionedRegionFunctionResultSender sender =
        new PartitionedRegionFunctionResultSender(dm,
            region, 1, rc, serverToClientFunctionResultSender, false, true, true,
            new TestFunction(), new int[2], null, (x, y) -> functionStats);

    if (methodToInvoke == LAST_RESULT) {
      sender.lastResult(new Object());
    } else {
      sender.sendException(new Exception());
    }

    assertThat(rc.isEndResultsCalled()).isEqualTo(true);
  }

  @ParameterizedTest(name = "{displayName} with {arguments}")
  @EnumSource(MethodToInvoke.class)
  public void whenResponseToClientInSendResultFailsEndResultsIsCalled_NotOnlyLocal_NotOnlyRemote(
      MethodToInvoke methodToInvoke) {
    doThrow(new FunctionException("IOException")).when(serverToClientFunctionResultSender)
        .sendResult(any(), any());
    PartitionedRegionFunctionResultSender sender =
        new PartitionedRegionFunctionResultSender(dm,
            region, 1, rc, serverToClientFunctionResultSender, false, false, true,
            new TestFunction(), new int[2], null, (x, y) -> functionStats);

    if (methodToInvoke == LAST_RESULT) {
      sender.lastResult(new Object(), true, rc, null);
    } else {
      sender.sendException(new Exception());
    }

    assertThat(rc.isEndResultsCalled()).isEqualTo(true);
  }

  private static class TestFunction implements Function {
    @Override
    public void execute(FunctionContext context) {}
  }

  private static class ATestResultCollector implements ResultCollector {
    private volatile boolean isEndResultsCalled = false;

    @Override
    public Object getResult() throws FunctionException {
      return null;
    }

    @Override
    public Object getResult(long timeout, TimeUnit unit)
        throws FunctionException, InterruptedException {
      return null;
    }

    @Override
    public void addResult(DistributedMember memberID, Object resultOfSingleExecution) {}

    @Override
    public void endResults() {
      isEndResultsCalled = true;
    }

    @Override
    public void clearResults() {}

    public boolean isEndResultsCalled() {
      return isEndResultsCalled;
    }
  }
}
