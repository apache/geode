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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DestroyAsyncEventQueueFunctionTest {

  private static final String TEST_AEQ_ID = "Test-AEQ";
  private AsyncEventQueue mockAEQ;
  private FunctionContext mockContext;
  private DestroyAsyncEventQueueFunctionArgs mockArgs;
  private GemFireCacheImpl cache;

  @Before
  public void setUp() throws Exception {
    mockAEQ = mock(AsyncEventQueueImpl.class);
    mockContext = mock(FunctionContext.class);
    mockArgs = mock(DestroyAsyncEventQueueFunctionArgs.class);
    cache = Fakes.cache();

    when(mockArgs.getId()).thenReturn(TEST_AEQ_ID);
    when(mockAEQ.getId()).thenReturn(TEST_AEQ_ID);
  }

  @Test
  public void execute_validAeqId_OK() throws Throwable {
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(mockAEQ);

    TestResultSender resultSender = new TestResultSender();

    FunctionContext context = new FunctionContextImpl(cache, "functionId", mockArgs, resultSender);
    new DestroyAsyncEventQueueFunction().execute(context);
    List<?> results = resultSender.getResults();
    assertThat(results.size()).isEqualTo(1);
    CliFunctionResult result = (CliFunctionResult) results.get(0);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMessage()).containsPattern(TEST_AEQ_ID + ".*destroyed");
  }

  @Test
  public void execute_nonexistentAeqId_returnsError() throws Throwable {
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(null);

    TestResultSender resultSender = new TestResultSender();

    FunctionContext context = new FunctionContextImpl(cache, "functionId", mockArgs, resultSender);
    new DestroyAsyncEventQueueFunction().execute(context);
    List<?> results = resultSender.getResults();
    assertThat(results.size()).isEqualTo(1);
    CliFunctionResult result = (CliFunctionResult) results.get(0);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getMessage()).containsPattern(TEST_AEQ_ID + ".*not found");
  }

  @Test
  public void execute_nonexistentAeqIdIfExists_returnsSuccess() throws Throwable {
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(null);
    when(mockArgs.isIfExists()).thenReturn(true);

    TestResultSender resultSender = new TestResultSender();

    FunctionContext context = new FunctionContextImpl(cache, "functionId", mockArgs, resultSender);
    new DestroyAsyncEventQueueFunction().execute(context);
    List<?> results = resultSender.getResults();
    assertThat(results.size()).isEqualTo(1);
    CliFunctionResult result = (CliFunctionResult) results.get(0);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMessage()).containsPattern("Skipping:.*" + TEST_AEQ_ID + ".*not found");
  }

  private static class TestResultSender implements ResultSender {

    private final List<Object> results = new LinkedList<Object>();

    private Throwable t;

    protected List<Object> getResults() throws Throwable {
      if (t != null) {
        throw t;
      }
      return Collections.unmodifiableList(results);
    }

    @Override
    public void lastResult(final Object lastResult) {
      results.add(lastResult);
    }

    @Override
    public void sendResult(final Object oneResult) {
      results.add(oneResult);
    }

    @Override
    public void sendException(final Throwable t) {
      this.t = t;
    }
  }
}
