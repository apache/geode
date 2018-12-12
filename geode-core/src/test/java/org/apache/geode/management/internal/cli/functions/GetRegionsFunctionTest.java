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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;

public class GetRegionsFunctionTest {

  private enum STATE {
    INITIAL, BLOCKING, FINISHED
  };

  private static GetRegionsFunctionTest.STATE lockingThreadState =
      GetRegionsFunctionTest.STATE.INITIAL;
  private static AtomicBoolean lockAquired = new AtomicBoolean(false);
  private static AtomicBoolean functionExecuted = new AtomicBoolean(false);

  private GetRegionsFunction getRegionsFunction;
  private FunctionContext functionContext;

  @Before
  public void before() {
    getRegionsFunction = new GetRegionsFunction();
    functionContext = mock(FunctionContext.class);
  }

  @Test
  public void doNotUseCacheFacotryToGetCache() throws Exception {
    // start a thread that would hold on to the CacheFactory's class lock
    new Thread(() -> {
      synchronized (CacheFactory.class) {
        lockAquired.set(true);
        lockingThreadState = GetRegionsFunctionTest.STATE.BLOCKING;
        try {
          await().untilTrue(functionExecuted);
        } catch (ConditionTimeoutException e) {
          e.printStackTrace();
          lockingThreadState = GetRegionsFunctionTest.STATE.FINISHED;
        }
      }
    }).start();

    // wait till the blocking thread aquired the lock on CacheFactory
    await().untilTrue(lockAquired);
    when(functionContext.getCache()).thenReturn(mock(Cache.class));
    when(functionContext.getResultSender()).thenReturn(mock(ResultSender.class));

    // execute a function that would get the cache, make sure that's not waiting on the lock
    // of CacheFactory
    getRegionsFunction.execute(functionContext);
    assertThat(lockingThreadState).isEqualTo(lockingThreadState.BLOCKING);


    // this will make the awaitility call in the thread return
    functionExecuted.set(true);
  }
}
