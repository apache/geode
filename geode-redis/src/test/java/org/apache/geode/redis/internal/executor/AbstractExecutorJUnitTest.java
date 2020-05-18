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
package org.apache.geode.redis.internal.executor;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.KeyRegistrar;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.string.SetExecutor;

public class AbstractExecutorJUnitTest {

  @Test
  public void testRemoveEntry() {
    // Create any instance of the AbstractExecutor
    AbstractExecutor abstractExecutor = new SetExecutor();

    // setup mocks
    ExecutionHandlerContext context = Mockito.mock(ExecutionHandlerContext.class);
    RegionProvider rp = Mockito.mock(RegionProvider.class);
    KeyRegistrar keyRegistrar = Mockito.mock(KeyRegistrar.class);
    Mockito.when(context.getRegionProvider()).thenReturn(rp);
    Mockito.when(context.getKeyRegistrar()).thenReturn(keyRegistrar);
    Mockito.when(rp.removeKey(any())).thenReturn(true);

    // Assert false to protected or null types
    assertFalse(abstractExecutor.removeEntry(Coder.stringToByteArrayWrapper("junit"),
        context));

    assertFalse(
        abstractExecutor.removeEntry(Coder.stringToByteArrayWrapper("junit"), context));

  }

}
