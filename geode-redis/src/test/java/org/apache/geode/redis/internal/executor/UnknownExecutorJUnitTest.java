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

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * Test for the UnkownExecutor
 *
 *
 */
public class UnknownExecutorJUnitTest {
  /**
   * Test the execution method
   */
  @Test
  public void testExecuteCommand() {
    UnknownExecutor exe = new UnknownExecutor();

    Command command = Mockito.mock(Command.class);
    ExecutionHandlerContext context = Mockito.mock(ExecutionHandlerContext.class);


    UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
    Mockito.when(context.getByteBufAllocator()).thenReturn(byteBuf);

    RedisResponse response = exe.executeCommand(command, context);

    assertThat(response.toString()).contains("ERR Unable to process unknown command");
  }

}
