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

package org.apache.geode.redis.internal.executor.string;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RegionProvider;

public class StringMGetExecutorJUnitTest {

  private Command command;
  private ExecutionHandlerContext context;
  private MGetExecutor executor;
  private ByteBuf buffer;
  private RegionProvider regionProvider;

  @Before
  public void setup() {
    command = mock(Command.class);
    context = mock(ExecutionHandlerContext.class);

    regionProvider = mock(RegionProvider.class);
    when(context.getRegionProvider()).thenReturn(regionProvider);
    ByteBufAllocator allocator = mock(ByteBufAllocator.class);

    buffer = Unpooled.buffer();
    when(allocator.buffer()).thenReturn(buffer);
    when(allocator.buffer(anyInt())).thenReturn(buffer);
    when(context.getByteBufAllocator()).thenReturn(allocator);

    executor = spy(new MGetExecutor());
    doAnswer(x -> null).when(executor).checkDataType(any(), any(), any());
  }

  private String getBuffer() {
    return buffer.toString(0, buffer.writerIndex(), Charset.defaultCharset());
  }

  @Test
  public void testTooFewOptions() {
    executor.executeCommand(command, context);

    assertThat(getBuffer()).startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void testWrongNumberOfOptions() {
    List<byte[]> args = Arrays.asList("MGET".getBytes());
    when(command.getProcessedCommand()).thenReturn(args);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));

    executor.executeCommand(command, context);

    assertThat(getBuffer()).startsWith("-ERR The wrong number of arguments or syntax was provided");
  }
}
