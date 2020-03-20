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

package org.apache.geode.redis.internal.executor.string;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.KeyRegistrar;
import org.apache.geode.redis.internal.RegionProvider;


public class IncrExecutorJUnitTest {
  private ExecutionHandlerContext context;
  private Command command;
  private UnpooledByteBufAllocator byteBuf;
  private IncrExecutor incrExecutor;
  private KeyRegistrar keyRegistrar;
  private Region<ByteArrayWrapper, ByteArrayWrapper> region;
  private RegionProvider regionProvider;
  private ByteBuf buffer;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    context = mock(ExecutionHandlerContext.class);
    command = mock(Command.class);
    byteBuf = new UnpooledByteBufAllocator(false);

    incrExecutor = spy(new IncrExecutor());

    regionProvider = mock(RegionProvider.class);
    when(context.getRegionProvider()).thenReturn(regionProvider);

    region = mock(Region.class);
    when(regionProvider.getStringsRegion()).thenReturn(region);

    ByteBufAllocator allocator = mock(ByteBufAllocator.class);
    buffer = Unpooled.buffer();
    when(allocator.buffer()).thenReturn(buffer);
    when(allocator.buffer(anyInt())).thenReturn(buffer);
    when(context.getByteBufAllocator()).thenReturn(allocator);

    keyRegistrar = mock(KeyRegistrar.class);
    when(context.getKeyRegistrar()).thenReturn(keyRegistrar);
  }

  @Test
  public void calledWithTooFewOptions_returnsError() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("INCR".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    incrExecutor.executeCommand(command, context);

    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void calledWithTooManyOptions_returnsError() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("INCR".getBytes());
    commandsAsBytes.add("key".getBytes());
    commandsAsBytes.add("spurious key".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    incrExecutor.executeCommand(command, context);

    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }
}
