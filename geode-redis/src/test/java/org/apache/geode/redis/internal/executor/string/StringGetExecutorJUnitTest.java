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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.KeyRegistrar;
import org.apache.geode.redis.internal.RegionProvider;

@SuppressWarnings("unchecked")
public class StringGetExecutorJUnitTest {

  private Command command;
  private ExecutionHandlerContext context;
  private GetExecutor executor;
  private ByteBuf buffer;
  private Region<ByteArrayWrapper, ByteArrayWrapper> region;
  private RegionProvider regionProvider;

  @Before
  public void setup() {
    command = mock(Command.class);
    context = mock(ExecutionHandlerContext.class);

    regionProvider = mock(RegionProvider.class);
    when(context.getRegionProvider()).thenReturn(regionProvider);
    region = mock(Region.class);
    when(regionProvider.getStringsRegion()).thenReturn(region);

    ByteBufAllocator allocator = mock(ByteBufAllocator.class);
    buffer = Unpooled.buffer();
    when(allocator.buffer()).thenReturn(buffer);
    when(allocator.buffer(anyInt())).thenReturn(buffer);
    when(context.getByteBufAllocator()).thenReturn(allocator);

    KeyRegistrar keyRegistrar = mock(KeyRegistrar.class);
    when(context.getKeyRegistrar()).thenReturn(keyRegistrar);

    executor = spy(new GetExecutor());
  }

  @Test
  public void testBasicGet() {
    List<byte[]> args = Arrays.asList("get".getBytes(), "key".getBytes());
    when(command.getProcessedCommand()).thenReturn(args);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).get(keyCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
  }

  @Test
  public void testGET_withIncorrectArgumentsReturnsError() {
    List<byte[]> commandArgumentWithTooFewArgs = Arrays.asList(
        "GET".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(command.getProcessedCommand()).thenReturn(commandArgumentWithTooFewArgs);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));

    executor.executeCommand(command, context);

    verify(command).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void testGET_withTooManyArgumentsReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "GET".getBytes(),
        "key".getBytes(),
        "something".getBytes(),
        "somethingelse".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(command.getProcessedCommand()).thenReturn(commandArgumentWithEXNoParameter);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));

    executor.executeCommand(command, context);

    verify(command).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .contains(
            "-ERR The wrong number of arguments or syntax was provided, the format for the GET command is \"GET key\"");
  }
}
