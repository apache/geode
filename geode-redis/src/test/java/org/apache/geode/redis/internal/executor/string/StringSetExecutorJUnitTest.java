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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

public class StringSetExecutorJUnitTest {

  private Command command;
  private ExecutionHandlerContext context;
  private SetExecutor executor;
  private ByteBuf buffer;
  private Region<ByteArrayWrapper, ByteArrayWrapper> region;
  private RegionProvider regionProvider;

  @SuppressWarnings("unchecked")
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

    executor = spy(new SetExecutor());
    doAnswer(x -> null).when(executor).checkDataType(any(), any(), any());
  }

  private String getBuffer() {
    return buffer.toString(0, buffer.writerIndex(), Charset.defaultCharset());
  }

  @Test
  public void testTooFewOptions() {
    executor.executeCommand(command, context);

    assertThat(getBuffer()).startsWith("-ERR");
  }

  @Test
  public void testBasicSet() {
    List<byte[]> args = Arrays.asList("SET".getBytes(), "key".getBytes(), "value".getBytes());
    when(command.getProcessedCommand()).thenReturn(args);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).put(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(getBuffer()).startsWith("+OK");
  }

  @Test
  public void testSetNX_WhenKeyDoesNotExist() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "NX".getBytes());
    when(command.getProcessedCommand()).thenReturn(args);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));
    when(region.putIfAbsent(any(), any())).thenReturn(null);

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).putIfAbsent(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(getBuffer()).startsWith("+OK");
  }

  @Test
  public void testSetNX_WhenKeyAlreadyExists() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "NX".getBytes());
    when(command.getProcessedCommand()).thenReturn(args);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));
    when(region.putIfAbsent(any(), any())).thenReturn(new ByteArrayWrapper("old-value".getBytes()));

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).putIfAbsent(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(getBuffer()).startsWith("$-1");
  }

  @Test
  public void testSetXX_WhenKeyDoesNotExist() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "XX".getBytes());
    when(command.getProcessedCommand()).thenReturn(args);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));
    when(region.containsKey(any())).thenReturn(false);

    executor.executeCommand(command, context);
    verify(region, never()).put(any(), any());

    assertThat(getBuffer()).startsWith("$-1");
  }

  @Test
  public void testSetXX_WhenKeyAlreadyExists() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "XX".getBytes());
    when(command.getProcessedCommand()).thenReturn(args);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));
    when(region.containsKey(any())).thenReturn(true);

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).put(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(getBuffer()).startsWith("+OK");
  }

  @Test
  public void testSetEX() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "EX".getBytes(),
        "5".getBytes());
    when(command.getProcessedCommand()).thenReturn(args);
    when(command.getKey()).thenReturn(new ByteArrayWrapper("key".getBytes()));

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> expireKey = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<Long> expireValue = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);

    verify(region).put(keyCaptor.capture(), valueCaptor.capture());
    verify(regionProvider).setExpiration(expireKey.capture(), expireValue.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(expireKey.getValue()).isEqualTo("key");
    assertThat(expireValue.getValue()).isEqualTo(5000);
    assertThat(getBuffer()).startsWith("+OK");
  }
}
