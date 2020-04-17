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

import static java.nio.charset.Charset.defaultCharset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RegionProvider;

public class StringSetExecutorJUnitTest {

  private ExecutionHandlerContext context;
  private SetExecutor executor;
  private ByteBuf buffer;
  private Region<ByteArrayWrapper, ByteArrayWrapper> region;
  private RegionProvider regionProvider;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
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


  @Test
  public void testBasicSet() {
    List<byte[]> args = Arrays.asList("SET".getBytes(), "key".getBytes(), "value".getBytes());
    Command command = new Command(args);

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).put(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(command.getResponse().toString(defaultCharset())).startsWith("+OK");
  }

  @Test
  public void testSET_TooFewArgumentsReturnsError() {
    List<byte[]> commandArgumentWithTooFewArgs = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes());
    Command command = new Command(commandArgumentWithTooFewArgs);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void testSET_EXargument_withoutParameterReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "EX".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(defaultCharset()))
        .contains(RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void testSET_EXargument_withNonNumericParameter_returnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "EX".getBytes(),
        "NotANumberAtAll".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(defaultCharset()))
        .contains(RedisConstants.ERROR_NOT_INTEGER);
  }

  @Test
  public void testSET_EXargument_withZeroExpireTime_returnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "EX".getBytes(),
        "0".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(defaultCharset()))
        .contains(RedisConstants.ERROR_INVALID_EXPIRE_TIME);
  }

  @Test
  public void testSET_PXargument_withoutParameterReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "PX".getBytes());

    Command command = new Command(commandArgumentWithEXNoParameter);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(defaultCharset()))
        .contains(RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void testSET_PXandEX_inSameCommand_ReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "PX".getBytes(),
        "30000".getBytes(),
        "EX".getBytes(),
        "30".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(defaultCharset()))
        .contains(RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void testSET_NXandXX_inSameCommand_ReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "NX".getBytes(),
        "XX".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(defaultCharset()))
        .contains(RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void testSetNX_WhenKeyDoesNotExist() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "NX".getBytes());

    when(region.putIfAbsent(any(), any())).thenReturn(null);
    Command command = new Command(args);

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).putIfAbsent(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(command.getResponse().toString(defaultCharset())).startsWith("+OK");
  }

  @Test
  public void testSetNX_WhenKeyAlreadyExists() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "NX".getBytes());
    when(region.putIfAbsent(any(), any())).thenReturn(new ByteArrayWrapper("old-value".getBytes()));
    Command command = new Command(args);

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).putIfAbsent(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(command.getResponse().toString(defaultCharset())).startsWith("$-1");
  }

  @Test
  public void testSetXX_WhenKeyDoesNotExist() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "XX".getBytes());
    when(region.containsKey(any())).thenReturn(false);
    Command command = new Command(args);

    executor.executeCommand(command, context);
    verify(region, never()).put(any(), any());

    assertThat(command.getResponse().toString(defaultCharset())).startsWith("$-1");
  }

  @Test
  public void testSetXX_WhenKeyAlreadyExists() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "XX".getBytes());
    when(region.containsKey(any())).thenReturn(true);
    Command command = new Command(args);

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).put(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(command.getResponse().toString(defaultCharset())).startsWith("+OK");
  }

  @Test
  public void testSetEX() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "EX".getBytes(),
        "5".getBytes());
    Command command = new Command(args);

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
    assertThat(command.getResponse().toString(defaultCharset())).startsWith("+OK");
  }

  @Test
  public void testSet_withKEEPTTL() {
    List<byte[]> args = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "KEEPTTL".getBytes());
    Command command = new Command(args);

    executor.executeCommand(command, context);

    ArgumentCaptor<ByteArrayWrapper> keyCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    ArgumentCaptor<ByteArrayWrapper> valueCaptor = ArgumentCaptor.forClass(ByteArrayWrapper.class);
    verify(region).put(keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo("key");
    assertThat(valueCaptor.getValue()).isEqualTo("value");
    assertThat(command.getResponse().toString(defaultCharset())).startsWith("+OK");

    ByteArrayWrapper keyWrapper = new ByteArrayWrapper("key".getBytes());
    verify(regionProvider, never()).cancelKeyExpiration(keyWrapper);
  }
}
