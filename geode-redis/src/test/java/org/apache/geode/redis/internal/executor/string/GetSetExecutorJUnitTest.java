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
import static org.mockito.Mockito.doThrow;
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

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.KeyRegistrar;
import org.apache.geode.redis.internal.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.RedisLockService;
import org.apache.geode.redis.internal.RegionProvider;

public class GetSetExecutorJUnitTest {
  private ExecutionHandlerContext context;
  private GetSetExecutor executor;
  private Region<ByteArrayWrapper, ByteArrayWrapper> region;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    context = mock(ExecutionHandlerContext.class);

    RegionProvider regionProvider = mock(RegionProvider.class);
    when(context.getRegionProvider()).thenReturn(regionProvider);
    region = mock(Region.class);
    when(regionProvider.getStringsRegion()).thenReturn(region);

    ByteBufAllocator allocator = mock(ByteBufAllocator.class);
    ByteBuf buffer = Unpooled.buffer();
    when(allocator.buffer()).thenReturn(buffer);
    when(allocator.buffer(anyInt())).thenReturn(buffer);
    when(context.getByteBufAllocator()).thenReturn(allocator);

    KeyRegistrar keyRegistrar = mock(KeyRegistrar.class);
    when(context.getKeyRegistrar()).thenReturn(keyRegistrar);

    RedisLockService lockService = mock(RedisLockService.class);
    when(context.getLockService()).thenReturn(lockService);

    executor = spy(new GetSetExecutor());
  }

  @Test
  public void test_givenTooFewOptions_returnsError() {
    List<byte[]> args = Arrays.asList(
        "GETSET".getBytes());
    Command command = new Command(args);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void test_givenWrongNumberOfOptions_returnsError() {
    List<byte[]> args = Arrays.asList(
        "GETSET".getBytes(),
        "key1".getBytes(),
        "val1".getBytes(),
        "key2".getBytes());
    Command command = new Command(args);

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void test_givenKeyHoldsWrongType_returnsError() {
    List<byte[]> args = Arrays.asList(
        "GETSET".getBytes(),
        "key1".getBytes(),
        "val1".getBytes());
    Command command = new Command(args);

    when(region.get(any())).thenReturn(new ByteArrayWrapper("non-null value".getBytes()));
    doThrow(new RedisDataTypeMismatchException("this string doesn't matter")).when(executor)
        .checkDataType(any(), any(), any());

    executor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR WRONGTYPE Operation against a key holding the wrong kind of value");
  }
}
