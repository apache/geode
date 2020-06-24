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
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class StringSetExecutorJUnitTest {

  private ExecutionHandlerContext context;
  private SetExecutor executor;
  private ByteBuf buffer;
  private Region<ByteArrayWrapper, RedisData> region;
  private RegionProvider regionProvider;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    context = mock(ExecutionHandlerContext.class);
    regionProvider = mock(RegionProvider.class);

    when(context.getRegionProvider()).thenReturn(regionProvider);
    region = mock(Region.class);
    when(regionProvider.getDataRegion()).thenReturn(region);
    ByteBufAllocator allocator = mock(ByteBufAllocator.class);

    buffer = Unpooled.buffer();
    when(allocator.buffer()).thenReturn(buffer);
    when(allocator.buffer(anyInt())).thenReturn(buffer);
    when(context.getByteBufAllocator()).thenReturn(allocator);

    executor = spy(new SetExecutor());
  }

  @Test
  public void testSET_nonexistentArgument_returnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "OR".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString())
        .contains(RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void testSET_EXargument_withoutParameterReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "EX".getBytes());

    Command command = new Command(commandArgumentWithEXNoParameter);

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString())
        .contains(RedisConstants.ERROR_SYNTAX);
  }


  @Test
  @Ignore("should pass When KeepTTL is implemented")
  public void testSET_KEEPTTLArgument_DoesNoThrowError_givenCorrectInput() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "KEEPTTL".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString()).doesNotContain("-ERR");
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

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString())
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

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString())
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

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString())
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

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString())
        .contains(RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void testSET_EXandPX_inSameCommand_ReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "EX".getBytes(),
        "30".getBytes(),
        "PX".getBytes(),
        "3000".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString())
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

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString()).contains(RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void testSET_XXandNX_inSameCommand_ReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "SET".getBytes(),
        "key".getBytes(),
        "value".getBytes(),
        "XX".getBytes(),
        "NX".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString()).contains(RedisConstants.ERROR_SYNTAX);
  }


}
