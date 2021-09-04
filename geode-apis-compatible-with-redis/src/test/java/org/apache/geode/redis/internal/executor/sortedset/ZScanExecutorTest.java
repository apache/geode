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
package org.apache.geode.redis.internal.executor.sortedset;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.executor.key.AbstractScanExecutor.UNSIGNED_LONG_CAPACITY;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.key.AbstractScanExecutor;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZScanExecutorTest {
  AbstractScanExecutor scanExecutor;
  private Command mockCommand;
  private ExecutionHandlerContext mockContext;

  @Before
  public void setUp() {
    mockCommand = mock(Command.class);
    when(mockCommand.getProcessedCommand()).thenReturn(
        Arrays.asList(stringToBytes("ZSCAN"), stringToBytes("key"), stringToBytes("0")));
    mockContext = mock(ExecutionHandlerContext.class);
    scanExecutor = spy(new ZScanExecutor());
  }

  @Test
  public void executeCommand_returnsErrorResponse_whenGetIntCursorThrows() {
    doThrow(new RedisException()).when(scanExecutor).getIntCursor(anyString());
    assertThat(scanExecutor.executeCommand(mockCommand, mockContext).toString())
        .isEqualTo(RedisResponse.error(ERROR_CURSOR).toString());
  }

  @Test
  public void convertGlobToRegex_handlesNullPattern() {
    assertThat(scanExecutor.convertGlobToRegex(null)).isNull();
  }

  @Test
  public void getIntCursor_throwsForNonIntegerString() {
    assertThatThrownBy(() -> scanExecutor.getIntCursor("notANumber"))
        .isInstanceOf(RedisException.class).hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void getIntCursor_throwsForIntegerOutOfRange() {
    String overULongCapacity = UNSIGNED_LONG_CAPACITY.add(new BigInteger("1")).toString();
    assertThatThrownBy(() -> scanExecutor.getIntCursor(overULongCapacity))
        .isInstanceOf(RedisException.class).hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void getIntCursor_narrowsToIntWhenGreaterThanIntegerMaxValue() {
    String uLongCapacity = UNSIGNED_LONG_CAPACITY.toString();
    assertThat(scanExecutor.getIntCursor(uLongCapacity)).isEqualTo(Integer.MAX_VALUE);

    String longMax = String.valueOf(Long.MAX_VALUE);
    assertThat(scanExecutor.getIntCursor(longMax)).isEqualTo(Integer.MAX_VALUE);

    String overIntMax = String.valueOf(Integer.MAX_VALUE + 10L);
    assertThat(scanExecutor.getIntCursor(overIntMax)).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void getIntCursor_returnsIntWhenLessThanIntegerMaxValue() {
    final long underIntMax = Integer.MAX_VALUE - 10L;
    assertThat(scanExecutor.getIntCursor(String.valueOf(underIntMax))).isEqualTo(underIntMax);

  }
}
