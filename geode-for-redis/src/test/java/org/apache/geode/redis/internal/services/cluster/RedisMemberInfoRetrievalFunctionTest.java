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
package org.apache.geode.redis.internal.services.cluster;

import static org.apache.geode.cache.execute.FunctionService.isRegistered;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedMember;

public class RedisMemberInfoRetrievalFunctionTest {

  private RedisMemberInfoRetrievalFunction function;

  @Before
  public void setUp() {
    function = spy(new RedisMemberInfoRetrievalFunction());
  }

  @Test
  public void register_registersFunctionWithFunctionService() {
    assertThat(RedisMemberInfoRetrievalFunction.register())
        .isInstanceOf(RedisMemberInfoRetrievalFunction.class);
    assertThat(isRegistered(RedisMemberInfoRetrievalFunction.ID)).isTrue();
  }

  @Test
  public void initialize_setsMemberInfo() {
    DistributedMember mockMember = mock(DistributedMember.class);
    String address = "1.1.1.1";
    int redisPort = 12345;
    RedisMemberInfo expectedMemberInfo = new RedisMemberInfo(mockMember, address, redisPort);

    function.initialize(mockMember, address, redisPort);

    assertThat(function.getMemberInfo()).isEqualTo(expectedMemberInfo);
  }

  @Test
  public void initialize_setsMemberInfo_givenNullAddress()
      throws UnknownHostException {
    DistributedMember mockMember = mock(DistributedMember.class);
    String address = "1.1.1.1";
    int redisPort = 12345;
    RedisMemberInfo expectedMemberInfo = new RedisMemberInfo(mockMember, address, redisPort);

    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.getHostAddress()).thenReturn(address);

    doReturn(mockInetAddress).when(function).getLocalHost();

    function.initialize(mockMember, null, redisPort);

    assertThat(function.getMemberInfo()).isEqualTo(expectedMemberInfo);
    verify(function).getLocalHost();
  }

  @Test
  public void initialize_setsMemberInfo_givenEmptyAddress()
      throws UnknownHostException {
    DistributedMember mockMember = mock(DistributedMember.class);
    String address = "1.1.1.1";
    int redisPort = 12345;
    RedisMemberInfo expectedMemberInfo = new RedisMemberInfo(mockMember, address, redisPort);

    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.getHostAddress()).thenReturn(address);

    doReturn(mockInetAddress).when(function).getLocalHost();

    function.initialize(mockMember, "", redisPort);

    assertThat(function.getMemberInfo()).isEqualTo(expectedMemberInfo);
    verify(function).getLocalHost();
  }

  @Test
  public void initialize_setsMemberInfo_givenInvalidAddress()
      throws UnknownHostException {
    DistributedMember mockMember = mock(DistributedMember.class);
    String address = "1.1.1.1";
    int redisPort = 12345;
    RedisMemberInfo expectedMemberInfo = new RedisMemberInfo(mockMember, address, redisPort);

    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.getHostAddress()).thenReturn(address);

    doReturn(mockInetAddress).when(function).getLocalHost();

    function.initialize(mockMember, "0.0.0.0", redisPort);

    assertThat(function.getMemberInfo()).isEqualTo(expectedMemberInfo);
    verify(function).getLocalHost();
  }

  @Test
  public void initialize_setsMemberInfo_givenGetLocalHostThrowsException()
      throws UnknownHostException {
    DistributedMember mockMember = mock(DistributedMember.class);
    int redisPort = 12345;
    RedisMemberInfo expectedMemberInfo = new RedisMemberInfo(mockMember, "127.0.0.1", redisPort);

    doThrow(new UnknownHostException()).when(function).getLocalHost();

    function.initialize(mockMember, null, redisPort);

    assertThat(function.getMemberInfo()).isEqualTo(expectedMemberInfo);
    verify(function).getLocalHost();
  }

  @Test
  public void execute_executesFunction() {
    FunctionContext<Void> mockContext = uncheckedCast(mock(FunctionContext.class));
    ResultSender<Object> mockResultSender = uncheckedCast(mock(ResultSender.class));

    when(mockContext.getResultSender()).thenReturn(mockResultSender);

    DistributedMember mockMember = mock(DistributedMember.class);
    String address = "1.1.1.1";
    int redisPort = 12345;

    function.initialize(mockMember, address, redisPort);
    function.execute(mockContext);

    verify(mockResultSender).lastResult(function.getMemberInfo());
  }
}
