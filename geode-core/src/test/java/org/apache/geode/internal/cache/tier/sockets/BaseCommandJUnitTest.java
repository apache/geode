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
package org.apache.geode.internal.cache.tier.sockets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteRegionFunction66;

public class BaseCommandJUnitTest {

  private final BaseCommand baseCommand = (BaseCommand) ExecuteRegionFunction66.getCommand();

  @Test
  public void whenLastReceivedIsSetThenCheckAndSetLastResultSentIfValidMustReturnTrue() {
    ServerToClientFunctionResultSender resultSender =
        mock(ServerToClientFunctionResultSender.class);
    when(resultSender.isLastResultReceived()).thenReturn(true);
    assertFalse(baseCommand.setLastResultReceived(resultSender));

  }

  @Test
  public void whenLastReceivedIsNotSetThenCheckAndSetLastResultSentIfValidMustReturnFalse() {
    ServerToClientFunctionResultSender resultSender =
        mock(ServerToClientFunctionResultSender.class);
    when(resultSender.isLastResultReceived()).thenReturn(false);
    assertTrue(baseCommand.setLastResultReceived(resultSender));

  }

  @Test
  public void whenLastReceivedIsNotSetThenCheckAndSetLastResultSentIfValidMustSetIt() {
    ServerToClientFunctionResultSender resultSender =
        mock(ServerToClientFunctionResultSender.class);
    when(resultSender.isLastResultReceived()).thenReturn(false);
    baseCommand.setLastResultReceived(resultSender);
    verify(resultSender, times(1)).setLastResultReceived(true);

  }

  @Test
  public void whenLastReceivedIsSetThenCheckAndSetLastResultSentIfValidMustNotSetIt() {
    ServerToClientFunctionResultSender resultSender =
        mock(ServerToClientFunctionResultSender.class);
    when(resultSender.isLastResultReceived()).thenReturn(true);
    baseCommand.setLastResultReceived(resultSender);
    verify(resultSender, times(0)).setLastResultReceived(true);

  }
}
