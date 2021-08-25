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

package org.apache.geode.internal.cache.tier.sockets.command;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.query.cq.internal.command.CloseCQ;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.dunit.rules.CQUnitTestRule;

public class CloseCQTest {

  @Rule
  public CQUnitTestRule cqRule = new CQUnitTestRule();

  @Test
  public void needDataReadRegionToClose() throws Exception {
    CloseCQ closeCQ = (CloseCQ) CloseCQ.getCommand();

    closeCQ.cmdExecute(cqRule.message, cqRule.connection, cqRule.securityService, 0);

    verify(cqRule.securityService).authorize(Resource.DATA, Operation.READ, "regionName");
  }

  @Test
  public void callsWriteChunkedExceptionOnAuthorizationExpiredException() throws Exception {
    AuthenticationExpiredException authenticationExpiredException =
        new AuthenticationExpiredException("ouch");
    CloseCQ closeCQ = (CloseCQ) CloseCQ.getCommand();
    ChunkedMessage chunkedMessage = mock(ChunkedMessage.class);
    when(cqRule.connection.getChunkedResponseMessage()).thenReturn(chunkedMessage);
    doThrow(authenticationExpiredException).when(cqRule.securityService).authorize(Resource.DATA,
        Operation.READ, "regionName");

    closeCQ.cmdExecute(cqRule.message, cqRule.connection, cqRule.securityService, 0);

    verify(chunkedMessage).setMessageType(MessageType.EXCEPTION);
    verify(chunkedMessage).sendChunk(same(cqRule.connection));
  }
}
