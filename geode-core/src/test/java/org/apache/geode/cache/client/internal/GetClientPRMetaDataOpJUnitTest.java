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
package org.apache.geode.cache.client.internal;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

public class GetClientPRMetaDataOpJUnitTest {
  @Test
  public void processResponseWhenCacheClosedShuouldReturnNull() throws Exception {
    Cache cache = mock(Cache.class);
    ClientMetadataService cms = new ClientMetadataService(cache);
    cms = spy(cms);
    doReturn(true).when(cache).isClosed();

    Message msg = mock(Message.class);
    GetClientPRMetaDataOp.GetClientPRMetaDataOpImpl op =
        new GetClientPRMetaDataOp.GetClientPRMetaDataOpImpl("testRegion", cms);
    op = spy(op);

    when(msg.getMessageType()).thenReturn(MessageType.RESPONSE_CLIENT_PR_METADATA);

    assertNull(op.processResponse(msg));
    verify(cms, times(1)).setMetadataStable(eq(true));
  }
}
