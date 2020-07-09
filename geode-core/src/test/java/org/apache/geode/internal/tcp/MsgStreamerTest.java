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

package org.apache.geode.internal.tcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLException;

import org.junit.Test;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.net.BufferPool;
import org.apache.geode.internal.serialization.KnownVersion;

public class MsgStreamerTest {
  private DMStats stats = mock(DMStats.class);
  private BufferPool pool = spy(new BufferPool(stats));
  ClusterConnection connection1 = mock(ClusterConnection.class);
  ClusterConnection connection2 = mock(ClusterConnection.class);

  @Test
  public void create() {
    final BaseMsgStreamer msgStreamer = createMsgStreamer(false);
    assertThat(msgStreamer).isInstanceOf(MsgStreamer.class);
  }

  @Test
  public void createWithMixedVersions() {
    final BaseMsgStreamer msgStreamer = createMsgStreamer(true);
    assertThat(msgStreamer).isInstanceOf(MsgStreamerList.class);
  }

  @Test
  public void streamerListRelease() throws IOException {
    final MsgStreamerList msgStreamer = (MsgStreamerList) createMsgStreamer(true);
    msgStreamer.writeMessage();
    verify(pool, times(2)).releaseSenderBuffer(isA(ByteBuffer.class));
  }

  @Test
  public void streamerListReleaseWithException() throws IOException {
    final MsgStreamerList msgStreamer = (MsgStreamerList) createMsgStreamer(true);
    // if the first streamer throws an exception while writing the message we should still only
    // release two buffers (one for each streamer)
    doThrow(new SSLException("")).when(connection1).sendPreserialized(any(ByteBuffer.class),
        any(Boolean.class), any(DistributionMessage.class));
    msgStreamer.writeMessage();
    verify(pool, times(2)).releaseSenderBuffer(isA(ByteBuffer.class));
  }

  protected BaseMsgStreamer createMsgStreamer(boolean mixedDestinationVersions) {

    InternalDistributedMember member1, member2;
    member1 = new InternalDistributedMember("localhost", 1234);
    member2 = new InternalDistributedMember("localhost", 2345);

    DistributionMessage message = new SerialAckedMessage();
    message.setRecipients(Arrays.asList(member1, member2));

    when(connection1.getRemoteAddress()).thenReturn(member1);
    when(connection1.getRemoteVersion()).thenReturn(KnownVersion.CURRENT);
    when(connection2.getRemoteAddress()).thenReturn(member2);
    if (mixedDestinationVersions) {
      when(connection1.getRemoteVersion()).thenReturn(KnownVersion.GEODE_1_12_0);
    } else {
      when(connection1.getRemoteVersion()).thenReturn(KnownVersion.CURRENT);
    }
    List<ClusterConnection> connections = Arrays.asList(connection1, connection2);

    return MsgStreamer.create(connections, message, false, stats, pool, true);
  }
}
