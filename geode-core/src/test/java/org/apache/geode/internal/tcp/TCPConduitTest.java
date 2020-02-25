/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.tcp;

import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.alerting.internal.spi.AlertingAction;
import org.apache.geode.alerting.internal.spi.AlertingIOException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SocketCreator;

public class TCPConduitTest {

  private Membership<InternalDistributedMember> membership;
  private DirectChannel directChannel;
  private InetAddress localHost;
  private ConnectionTable connectionTable;
  private SocketCreator socketCreator;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() throws Exception {
    membership = cast(mock(Membership.class));
    directChannel = mock(DirectChannel.class);
    connectionTable = mock(ConnectionTable.class);
    socketCreator = mock(SocketCreator.class);
    localHost = LocalHostUtil.getLocalHost();

    when(directChannel.getDM())
        .thenReturn(mock(DistributionManager.class));
  }

  @Test
  public void getConnectionThrowsAlertingIOException_ifCaughtIOException_whileAlerting()
      throws Exception {
    TCPConduit tcpConduit =
        new TCPConduit(membership, 0, localHost, false, directChannel, new Properties(),
            TCPConduit -> connectionTable, socketCreator, doNothing(), false);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    doThrow(new IOException("Cannot form connection to alert listener"))
        .when(connectionTable).get(eq(member), anyBoolean(), anyLong(), anyLong(), anyLong());
    when(membership.memberExists(eq(member)))
        .thenReturn(true);
    when(membership.isShunned(same(member)))
        .thenReturn(false);

    AlertingAction.execute(() -> {
      Throwable thrown = catchThrowable(() -> {
        tcpConduit.getConnection(member, false, false, 0L, 0L, 0L);
      });

      assertThat(thrown)
          .isInstanceOf(AlertingIOException.class);
    });
  }

  @Test
  public void getConnectionRethrows_ifCaughtIOException_whileNotAlerting() throws Exception {
    TCPConduit tcpConduit =
        new TCPConduit(membership, 0, localHost, false, directChannel, new Properties(),
            TCPConduit -> connectionTable, socketCreator, doNothing(), false);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    Connection connection = mock(Connection.class);
    when(connection.getRemoteAddress())
        .thenReturn(member);
    doThrow(new IOException("Cannot form connection to alert listener"))
        // getConnection will loop indefinitely until connectionTable returns connection
        .doReturn(connection)
        .when(connectionTable).get(eq(member), anyBoolean(), anyLong(), anyLong(), anyLong());
    when(membership.memberExists(eq(member)))
        .thenReturn(true);
    when(membership.isShunned(same(member)))
        .thenReturn(false);

    Connection value = tcpConduit.getConnection(member, false, false, 0L, 0L, 0L);

    assertThat(value)
        .isSameAs(connection);
  }

  @Test
  public void getConnectionRethrows_ifCaughtIOException_whenMemberDoesNotExist() throws Exception {
    TCPConduit tcpConduit =
        new TCPConduit(membership, 0, localHost, false, directChannel, new Properties(),
            TCPConduit -> connectionTable, socketCreator, doNothing(), false);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    doThrow(new IOException("Cannot form connection to alert listener"))
        .when(connectionTable).get(eq(member), anyBoolean(), anyLong(), anyLong(), anyLong());
    when(membership.memberExists(eq(member)))
        .thenReturn(false);

    Throwable thrown = catchThrowable(() -> {
      tcpConduit.getConnection(member, false, false, 0L, 0L, 0L);
    });

    assertThat(thrown)
        .isInstanceOf(IOException.class)
        .isNotInstanceOf(AlertingIOException.class);
  }

  @Test
  public void getConnectionRethrows_ifCaughtIOException_whenMemberIsShunned() throws Exception {
    TCPConduit tcpConduit =
        new TCPConduit(membership, 0, localHost, false, directChannel, new Properties(),
            TCPConduit -> connectionTable, socketCreator, doNothing(), false);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    doThrow(new IOException("Cannot form connection to alert listener"))
        .when(connectionTable).get(same(member), anyBoolean(), anyLong(), anyLong(), anyLong());
    when(membership.memberExists(same(member)))
        .thenReturn(true);
    when(membership.isShunned(same(member)))
        .thenReturn(true);

    Throwable thrown = catchThrowable(() -> {
      tcpConduit.getConnection(member, false, false, 0L, 0L, 0L);
    });

    assertThat(thrown)
        .isInstanceOf(IOException.class)
        .isNotInstanceOf(AlertingIOException.class);
  }

  @Test
  public void getConnectionThrowsDistributedSystemDisconnectedException_ifCaughtIOException_whenShutdownIsInProgress()
      throws Exception {
    TCPConduit tcpConduit =
        new TCPConduit(membership, 0, localHost, false, directChannel, new Properties(),
            TCPConduit -> connectionTable, socketCreator, doNothing(), false);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    doThrow(new IOException("Cannot form connection to alert listener"))
        .when(connectionTable).get(same(member), anyBoolean(), anyLong(), anyLong(), anyLong());
    when(membership.memberExists(same(member)))
        .thenReturn(true);
    when(membership.isShunned(same(member)))
        .thenReturn(false);
    when(membership.shutdownInProgress())
        .thenReturn(true);

    Throwable thrown = catchThrowable(() -> {
      tcpConduit.getConnection(member, false, false, 0L, 0L, 0L);
    });

    assertThat(thrown)
        .isInstanceOf(DistributedSystemDisconnectedException.class)
        .hasMessage("Abandoned because shutdown is in progress");
  }

  @Test
  public void getConnectionThrowsDistributedSystemDisconnectedException_ifCaughtIOException_whenShutdownIsInProgress_andCancelIsInProgress()
      throws Exception {
    TCPConduit tcpConduit =
        new TCPConduit(membership, 0, localHost, false, directChannel, new Properties(),
            TCPConduit -> connectionTable, socketCreator, doNothing(), false);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    doThrow(new IOException("Cannot form connection to alert listener"))
        .when(connectionTable).get(same(member), anyBoolean(), anyLong(), anyLong(), anyLong());
    when(membership.memberExists(same(member)))
        .thenReturn(true);
    when(membership.isShunned(same(member)))
        .thenReturn(false);
    when(membership.shutdownInProgress())
        .thenReturn(true);

    Throwable thrown = catchThrowable(() -> {
      tcpConduit.getConnection(member, false, false, 0L, 0L, 0L);
    });

    assertThat(thrown)
        .isInstanceOf(DistributedSystemDisconnectedException.class)
        .hasMessage("Abandoned because shutdown is in progress");
  }

  private Runnable doNothing() {
    return () -> {
      // nothing
    };
  }
}
