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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.command.AddPdxEnum;
import org.apache.geode.internal.cache.tier.sockets.command.AddPdxType;
import org.apache.geode.internal.cache.tier.sockets.command.ClearRegion;
import org.apache.geode.internal.cache.tier.sockets.command.ClientReady;
import org.apache.geode.internal.cache.tier.sockets.command.CloseConnection;
import org.apache.geode.internal.cache.tier.sockets.command.CommitCommand;
import org.apache.geode.internal.cache.tier.sockets.command.ContainsKey66;
import org.apache.geode.internal.cache.tier.sockets.command.CreateRegion;
import org.apache.geode.internal.cache.tier.sockets.command.Destroy70;
import org.apache.geode.internal.cache.tier.sockets.command.DestroyRegion;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction70;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteRegionFunction66;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteRegionFunctionGeode18;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteRegionFunctionSingleHop;
import org.apache.geode.internal.cache.tier.sockets.command.GatewayReceiverCommand;
import org.apache.geode.internal.cache.tier.sockets.command.Get70;
import org.apache.geode.internal.cache.tier.sockets.command.GetAll70;
import org.apache.geode.internal.cache.tier.sockets.command.GetAllWithCallback;
import org.apache.geode.internal.cache.tier.sockets.command.GetClientPRMetadataCommand66;
import org.apache.geode.internal.cache.tier.sockets.command.GetClientPartitionAttributesCommand66;
import org.apache.geode.internal.cache.tier.sockets.command.GetEntry70;
import org.apache.geode.internal.cache.tier.sockets.command.GetFunctionAttribute;
import org.apache.geode.internal.cache.tier.sockets.command.GetPDXEnumById;
import org.apache.geode.internal.cache.tier.sockets.command.GetPDXIdForEnum;
import org.apache.geode.internal.cache.tier.sockets.command.GetPDXIdForType;
import org.apache.geode.internal.cache.tier.sockets.command.GetPDXTypeById;
import org.apache.geode.internal.cache.tier.sockets.command.GetPdxEnums70;
import org.apache.geode.internal.cache.tier.sockets.command.GetPdxTypes70;
import org.apache.geode.internal.cache.tier.sockets.command.Invalid;
import org.apache.geode.internal.cache.tier.sockets.command.Invalidate70;
import org.apache.geode.internal.cache.tier.sockets.command.KeySet;
import org.apache.geode.internal.cache.tier.sockets.command.MakePrimary;
import org.apache.geode.internal.cache.tier.sockets.command.PeriodicAck;
import org.apache.geode.internal.cache.tier.sockets.command.Ping;
import org.apache.geode.internal.cache.tier.sockets.command.Put70;
import org.apache.geode.internal.cache.tier.sockets.command.PutAll80;
import org.apache.geode.internal.cache.tier.sockets.command.PutAllWithCallback;
import org.apache.geode.internal.cache.tier.sockets.command.PutUserCredentials;
import org.apache.geode.internal.cache.tier.sockets.command.Query651;
import org.apache.geode.internal.cache.tier.sockets.command.QueryGeode10;
import org.apache.geode.internal.cache.tier.sockets.command.QueryWithParametersGeode10;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterDataSerializers;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInstantiators;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInterest61;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInterestList66;
import org.apache.geode.internal.cache.tier.sockets.command.RemoveAll;
import org.apache.geode.internal.cache.tier.sockets.command.RemoveUserAuth;
import org.apache.geode.internal.cache.tier.sockets.command.RequestEventValue;
import org.apache.geode.internal.cache.tier.sockets.command.RollbackCommand;
import org.apache.geode.internal.cache.tier.sockets.command.Size;
import org.apache.geode.internal.cache.tier.sockets.command.TXFailoverCommand;
import org.apache.geode.internal.cache.tier.sockets.command.TXSynchronizationCommand;
import org.apache.geode.internal.cache.tier.sockets.command.UnregisterInterest;
import org.apache.geode.internal.cache.tier.sockets.command.UnregisterInterestList;
import org.apache.geode.internal.cache.tier.sockets.command.UpdateClientNotification;
import org.apache.geode.internal.serialization.KnownVersion;

public class CommandInitializerTest {

  @Test
  public void testCommandMapContainsAllVersions() {
    for (KnownVersion productVersion : KnownVersion.getAllVersions()) {
      KnownVersion protocolVersion = productVersion.getClientServerProtocolVersion();
      if (protocolVersion.isNotOlderThan(KnownVersion.OLDEST)) {
        org.junit.Assert.assertNotNull(
            "Please add a command set for " + protocolVersion + " of Geode to CommandInitializer",
            CommandInitializer.getDefaultInstance().get(protocolVersion));
      }
    }
  }

  @Test
  public void initializeGeode18Commands() {
    @SuppressWarnings("unchecked")
    final Map<Integer, Command> commands = mock(Map.class);

    CommandInitializer.initializeGeode18Commands(commands);

    verify(commands).put(MessageType.EXECUTE_REGION_FUNCTION,
        ExecuteRegionFunctionGeode18.getCommand());

    verifyNoMoreInteractions(commands);
  }

  @Test
  public void initializeGfe90Commands() {
    @SuppressWarnings("unchecked")
    final Map<Integer, Command> commands = mock(Map.class);

    CommandInitializer.initializeGfe90Commands(commands);

    verify(commands).put(MessageType.QUERY_WITH_PARAMETERS,
        QueryWithParametersGeode10.getCommand());
    verify(commands).put(MessageType.QUERY, QueryGeode10.getCommand());

    verifyNoMoreInteractions(commands);
  }

  @Test
  public void initializeGfe82Commands() {
    @SuppressWarnings("unchecked")
    final Map<Integer, Command> commands = mock(Map.class);

    CommandInitializer.initializeGfe81Commands(commands);

    verify(commands).put(MessageType.PING, Ping.getCommand());
    verify(commands).put(MessageType.QUERY,
        org.apache.geode.internal.cache.tier.sockets.command.Query.getCommand());
    verify(commands).put(MessageType.CLEAR_REGION, ClearRegion.getCommand());
    verify(commands).put(MessageType.DESTROY_REGION, DestroyRegion.getCommand());
    verify(commands).put(MessageType.UNREGISTER_INTEREST, UnregisterInterest.getCommand());
    verify(commands).put(MessageType.UNREGISTER_INTEREST_LIST, UnregisterInterestList.getCommand());
    verify(commands).put(MessageType.KEY_SET, KeySet.getCommand());
    verify(commands).put(MessageType.CREATE_REGION, CreateRegion.getCommand());
    verify(commands).put(MessageType.MAKE_PRIMARY, MakePrimary.getCommand());
    verify(commands).put(MessageType.PERIODIC_ACK, PeriodicAck.getCommand());
    verify(commands).put(MessageType.REGISTER_INSTANTIATORS, RegisterInstantiators.getCommand());
    verify(commands).put(MessageType.UPDATE_CLIENT_NOTIFICATION,
        UpdateClientNotification.getCommand());
    verify(commands).put(MessageType.CLOSE_CONNECTION, CloseConnection.getCommand());
    verify(commands).put(MessageType.CLIENT_READY, ClientReady.getCommand());
    verify(commands).put(MessageType.INVALID, Invalid.getCommand());

    verify(commands).put(MessageType.REGISTER_INTEREST, RegisterInterest61.getCommand());
    verify(commands).put(MessageType.REQUEST_EVENT_VALUE, RequestEventValue.getCommand());
    verify(commands).put(MessageType.REGISTER_DATASERIALIZERS,
        RegisterDataSerializers.getCommand());

    verify(commands).put(MessageType.USER_CREDENTIAL_MESSAGE, PutUserCredentials.getCommand());
    verify(commands).put(MessageType.REMOVE_USER_AUTH, RemoveUserAuth.getCommand());
    verify(commands).put(MessageType.EXECUTE_REGION_FUNCTION_SINGLE_HOP,
        ExecuteRegionFunctionSingleHop.getCommand());

    verify(commands).put(MessageType.QUERY_WITH_PARAMETERS, Query651.getCommand());

    verify(commands).put(MessageType.GET_CLIENT_PR_METADATA,
        GetClientPRMetadataCommand66.getCommand());

    verify(commands).put(MessageType.ADD_PDX_TYPE, AddPdxType.getCommand());
    verify(commands).put(MessageType.GET_PDX_ID_FOR_TYPE, GetPDXIdForType.getCommand());
    verify(commands).put(MessageType.GET_PDX_TYPE_BY_ID, GetPDXTypeById.getCommand());
    verify(commands).put(MessageType.SIZE, Size.getCommand());
    verify(commands).put(MessageType.COMMIT, CommitCommand.getCommand());
    verify(commands).put(MessageType.ROLLBACK, RollbackCommand.getCommand());
    verify(commands).put(MessageType.TX_FAILOVER, TXFailoverCommand.getCommand());
    verify(commands).put(MessageType.TX_SYNCHRONIZATION, TXSynchronizationCommand.getCommand());
    verify(commands).put(MessageType.GET_CLIENT_PARTITION_ATTRIBUTES,
        GetClientPartitionAttributesCommand66.getCommand());
    verify(commands).put(MessageType.REGISTER_INTEREST_LIST, RegisterInterestList66.getCommand());
    verify(commands).put(MessageType.GET_FUNCTION_ATTRIBUTES, GetFunctionAttribute.getCommand());
    verify(commands).put(MessageType.EXECUTE_REGION_FUNCTION, ExecuteRegionFunction66.getCommand());
    verify(commands).put(MessageType.GATEWAY_RECEIVER_COMMAND, GatewayReceiverCommand.getCommand());
    verify(commands).put(MessageType.CONTAINS_KEY, ContainsKey66.getCommand());

    verify(commands).put(MessageType.ADD_PDX_ENUM, AddPdxEnum.getCommand());
    verify(commands).put(MessageType.GET_PDX_ID_FOR_ENUM, GetPDXIdForEnum.getCommand());
    verify(commands).put(MessageType.GET_PDX_ENUM_BY_ID, GetPDXEnumById.getCommand());

    verify(commands).put(MessageType.REQUEST, Get70.getCommand());
    verify(commands).put(MessageType.GET_ENTRY, GetEntry70.getCommand());
    verify(commands).put(MessageType.GET_ALL_70, GetAll70.getCommand());
    verify(commands).put(MessageType.PUT, Put70.getCommand());
    verify(commands).put(MessageType.DESTROY, Destroy70.getCommand());
    verify(commands).put(MessageType.INVALIDATE, Invalidate70.getCommand());
    verify(commands).put(MessageType.GET_PDX_TYPES, GetPdxTypes70.getCommand());
    verify(commands).put(MessageType.GET_PDX_ENUMS, GetPdxEnums70.getCommand());
    verify(commands).put(MessageType.EXECUTE_FUNCTION, ExecuteFunction70.getCommand());

    verify(commands).put(MessageType.PUTALL, PutAll80.getCommand());
    verify(commands).put(MessageType.GET_ALL_WITH_CALLBACK, GetAllWithCallback.getCommand());
    verify(commands).put(MessageType.PUT_ALL_WITH_CALLBACK, PutAllWithCallback.getCommand());
    verify(commands).put(MessageType.REMOVE_ALL, RemoveAll.getCommand());

    verifyNoMoreInteractions(commands);
  }

  @Test
  public void commandMapUnmodifiable() {
    final CommandInitializer commandInitializer = new CommandInitializer();
    final Map<Integer, Command> commands =
        commandInitializer.get(KnownVersion.CURRENT.getClientServerProtocolVersion());
    assertThatThrownBy(() -> commands.put(1, Put70.getCommand()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void newlyRegisteredCommandsVisibleInCommandMap() {
    final Command command = mock(Command.class);
    Map<KnownVersion, Command> newCommandMap = new HashMap<>();
    newCommandMap.put(KnownVersion.CURRENT.getClientServerProtocolVersion(), command);

    final CommandInitializer commandInitializer = new CommandInitializer();
    final Map<Integer, Command> commands =
        commandInitializer.get(KnownVersion.CURRENT.getClientServerProtocolVersion());
    assertThat(commands).doesNotContainKeys(-2);
    commandInitializer.register(-2, newCommandMap);
    assertThat(commands).containsEntry(-2, command);
  }

}
