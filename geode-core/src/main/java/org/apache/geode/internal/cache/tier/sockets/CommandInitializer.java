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

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.Immutable;
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

/**
 * A <code>CommandInitializer</code> maintains version specific commands map.
 *
 * @since GemFire 5.7
 */

public class CommandInitializer {

  // Not truly Immutable given that registerCommand can mutate after initialization.
  @Immutable
  static final Map<KnownVersion, Map<Integer, Command>> ALL_COMMANDS = initializeAllCommands();

  /**
   * Register a new command with the system.
   *
   * @param messageType - An ordinal for this message. This must be something defined in MessageType
   *        that has not already been allocated to a different command.
   * @param versionToNewCommand The command to register, for different versions. The key is the
   *        earliest version for which this command class is valid (starting with GFE_57). The value
   */
  public static void registerCommand(int messageType,
      Map<KnownVersion, Command> versionToNewCommand) {
    if (!registerCommand(messageType, versionToNewCommand, ALL_COMMANDS)) {
      throw new InternalGemFireError(String.format("Message %d was not registered.", messageType));
    }
  }

  /***
   * Iterate through all the Geode versions add a command to the map for that version
   */
  static boolean registerCommand(final int messageType,
      final Map<KnownVersion, Command> versionToNewCommand,
      final Map<KnownVersion, Map<Integer, Command>> allCommands) {
    boolean modified = false;
    Command command = null;

    for (Map.Entry<KnownVersion, Map<Integer, Command>> entry : allCommands.entrySet()) {
      KnownVersion version = entry.getKey();

      // Get the current set of commands for this version.
      Map<Integer, Command> commandMap = entry.getValue();

      // See if we have a new command to insert into this map. Otherwise, keep using the command we
      // have already read
      Command newCommand = versionToNewCommand.get(version);
      if (newCommand != null) {
        command = newCommand;
      }
      if (command != null) {
        Command oldCommand = commandMap.get(messageType);
        if (oldCommand != null && oldCommand != command) {
          throw new InternalGemFireError("Command is already defined int the map for message Type "
              + MessageType.getString(messageType) + ". Old Value=" + commandMap.get(messageType)
              + ", newValue=" + command + ", version=" + version);
        }
        commandMap.put(messageType, command);
        modified = true;
      }
    }
    return modified;
  }

  private static Map<KnownVersion, Map<Integer, Command>> initializeAllCommands() {
    final LinkedHashMap<KnownVersion, Map<Integer, Command>> allCommands =
        new LinkedHashMap<>();

    final Map<Integer, Command> gfe82Commands = buildGfe82Commands();
    allCommands.put(KnownVersion.GFE_82, gfe82Commands);

    final Map<Integer, Command> gfe90Commands =
        buildGfe90Commands(allCommands.get(KnownVersion.GFE_82));
    allCommands.put(KnownVersion.GFE_90, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_1_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_1_1, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_2_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_3_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_4_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_5_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_6_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_7_0, gfe90Commands);

    final Map<Integer, Command> geode18Commands =
        buildGeode18Commands(allCommands.get(KnownVersion.GEODE_1_7_0));
    allCommands.put(KnownVersion.GEODE_1_8_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_9_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_10_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_11_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_12_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_12_1, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_13_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_13_1, geode18Commands);

    allCommands.put(KnownVersion.GEODE_1_14_0, geode18Commands);

    return unmodifiableMap(allCommands);
  }

  private static Map<Integer, Command> buildGeode18Commands(
      final Map<Integer, Command> baseCommands) {
    final Map<Integer, Command> commands = new HashMap<>(baseCommands);
    initializeGeode18Commands(commands);
    return commands;
  }

  private static Map<Integer, Command> buildGfe90Commands(
      final Map<Integer, Command> baseCommands) {
    final Map<Integer, Command> commands = new HashMap<>(baseCommands);
    initializeGfe90Commands(commands);
    return commands;
  }

  private static Map<Integer, Command> buildGfe82Commands() {
    final Map<Integer, Command> commands = new HashMap<>();
    initializeGfe82Commands(commands);
    return commands;
  }

  static void initializeGeode18Commands(final Map<Integer, Command> commands) {
    commands.put(MessageType.EXECUTE_REGION_FUNCTION, ExecuteRegionFunctionGeode18.getCommand());
  }

  static void initializeGfe90Commands(final Map<Integer, Command> commands) {
    commands.put(MessageType.QUERY_WITH_PARAMETERS, QueryWithParametersGeode10.getCommand());
    commands.put(MessageType.QUERY, QueryGeode10.getCommand());
  }

  static void initializeGfe82Commands(final Map<Integer, Command> commands) {
    commands.put(MessageType.PING, Ping.getCommand());
    commands.put(MessageType.QUERY,
        org.apache.geode.internal.cache.tier.sockets.command.Query.getCommand());
    commands.put(MessageType.CLEAR_REGION, ClearRegion.getCommand());
    commands.put(MessageType.DESTROY_REGION, DestroyRegion.getCommand());
    commands.put(MessageType.UNREGISTER_INTEREST, UnregisterInterest.getCommand());
    commands.put(MessageType.UNREGISTER_INTEREST_LIST, UnregisterInterestList.getCommand());
    commands.put(MessageType.KEY_SET, KeySet.getCommand());
    commands.put(MessageType.CREATE_REGION, CreateRegion.getCommand());
    commands.put(MessageType.MAKE_PRIMARY, MakePrimary.getCommand());
    commands.put(MessageType.PERIODIC_ACK, PeriodicAck.getCommand());
    commands.put(MessageType.REGISTER_INSTANTIATORS, RegisterInstantiators.getCommand());
    commands.put(MessageType.UPDATE_CLIENT_NOTIFICATION,
        UpdateClientNotification.getCommand());
    commands.put(MessageType.CLOSE_CONNECTION, CloseConnection.getCommand());
    commands.put(MessageType.CLIENT_READY, ClientReady.getCommand());
    commands.put(MessageType.INVALID, Invalid.getCommand());

    commands.put(MessageType.REGISTER_INTEREST, RegisterInterest61.getCommand());
    commands.put(MessageType.REQUEST_EVENT_VALUE, RequestEventValue.getCommand());
    commands.put(MessageType.REGISTER_DATASERIALIZERS, RegisterDataSerializers.getCommand());

    commands.put(MessageType.USER_CREDENTIAL_MESSAGE, PutUserCredentials.getCommand());
    commands.put(MessageType.REMOVE_USER_AUTH, RemoveUserAuth.getCommand());
    commands.put(MessageType.EXECUTE_REGION_FUNCTION_SINGLE_HOP,
        ExecuteRegionFunctionSingleHop.getCommand());

    commands.put(MessageType.QUERY_WITH_PARAMETERS, Query651.getCommand());

    commands.put(MessageType.GET_CLIENT_PR_METADATA, GetClientPRMetadataCommand66.getCommand());

    commands.put(MessageType.ADD_PDX_TYPE, AddPdxType.getCommand());
    commands.put(MessageType.GET_PDX_ID_FOR_TYPE, GetPDXIdForType.getCommand());
    commands.put(MessageType.GET_PDX_TYPE_BY_ID, GetPDXTypeById.getCommand());
    commands.put(MessageType.SIZE, Size.getCommand());
    commands.put(MessageType.COMMIT, CommitCommand.getCommand());
    commands.put(MessageType.ROLLBACK, RollbackCommand.getCommand());
    commands.put(MessageType.TX_FAILOVER, TXFailoverCommand.getCommand());
    commands.put(MessageType.TX_SYNCHRONIZATION, TXSynchronizationCommand.getCommand());
    commands.put(MessageType.GET_CLIENT_PARTITION_ATTRIBUTES,
        GetClientPartitionAttributesCommand66.getCommand());
    commands.put(MessageType.REGISTER_INTEREST_LIST, RegisterInterestList66.getCommand());
    commands.put(MessageType.GET_FUNCTION_ATTRIBUTES, GetFunctionAttribute.getCommand());
    commands.put(MessageType.EXECUTE_REGION_FUNCTION, ExecuteRegionFunction66.getCommand());
    commands.put(MessageType.GATEWAY_RECEIVER_COMMAND, GatewayReceiverCommand.getCommand());
    commands.put(MessageType.CONTAINS_KEY, ContainsKey66.getCommand());

    commands.put(MessageType.ADD_PDX_ENUM, AddPdxEnum.getCommand());
    commands.put(MessageType.GET_PDX_ID_FOR_ENUM, GetPDXIdForEnum.getCommand());
    commands.put(MessageType.GET_PDX_ENUM_BY_ID, GetPDXEnumById.getCommand());

    commands.put(MessageType.REQUEST, Get70.getCommand());
    commands.put(MessageType.GET_ENTRY, GetEntry70.getCommand());
    commands.put(MessageType.GET_ALL_70, GetAll70.getCommand());
    commands.put(MessageType.PUT, Put70.getCommand());
    commands.put(MessageType.DESTROY, Destroy70.getCommand());
    commands.put(MessageType.INVALIDATE, Invalidate70.getCommand());
    commands.put(MessageType.GET_PDX_TYPES, GetPdxTypes70.getCommand());
    commands.put(MessageType.GET_PDX_ENUMS, GetPdxEnums70.getCommand());
    commands.put(MessageType.EXECUTE_FUNCTION, ExecuteFunction70.getCommand());

    commands.put(MessageType.PUTALL, PutAll80.getCommand());
    commands.put(MessageType.GET_ALL_WITH_CALLBACK, GetAllWithCallback.getCommand());
    commands.put(MessageType.PUT_ALL_WITH_CALLBACK, PutAllWithCallback.getCommand());
    commands.put(MessageType.REMOVE_ALL, RemoveAll.getCommand());
  }

  public static Map<Integer, Command> getCommands(KnownVersion version) {
    return unmodifiableMap(ALL_COMMANDS.get(version));
  }

  public static Map<Integer, Command> getCommands(ServerConnection connection) {
    return getCommands(connection.getClientVersion());
  }
}
