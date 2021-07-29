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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.internal.MakeNotStatic;
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

public class CommandInitializer implements CommandRegistry {

  @Deprecated
  @MakeNotStatic
  static final CommandInitializer instance = new CommandInitializer();

  /**
   * Gets legacy singleton instance.
   *
   * @deprecated Efforts should be made to get and instance from the cache or other object.
   *
   * @return legacy singleton instance. Instance is not immutable.
   */
  @Deprecated
  public static CommandInitializer getDefaultInstance() {
    return instance;
  }

  final Map<KnownVersion, Map<Integer, Command>> unmodifiableRegisteredCommands;
  final LinkedHashMap<KnownVersion, ConcurrentMap<Integer, Command>> modifiableRegisteredCommands;

  public CommandInitializer() {
    modifiableRegisteredCommands = initializeAllCommands();
    unmodifiableRegisteredCommands = makeUnmodifiable(modifiableRegisteredCommands);
  }

  @Override
  public void register(int messageType,
      Map<KnownVersion, Command> versionToNewCommand) {
    if (!registerCommand(messageType, versionToNewCommand, modifiableRegisteredCommands)) {
      throw new InternalGemFireError(String.format("Message %d was not registered.", messageType));
    }
  }

  /**
   * Gets the command map for a given version.
   *
   * @param version of command map to return.
   *
   * @return immutable {@link Map} for {@link MessageType} to {@link Command}.
   */
  @Override
  public Map<Integer, Command> get(final KnownVersion version) {
    return unmodifiableRegisteredCommands.get(version);
  }

  /**
   * Iterate through all the Geode versions add a command to the map for that version
   *
   * @return returns true if command was registered or same command was already registered,
   *         otherwise false.
   * @throws InternalGemFireError if a different command was already registered.
   */
  boolean registerCommand(final int messageType,
      final Map<KnownVersion, Command> versionToNewCommand,
      final LinkedHashMap<KnownVersion, ConcurrentMap<Integer, Command>> allCommands) {
    boolean modified = false;
    Command command = null;

    for (Map.Entry<KnownVersion, ConcurrentMap<Integer, Command>> entry : allCommands.entrySet()) {
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

  private static LinkedHashMap<KnownVersion, ConcurrentMap<Integer, Command>> initializeAllCommands() {
    final LinkedHashMap<KnownVersion, ConcurrentMap<Integer, Command>> allCommands =
        new LinkedHashMap<>();

    final ConcurrentMap<Integer, Command> gfe81Commands = buildGfe81Commands();
    allCommands.put(KnownVersion.GFE_81, gfe81Commands);

    final ConcurrentMap<Integer, Command> gfe90Commands =
        buildGfe90Commands(allCommands.get(KnownVersion.GFE_81));
    allCommands.put(KnownVersion.GFE_90, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_1_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_1_1, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_2_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_3_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_4_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_5_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_6_0, gfe90Commands);
    allCommands.put(KnownVersion.GEODE_1_7_0, gfe90Commands);

    final ConcurrentMap<Integer, Command> geode18Commands =
        buildGeode18Commands(allCommands.get(KnownVersion.GEODE_1_7_0));
    allCommands.put(KnownVersion.GEODE_1_8_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_9_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_10_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_11_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_12_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_12_1, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_13_0, geode18Commands);
    allCommands.put(KnownVersion.GEODE_1_13_2, geode18Commands);

    allCommands.put(KnownVersion.GEODE_1_14_0, geode18Commands);

    // as of GEODE_1_14_0 we only create new command sets when the
    // client/server protocol changes

    return allCommands;
  }

  private static ConcurrentMap<Integer, Command> buildGeode18Commands(
      final ConcurrentMap<Integer, Command> baseCommands) {
    final ConcurrentMap<Integer, Command> commands = new ConcurrentHashMap<>(baseCommands);
    initializeGeode18Commands(commands);
    return commands;
  }

  private static ConcurrentMap<Integer, Command> buildGfe90Commands(
      final ConcurrentMap<Integer, Command> baseCommands) {
    final ConcurrentMap<Integer, Command> commands = new ConcurrentHashMap<>(baseCommands);
    initializeGfe90Commands(commands);
    return commands;
  }

  private static ConcurrentMap<Integer, Command> buildGfe81Commands() {
    final ConcurrentMap<Integer, Command> commands = new ConcurrentHashMap<>();
    initializeGfe81Commands(commands);
    return commands;
  }

  static void initializeGeode18Commands(final Map<Integer, Command> commands) {
    commands.put(MessageType.EXECUTE_REGION_FUNCTION, ExecuteRegionFunctionGeode18.getCommand());
  }

  static void initializeGfe90Commands(final Map<Integer, Command> commands) {
    commands.put(MessageType.QUERY_WITH_PARAMETERS, QueryWithParametersGeode10.getCommand());
    commands.put(MessageType.QUERY, QueryGeode10.getCommand());
  }

  static void initializeGfe81Commands(final Map<Integer, Command> commands) {
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

  static Map<KnownVersion, Map<Integer, Command>> makeUnmodifiable(
      final Map<KnownVersion, ConcurrentMap<Integer, Command>> modifiableMap) {
    final Map<KnownVersion, Map<Integer, Command>> unmodifiableMap =
        new LinkedHashMap<>(modifiableMap.size());
    for (Map.Entry<KnownVersion, ConcurrentMap<Integer, Command>> e : modifiableMap.entrySet()) {
      unmodifiableMap.put(e.getKey(), unmodifiableMap(e.getValue()));
    }
    return unmodifiableMap(unmodifiableMap);
  }

}
