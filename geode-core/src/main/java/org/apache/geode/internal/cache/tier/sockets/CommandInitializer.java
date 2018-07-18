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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.command.AddPdxEnum;
import org.apache.geode.internal.cache.tier.sockets.command.AddPdxType;
import org.apache.geode.internal.cache.tier.sockets.command.ClearRegion;
import org.apache.geode.internal.cache.tier.sockets.command.ClientReady;
import org.apache.geode.internal.cache.tier.sockets.command.CloseConnection;
import org.apache.geode.internal.cache.tier.sockets.command.CommitCommand;
import org.apache.geode.internal.cache.tier.sockets.command.ContainsKey;
import org.apache.geode.internal.cache.tier.sockets.command.ContainsKey66;
import org.apache.geode.internal.cache.tier.sockets.command.CreateRegion;
import org.apache.geode.internal.cache.tier.sockets.command.Destroy;
import org.apache.geode.internal.cache.tier.sockets.command.Destroy65;
import org.apache.geode.internal.cache.tier.sockets.command.Destroy70;
import org.apache.geode.internal.cache.tier.sockets.command.DestroyRegion;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction65;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction66;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction70;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteRegionFunction;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteRegionFunction65;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteRegionFunction66;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteRegionFunctionSingleHop;
import org.apache.geode.internal.cache.tier.sockets.command.GatewayReceiverCommand;
import org.apache.geode.internal.cache.tier.sockets.command.Get70;
import org.apache.geode.internal.cache.tier.sockets.command.GetAll;
import org.apache.geode.internal.cache.tier.sockets.command.GetAll651;
import org.apache.geode.internal.cache.tier.sockets.command.GetAll70;
import org.apache.geode.internal.cache.tier.sockets.command.GetAllForRI;
import org.apache.geode.internal.cache.tier.sockets.command.GetAllWithCallback;
import org.apache.geode.internal.cache.tier.sockets.command.GetClientPRMetadataCommand;
import org.apache.geode.internal.cache.tier.sockets.command.GetClientPRMetadataCommand66;
import org.apache.geode.internal.cache.tier.sockets.command.GetClientPartitionAttributesCommand;
import org.apache.geode.internal.cache.tier.sockets.command.GetClientPartitionAttributesCommand66;
import org.apache.geode.internal.cache.tier.sockets.command.GetEntry70;
import org.apache.geode.internal.cache.tier.sockets.command.GetEntryCommand;
import org.apache.geode.internal.cache.tier.sockets.command.GetFunctionAttribute;
import org.apache.geode.internal.cache.tier.sockets.command.GetPDXEnumById;
import org.apache.geode.internal.cache.tier.sockets.command.GetPDXIdForEnum;
import org.apache.geode.internal.cache.tier.sockets.command.GetPDXIdForType;
import org.apache.geode.internal.cache.tier.sockets.command.GetPDXTypeById;
import org.apache.geode.internal.cache.tier.sockets.command.GetPdxEnums70;
import org.apache.geode.internal.cache.tier.sockets.command.GetPdxTypes70;
import org.apache.geode.internal.cache.tier.sockets.command.Invalid;
import org.apache.geode.internal.cache.tier.sockets.command.Invalidate;
import org.apache.geode.internal.cache.tier.sockets.command.Invalidate70;
import org.apache.geode.internal.cache.tier.sockets.command.KeySet;
import org.apache.geode.internal.cache.tier.sockets.command.MakePrimary;
import org.apache.geode.internal.cache.tier.sockets.command.PeriodicAck;
import org.apache.geode.internal.cache.tier.sockets.command.Ping;
import org.apache.geode.internal.cache.tier.sockets.command.Put;
import org.apache.geode.internal.cache.tier.sockets.command.Put61;
import org.apache.geode.internal.cache.tier.sockets.command.Put65;
import org.apache.geode.internal.cache.tier.sockets.command.Put70;
import org.apache.geode.internal.cache.tier.sockets.command.PutAll;
import org.apache.geode.internal.cache.tier.sockets.command.PutAll70;
import org.apache.geode.internal.cache.tier.sockets.command.PutAll80;
import org.apache.geode.internal.cache.tier.sockets.command.PutAllWithCallback;
import org.apache.geode.internal.cache.tier.sockets.command.PutUserCredentials;
import org.apache.geode.internal.cache.tier.sockets.command.Query651;
import org.apache.geode.internal.cache.tier.sockets.command.QueryGeode10;
import org.apache.geode.internal.cache.tier.sockets.command.QueryWithParametersGeode10;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterDataSerializers;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInstantiators;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInterest;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInterest61;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInterestList;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInterestList61;
import org.apache.geode.internal.cache.tier.sockets.command.RegisterInterestList66;
import org.apache.geode.internal.cache.tier.sockets.command.RemoveAll;
import org.apache.geode.internal.cache.tier.sockets.command.RemoveUserAuth;
import org.apache.geode.internal.cache.tier.sockets.command.Request;
import org.apache.geode.internal.cache.tier.sockets.command.RequestEventValue;
import org.apache.geode.internal.cache.tier.sockets.command.RollbackCommand;
import org.apache.geode.internal.cache.tier.sockets.command.Size;
import org.apache.geode.internal.cache.tier.sockets.command.TXFailoverCommand;
import org.apache.geode.internal.cache.tier.sockets.command.TXSynchronizationCommand;
import org.apache.geode.internal.cache.tier.sockets.command.UnregisterInterest;
import org.apache.geode.internal.cache.tier.sockets.command.UnregisterInterestList;
import org.apache.geode.internal.cache.tier.sockets.command.UpdateClientNotification;

/**
 * A <code>CommandInitializer</code> maintains version specific commands map.
 *
 * @since GemFire 5.7
 */

public class CommandInitializer {

  static Map<Version, Map<Integer, Command>> ALL_COMMANDS;

  static {
    initializeAllCommands();
  }

  /**
   * Register a new command with the system.
   *
   * @param messageType - An ordinal for this message. This must be something defined in MessageType
   *        that has not already been allocated to a different command.
   * @param versionToNewCommand The command to register, for different versions. The key is the
   *        earliest version for which this command class is valid (starting with GFE_57). The value
   *        is the command object for clients starting with that version.
   */
  public static void registerCommand(int messageType, Map<Version, Command> versionToNewCommand) {
    Command command = null;
    // Iterate through all the gemfire versions, and
    // add a command to the map for that version
    for (Map.Entry<Version, Map<Integer, Command>> entry : ALL_COMMANDS.entrySet()) {
      Version version = entry.getKey();

      // Get the current set of commands for this version.
      Map<Integer, Command> commandMap = entry.getValue();

      // See if we have a new command to insert into this map. Otherwise, keep using the command we
      // have
      // already read
      Command newerVersion = versionToNewCommand.get(version);
      if (newerVersion != null) {
        command = newerVersion;
      }
      if (command != null) {
        Command oldCommand = commandMap.get(messageType);
        if (oldCommand != null && oldCommand != command) {
          throw new InternalGemFireError("Command is already defined int the map for message Type "
              + MessageType.getString(messageType) + ". Old Value=" + commandMap.get(messageType)
              + ", newValue=" + command + ", version=" + version);
        }
        commandMap.put(messageType, command);
      }
    }
  }

  private static void initializeAllCommands() {
    ALL_COMMANDS = new LinkedHashMap<Version, Map<Integer, Command>>();

    ALL_COMMANDS.put(Version.GFE_56, new HashMap<>());

    // Initialize the GFE 5.7 commands
    Map<Integer, Command> gfe57Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_57, gfe57Commands);
    gfe57Commands.put(MessageType.PING, Ping.getCommand());
    gfe57Commands.put(MessageType.REQUEST, Request.getCommand());
    gfe57Commands.put(MessageType.PUT, Put.getCommand());
    gfe57Commands.put(MessageType.PUTALL, PutAll.getCommand());
    gfe57Commands.put(MessageType.DESTROY, Destroy.getCommand());
    gfe57Commands.put(MessageType.QUERY,
        org.apache.geode.internal.cache.tier.sockets.command.Query.getCommand());
    gfe57Commands.put(MessageType.CLEAR_REGION, ClearRegion.getCommand());
    gfe57Commands.put(MessageType.DESTROY_REGION, DestroyRegion.getCommand());
    gfe57Commands.put(MessageType.REGISTER_INTEREST, RegisterInterest.getCommand());
    gfe57Commands.put(MessageType.UNREGISTER_INTEREST, UnregisterInterest.getCommand());
    gfe57Commands.put(MessageType.REGISTER_INTEREST_LIST, RegisterInterestList.getCommand());
    gfe57Commands.put(MessageType.UNREGISTER_INTEREST_LIST, UnregisterInterestList.getCommand());
    gfe57Commands.put(MessageType.KEY_SET, KeySet.getCommand());
    gfe57Commands.put(MessageType.CONTAINS_KEY, ContainsKey.getCommand());
    gfe57Commands.put(MessageType.CREATE_REGION, CreateRegion.getCommand());
    gfe57Commands.put(MessageType.MAKE_PRIMARY, MakePrimary.getCommand());
    gfe57Commands.put(MessageType.PERIODIC_ACK, PeriodicAck.getCommand());
    gfe57Commands.put(MessageType.REGISTER_INSTANTIATORS, RegisterInstantiators.getCommand());
    gfe57Commands.put(MessageType.UPDATE_CLIENT_NOTIFICATION,
        UpdateClientNotification.getCommand());
    gfe57Commands.put(MessageType.CLOSE_CONNECTION, CloseConnection.getCommand());
    gfe57Commands.put(MessageType.CLIENT_READY, ClientReady.getCommand());
    gfe57Commands.put(MessageType.INVALID, Invalid.getCommand());


    gfe57Commands.put(MessageType.GET_ALL, GetAll.getCommand());

    // Initialize the GFE 5.8 commands example
    Map<Integer, Command> gfe58Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_58, gfe58Commands);
    gfe58Commands.putAll(ALL_COMMANDS.get(Version.GFE_57));
    gfe58Commands.put(MessageType.EXECUTE_REGION_FUNCTION, ExecuteRegionFunction.getCommand());
    gfe58Commands.put(MessageType.EXECUTE_FUNCTION, ExecuteFunction.getCommand());

    // Initialize the GFE 6.0.3 commands map
    Map<Integer, Command> gfe603Commands = new HashMap<Integer, Command>();
    gfe603Commands.putAll(ALL_COMMANDS.get(Version.GFE_58));
    ALL_COMMANDS.put(Version.GFE_603, gfe603Commands);

    // Initialize the GFE 6.1 commands
    Map<Integer, Command> gfe61Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_61, gfe61Commands);
    gfe61Commands.putAll(ALL_COMMANDS.get(Version.GFE_603));
    gfe61Commands.put(MessageType.REGISTER_INTEREST, RegisterInterest61.getCommand());
    gfe61Commands.put(MessageType.REGISTER_INTEREST_LIST, RegisterInterestList61.getCommand());
    gfe61Commands.put(MessageType.REQUEST_EVENT_VALUE, RequestEventValue.getCommand());
    gfe61Commands.put(MessageType.PUT, Put61.getCommand());
    gfe61Commands.put(MessageType.REGISTER_DATASERIALIZERS, RegisterDataSerializers.getCommand());

    // Initialize the GFE 6.5 commands
    Map<Integer, Command> gfe65Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_65, gfe65Commands);
    gfe65Commands.putAll(ALL_COMMANDS.get(Version.GFE_61));
    gfe65Commands.put(MessageType.DESTROY, Destroy65.getCommand());
    gfe65Commands.put(MessageType.PUT, Put65.getCommand());
    gfe65Commands.put(MessageType.EXECUTE_REGION_FUNCTION, ExecuteRegionFunction65.getCommand());
    gfe65Commands.put(MessageType.EXECUTE_FUNCTION, ExecuteFunction65.getCommand());
    gfe65Commands.put(MessageType.GET_CLIENT_PR_METADATA, GetClientPRMetadataCommand.getCommand());
    gfe65Commands.put(MessageType.GET_CLIENT_PARTITION_ATTRIBUTES,
        GetClientPartitionAttributesCommand.getCommand());
    gfe65Commands.put(MessageType.USER_CREDENTIAL_MESSAGE, PutUserCredentials.getCommand());
    gfe65Commands.put(MessageType.REMOVE_USER_AUTH, RemoveUserAuth.getCommand());
    gfe65Commands.put(MessageType.EXECUTE_REGION_FUNCTION_SINGLE_HOP,
        ExecuteRegionFunctionSingleHop.getCommand());

    // Initialize the GFE 6.5.1 commands
    Map<Integer, Command> gfe651Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_651, gfe651Commands);
    gfe651Commands.putAll(ALL_COMMANDS.get(Version.GFE_65));
    gfe651Commands.put(MessageType.QUERY_WITH_PARAMETERS, Query651.getCommand());

    // Initialize the GFE 6.5.1.6 commands
    Map<Integer, Command> gfe6516Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_6516, gfe6516Commands);
    gfe6516Commands.putAll(ALL_COMMANDS.get(Version.GFE_651));
    gfe6516Commands.put(MessageType.GET_ALL, GetAll651.getCommand());
    gfe6516Commands.put(MessageType.GET_CLIENT_PR_METADATA,
        GetClientPRMetadataCommand66.getCommand());

    // Initialize the GFE 6.6 commands
    Map<Integer, Command> gfe66Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_66, gfe66Commands);
    gfe66Commands.putAll(ALL_COMMANDS.get(Version.GFE_6516));
    gfe66Commands.put(MessageType.ADD_PDX_TYPE, AddPdxType.getCommand());
    gfe66Commands.put(MessageType.GET_PDX_ID_FOR_TYPE, GetPDXIdForType.getCommand());
    gfe66Commands.put(MessageType.GET_PDX_TYPE_BY_ID, GetPDXTypeById.getCommand());
    gfe66Commands.put(MessageType.SIZE, Size.getCommand());
    gfe66Commands.put(MessageType.INVALIDATE, Invalidate.getCommand());
    gfe66Commands.put(MessageType.COMMIT, CommitCommand.getCommand());
    gfe66Commands.put(MessageType.ROLLBACK, RollbackCommand.getCommand());
    gfe66Commands.put(MessageType.TX_FAILOVER, TXFailoverCommand.getCommand());
    gfe66Commands.put(MessageType.GET_ENTRY, GetEntryCommand.getCommand());
    gfe66Commands.put(MessageType.TX_SYNCHRONIZATION, TXSynchronizationCommand.getCommand());
    gfe66Commands.put(MessageType.GET_CLIENT_PARTITION_ATTRIBUTES,
        GetClientPartitionAttributesCommand66.getCommand());
    gfe66Commands.put(MessageType.REGISTER_INTEREST_LIST, RegisterInterestList66.getCommand());
    gfe66Commands.put(MessageType.GET_FUNCTION_ATTRIBUTES, GetFunctionAttribute.getCommand());
    gfe66Commands.put(MessageType.EXECUTE_REGION_FUNCTION, ExecuteRegionFunction66.getCommand());
    gfe66Commands.put(MessageType.EXECUTE_FUNCTION, ExecuteFunction66.getCommand());
    gfe66Commands.put(MessageType.GET_ALL_FOR_RI, GetAllForRI.getCommand());
    gfe66Commands.put(MessageType.GATEWAY_RECEIVER_COMMAND, GatewayReceiverCommand.getCommand());
    gfe66Commands.put(MessageType.CONTAINS_KEY, ContainsKey66.getCommand());

    // Initialize the GFE 6.6.2 commands
    Map<Integer, Command> gfe662Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_662, gfe662Commands);
    gfe662Commands.putAll(ALL_COMMANDS.get(Version.GFE_66));
    gfe662Commands.put(MessageType.ADD_PDX_ENUM, AddPdxEnum.getCommand());
    gfe662Commands.put(MessageType.GET_PDX_ID_FOR_ENUM, GetPDXIdForEnum.getCommand());
    gfe662Commands.put(MessageType.GET_PDX_ENUM_BY_ID, GetPDXEnumById.getCommand());

    // Initialize the GFE 6.6.2.2 commands (same commands as the GFE 6.6.2 commands)
    // The SERVER_TO_CLIENT_PING message was added, but it doesn't need to be registered here
    ALL_COMMANDS.put(Version.GFE_6622, gfe662Commands);

    // Initialize the GFE 70 commands
    Map<Integer, Command> gfe70Commands = new HashMap<Integer, Command>();
    ALL_COMMANDS.put(Version.GFE_70, gfe70Commands);
    gfe70Commands.putAll(ALL_COMMANDS.get(Version.GFE_662));
    gfe70Commands.remove(MessageType.GET_ALL_FOR_RI);
    gfe70Commands.put(MessageType.REQUEST, Get70.getCommand());
    gfe70Commands.put(MessageType.GET_ENTRY, GetEntry70.getCommand());
    gfe70Commands.put(MessageType.GET_ALL_70, GetAll70.getCommand());
    gfe70Commands.put(MessageType.PUTALL, PutAll70.getCommand());
    gfe70Commands.put(MessageType.PUT, Put70.getCommand());
    gfe70Commands.put(MessageType.DESTROY, Destroy70.getCommand());
    gfe70Commands.put(MessageType.INVALIDATE, Invalidate70.getCommand());
    gfe70Commands.put(MessageType.GET_PDX_TYPES, GetPdxTypes70.getCommand());
    gfe70Commands.put(MessageType.GET_PDX_ENUMS, GetPdxEnums70.getCommand());
    gfe70Commands.put(MessageType.EXECUTE_FUNCTION, ExecuteFunction70.getCommand());

    ALL_COMMANDS.put(Version.GFE_701, gfe70Commands);
    ALL_COMMANDS.put(Version.GFE_7099, gfe70Commands);
    ALL_COMMANDS.put(Version.GFE_71, gfe70Commands);

    Map<Integer, Command> gfe80Commands = new HashMap<Integer, Command>();
    gfe80Commands.putAll(ALL_COMMANDS.get(Version.GFE_71));
    ALL_COMMANDS.put(Version.GFE_80, gfe80Commands);
    // PutAll is changed to chunk responses back to the client
    gfe80Commands.put(MessageType.PUTALL, PutAll80.getCommand());

    ALL_COMMANDS.put(Version.GFE_8009, gfe80Commands);

    Map<Integer, Command> gfe81Commands = new HashMap<Integer, Command>();
    gfe81Commands.putAll(gfe80Commands);
    gfe81Commands.put(MessageType.GET_ALL_WITH_CALLBACK, GetAllWithCallback.getCommand());
    gfe81Commands.put(MessageType.PUT_ALL_WITH_CALLBACK, PutAllWithCallback.getCommand());
    gfe81Commands.put(MessageType.REMOVE_ALL, RemoveAll.getCommand());

    ALL_COMMANDS.put(Version.GFE_81, gfe81Commands);
    ALL_COMMANDS.put(Version.GFE_82, gfe81Commands);

    Map<Integer, Command> commands = new HashMap<Integer, Command>();
    commands.putAll(ALL_COMMANDS.get(Version.GFE_82));
    ALL_COMMANDS.put(Version.GFE_90, commands);
    commands.put(MessageType.QUERY_WITH_PARAMETERS, QueryWithParametersGeode10.getCommand());
    commands.put(MessageType.QUERY, QueryGeode10.getCommand());

    ALL_COMMANDS.put(Version.GEODE_110, commands);
    ALL_COMMANDS.put(Version.GEODE_111, commands);
    ALL_COMMANDS.put(Version.GEODE_120, commands);
    ALL_COMMANDS.put(Version.GEODE_130, commands);
    ALL_COMMANDS.put(Version.GEODE_140, commands);
    ALL_COMMANDS.put(Version.GEODE_150, commands);
    ALL_COMMANDS.put(Version.GEODE_160, commands);
    ALL_COMMANDS.put(Version.GEODE_170, commands);
    ALL_COMMANDS.put(Version.GEODE_180, commands);

  }

  public static Map<Integer, Command> getCommands(Version version) {
    return ALL_COMMANDS.get(version);
  }

  public static Map<Integer, Command> getCommands(ServerConnection connection) {
    return getCommands(connection.getClientVersion());
  }
}
