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

import java.util.Map;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * Registry of server commands by version.
 */
public interface CommandRegistry {

  /**
   * Register a new command with the system.
   *
   * @param messageType - An ordinal for this message. This must be something defined in MessageType
   *        that has not already been allocated to a different command.
   * @param versionToNewCommand The command to register, for different versions. The key is the
   *        earliest version for which this command class is valid (starting with
   *        {@link KnownVersion#OLDEST}).
   *
   * @throws InternalGemFireError if a different command is already registered for given version or
   *         if no command was added to the registry.
   */
  void register(int messageType, Map<KnownVersion, Command> versionToNewCommand);

  Map<Integer, Command> get(KnownVersion version);
}
