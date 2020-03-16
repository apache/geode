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
package org.apache.geode.management.cli;

import java.util.Collections;
import java.util.Map;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DependenciesNotFoundException;
import org.apache.geode.management.internal.cli.CliUtil;

/**
 * Processes remote GemFire Command Line Interface (CLI) commands. Refer to the vFabric GemFire
 * documentation for information regarding local vs. remote commands.
 * <p>
 * <b>NOTE:</b> <code>CommandService</code> is currently available only on GemFire Manager nodes.
 *
 *
 * @since GemFire 7.0
 *
 * @deprecated since 1.3 use OnlineCommandProcessor directly
 */
@Deprecated
public abstract class CommandService {
  @Immutable
  protected static final Map<String, String> EMPTY_ENV = Collections.emptyMap();

  @MakeNotStatic
  private static CommandService localCommandService;

  /* ************* Methods to be implemented by sub-classes START *********** */

  /**
   * Returns whether the underlying <code>Cache</code> exists and is not closed. The Cache must be
   * ready in order for commands to be processed using this <code>CommandService</code>. A call to
   * this method should be made before attempting to process commands.
   *
   * @return True if the <code>Cache</code> exists and is not closed, false otherwise.
   */
  public abstract boolean isUsable();

  /**
   * Processes the specified command string. Only remote commands can be processed using this
   * method. Refer to the vFabric GemFire documentation for details.
   *
   * @param commandString Command string to be processed.
   * @return The {@link Result} of the execution of this command string.
   */
  public abstract Result processCommand(String commandString);

  /**
   * Processes the specified command string. Only remote commands can be processed using this
   * method. Refer to the vFabric GemFire documentation for details.
   *
   * @param commandString Command string to be processed.
   * @param env Environmental values that will be used during the execution of this command.
   * @return The {@link Result} of the execution of this command string.
   */
  protected abstract Result processCommand(String commandString, Map<String, String> env);

  /**
   * Creates a <code>CommandStatement</code> from the specified command string. Only remote commands
   * can be processed using this method. Refer to the vFabric GemFire documentation for details.
   *
   * @param commandString Command string from which to create a <code>CommandStatement</code>.
   * @return A <code>CommandStatement</code> which can be used to repeatedly process the same
   *         command.
   *
   * @see CommandStatement#process()
   * @deprecated since Geode 1.3, simply call processCommand to execute the command
   */
  public abstract CommandStatement createCommandStatement(String commandString);

  /**
   * Creates a <code>CommandStatement</code> from the specified command string. Only remote commands
   * can be processed using this method. Refer to the vFabric GemFire documentation for details.
   *
   * @param commandString Command string from which to create a <code>CommandStatement</code>.
   * @param env Environmental values that will be used during the execution of this command.
   * @return A <code>CommandStatement</code> which can be used to repeatedly process the same
   *         command.
   *
   * @see CommandStatement#process()
   * @deprecated since Geode 1.3, simply call processCommand to execute the command
   */
  protected abstract CommandStatement createCommandStatement(String commandString,
      Map<String, String> env);

  /* ************** Methods to be implemented by sub-classes END ************ */

  /* **************************** factory methods *************************** */
  /**
   * Returns a newly created or existing instance of the
   * <code>CommandService<code> associated with the
   * specified <code>Cache</code>.
   *
   * @param cache Underlying <code>Cache</code> instance to be used to create a Command Service.
   * @throws CommandServiceException If command service could not be initialized.
   */
  public static CommandService createLocalCommandService(Cache cache)
      throws CommandServiceException {
    if (cache == null) {
      throw new CacheClosedException("Can not create command service as cache doesn't exist.");
    } else if (cache.isClosed()) {
      throw ((InternalCache) cache)
          .getCacheClosedException("Can not create command service as cache is closed.");
    }

    if (localCommandService == null || !localCommandService.isUsable()) {
      String nonExistingDependency = CliUtil.cliDependenciesExist(false);
      if (nonExistingDependency != null) {
        throw new DependenciesNotFoundException(
            String.format(
                "Could not find %s library which is needed for CLI/gfsh in classpath. Internal support for CLI & gfsh is not enabled. Note: For convenience, absolute path of gfsh-dependencies.jar from lib directory of GemFire product distribution can be included in CLASSPATH of an application.",
                nonExistingDependency));
      }

      localCommandService =
          new org.apache.geode.management.internal.cli.remote.MemberCommandService(
              (InternalCache) cache);
    }

    return localCommandService;
  }

  /**
   * Returns an existing 'usable' <code>CommandService></code>. A <code>CommandService</code> is
   * considered usable if at has an underlying <code>Cache</code> which is not closed.
   *
   * @return A usable <code>CommandService</code> or null if one cannot be found.
   */
  public static CommandService getUsableLocalCommandService() {
    if (localCommandService != null && localCommandService.isUsable()) {
      return localCommandService;
    }

    return null;
  }
}
