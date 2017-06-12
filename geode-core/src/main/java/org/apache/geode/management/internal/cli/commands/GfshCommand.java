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
package org.apache.geode.management.internal.cli.commands;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.CommandMarker;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.MemberNotFoundException;

/**
 * Encapsulates common functionality for implementing command classes for the Geode shell (gfsh).
 *
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.execute.FunctionService
 * @see org.apache.geode.distributed.DistributedMember
 * @see org.apache.geode.management.internal.cli.shell.Gfsh
 * @see org.springframework.shell.core.CommandMarker
 */
@SuppressWarnings("unused")
public interface GfshCommand extends CommandMarker {
  default String convertDefaultValue(final String from, final String to) {
    return CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(from) ? to : from;
  }

  default String toString(final Boolean condition, final String trueValue,
      final String falseValue) {
    return Boolean.TRUE.equals(condition) ? StringUtils.defaultIfBlank(trueValue, "true")
        : StringUtils.defaultIfBlank(falseValue, "false");
  }

  default String toString(final Throwable t, final boolean printStackTrace) {
    String message = t.getMessage();

    if (printStackTrace) {
      StringWriter writer = new StringWriter();
      t.printStackTrace(new PrintWriter(writer));
      message = writer.toString();
    }

    return message;
  }

  default boolean isConnectedAndReady() {
    return getGfsh() != null && getGfsh().isConnectedAndReady();
  }

  default ClusterConfigurationService getSharedConfiguration() {
    InternalLocator locator = InternalLocator.getLocator();
    return locator == null ? null : locator.getSharedConfiguration();
  }

  default void persistClusterConfiguration(Result result, Runnable runnable) {
    if (result == null) {
      throw new IllegalArgumentException("Result should not be null");
    }
    ClusterConfigurationService sc = getSharedConfiguration();
    if (sc == null) {
      result.setCommandPersisted(false);
    } else {
      runnable.run();
      result.setCommandPersisted(true);
    }
  }

  default boolean isDebugging() {
    return getGfsh() != null && getGfsh().getDebug();
  }

  default boolean isLogging() {
    return getGfsh() != null;
  }

  default InternalCache getCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  default SecurityService getSecurityService() {
    return getCache().getSecurityService();
  }

  default Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  @SuppressWarnings("deprecated")
  default DistributedMember getMember(final InternalCache cache, final String memberName) {
    for (final DistributedMember member : getMembers(cache)) {
      if (memberName.equalsIgnoreCase(member.getName())
          || memberName.equalsIgnoreCase(member.getId())) {
        return member;
      }
    }

    throw new MemberNotFoundException(
        CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberName));
  }

  /**
   * Gets all members in the GemFire distributed system/cache.
   *
   * @param cache the GemFire cache.
   * @return all members in the GemFire distributed system/cache.
   * @see org.apache.geode.management.internal.cli.CliUtil#getAllMembers(org.apache.geode.internal.cache.InternalCache)
   * @deprecated use CliUtil.getAllMembers(org.apache.geode.cache.Cache) instead
   */
  @Deprecated
  default Set<DistributedMember> getMembers(final InternalCache cache) {
    Set<DistributedMember> members = new HashSet<>(cache.getMembers());
    members.add(cache.getDistributedSystem().getDistributedMember());
    return members;
  }

  default Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
    return FunctionService.onMembers(members);
  }

  default void logInfo(final String message) {
    logInfo(message, null);
  }

  default void logInfo(final Throwable cause) {
    logInfo(cause.getMessage(), cause);
  }

  default void logInfo(final String message, final Throwable cause) {
    if (isLogging()) {
      getGfsh().logInfo(message, cause);
    }
  }

  default void logWarning(final String message) {
    logWarning(message, null);
  }

  default void logWarning(final Throwable cause) {
    logWarning(cause.getMessage(), cause);
  }

  default void logWarning(final String message, final Throwable cause) {
    if (isLogging()) {
      getGfsh().logWarning(message, cause);
    }
  }

  default void logSevere(final String message) {
    logSevere(message, null);
  }

  default void logSevere(final Throwable cause) {
    logSevere(cause.getMessage(), cause);
  }

  default void logSevere(final String message, final Throwable cause) {
    if (isLogging()) {
      getGfsh().logSevere(message, cause);
    }
  }

  @SuppressWarnings("unchecked")
  default <T extends Function> T register(T function) {
    if (FunctionService.isRegistered(function.getId())) {
      function = (T) FunctionService.getFunction(function.getId());
    } else {
      FunctionService.registerFunction(function);
    }

    return function;
  }

}
