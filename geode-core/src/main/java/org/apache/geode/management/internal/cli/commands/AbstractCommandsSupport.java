/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.cli.commands;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.MemberNotFoundException;

import org.springframework.shell.core.CommandMarker;

/**
 * The AbstractCommandsSupport class is an abstract base class encapsulating common functionality for implementing
 * command classes with command for the GemFire shell (gfsh).
 * <p>
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.execute.FunctionService
 * @see org.apache.geode.distributed.DistributedMember
 * @see org.apache.geode.management.internal.cli.shell.Gfsh
 * @see org.springframework.shell.core.CommandMarker
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public abstract class AbstractCommandsSupport implements CommandMarker {

  protected static void assertArgument(final boolean valid, final String message, final Object... args) {
    if (!valid) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  protected static void assertNotNull(final Object obj, final String message, final Object... args) {
    if (obj == null) {
      throw new NullPointerException(String.format(message, args));
    }
  }

  protected static void assertState(final boolean valid, final String message, final Object... args) {
    if (!valid) {
      throw new IllegalStateException(String.format(message, args));
    }
  }

  protected static String convertDefaultValue(final String from, final String to) {
    return (CliMetaData.ANNOTATION_DEFAULT_VALUE.equals(from) ? to : from);
  }

  protected static String toString(final Boolean condition, final String trueValue, final String falseValue) {
    return (Boolean.TRUE.equals(condition) ? StringUtils.defaultIfBlank(trueValue, "true")
      : StringUtils.defaultIfBlank(falseValue, "false"));
  }

  protected static String toString(final Throwable t, final boolean printStackTrace) {
    String message = t.getMessage();

    if (printStackTrace) {
      StringWriter writer = new StringWriter();
      t.printStackTrace(new PrintWriter(writer));
      message = writer.toString();
    }

    return message;
  }

  protected boolean isConnectedAndReady() {
    return (getGfsh() != null && getGfsh().isConnectedAndReady());
  }

  protected boolean isDebugging() {
    return (getGfsh() != null && getGfsh().getDebug());
  }

  protected boolean isLogging() {
    return (getGfsh() != null);
  }

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  protected Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  @SuppressWarnings("deprecated")
  protected DistributedMember getMember(final Cache cache, final String memberName) {
    for (final DistributedMember member : getMembers(cache)) {
      if (memberName.equalsIgnoreCase(member.getName()) || memberName.equalsIgnoreCase(member.getId())) {
        return member;
      }
    }

    throw new MemberNotFoundException(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberName));
  }

  /**
   * Gets all members in the GemFire distributed system/cache.
   * </p>
   * @param cache the GemFire cache.
   * @return all members in the GemFire distributed system/cache.
   * @see org.apache.geode.management.internal.cli.CliUtil#getAllMembers(org.apache.geode.cache.Cache)
   * @deprecated use CliUtil.getAllMembers(org.apache.geode.cache.Cache) instead
   */
  @Deprecated
  protected Set<DistributedMember> getMembers(final Cache cache) {
    Set<DistributedMember> members = new HashSet<DistributedMember>(cache.getMembers());
    members.add(cache.getDistributedSystem().getDistributedMember());
    return members;
  }

  protected Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
    return FunctionService.onMembers(members);
  }

  protected void logInfo(final String message) {
    logInfo(message, null);
  }

  protected void logInfo(final Throwable cause) {
    logInfo(cause.getMessage(), cause);
  }

  protected void logInfo(final String message, final Throwable cause) {
    if (isLogging()) {
      getGfsh().logInfo(message, cause);
    }
  }

  protected void logWarning(final String message) {
    logWarning(message, null);
  }

  protected void logWarning(final Throwable cause) {
    logWarning(cause.getMessage(), cause);
  }

  protected void logWarning(final String message, final Throwable cause) {
    if (isLogging()) {
      getGfsh().logWarning(message, cause);
    }
  }

  protected void logSevere(final String message) {
    logSevere(message, null);
  }

  protected void logSevere(final Throwable cause) {
    logSevere(cause.getMessage(), cause);
  }

  protected void logSevere(final String message, final Throwable cause) {
    if (isLogging()) {
      getGfsh().logSevere(message, cause);
    }
  }

  @SuppressWarnings("unchecked")
  protected <T extends Function> T register(T function) {
    if (FunctionService.isRegistered(function.getId())) {
      function = (T) FunctionService.getFunction(function.getId());
    }
    else {
      FunctionService.registerFunction(function);
    }

    return function;
  }

}
