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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.shell.core.CommandMarker;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CacheMembers;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 * Encapsulates common functionality for implementing command classes for the Geode shell (gfsh).
 *
 * This class should not have much implementation of its own other then those tested in
 * GfshCommandJUnitTest.
 */
@SuppressWarnings("unused")
public interface GfshCommand extends CacheMembers, CommandMarker {

  String EXPERIMENTAL = "(Experimental) ";

  @Override
  default InternalCache getCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  /**
   * @return The cache, if it exists,
   *         or else null if a {@link org.apache.geode.cache.CacheClosedException} arises.
   */
  default InternalCache getCacheIfExists() {
    InternalCache cache = null;
    try {
      cache = getCache();
    } catch (CacheClosedException ignored) {
    }
    return cache;
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

  default XmlEntity findXmlEntity(List<CliFunctionResult> functionResults) {
    return functionResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity).filter(Objects::nonNull).findFirst().orElse(null);
  }

  default boolean isDebugging() {
    return getGfsh() != null && getGfsh().getDebug();
  }

  default boolean isLogging() {
    return getGfsh() != null;
  }

  default SecurityService getSecurityService() {
    return getCache().getSecurityService();
  }

  default Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  default Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
    return FunctionService.onMembers(members);
  }

  default ManagementService getManagementService() {
    return ManagementService.getExistingManagementService(getCache());
  }

  default ResultCollector<?, ?> executeFunction(Function function, Object args,
      final Set<DistributedMember> targetMembers) {
    Execution execution;

    if (args != null) {
      execution = FunctionService.onMembers(targetMembers).setArguments(args);
    } else {
      execution = FunctionService.onMembers(targetMembers);
    }

    ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
    return execution.execute(function);
  }

  default ResultCollector<?, ?> executeFunction(Function function, Object args,
      final DistributedMember targetMember) {
    return executeFunction(function, args, Collections.singleton(targetMember));
  }

  default List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector rc = executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }
}
