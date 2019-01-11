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

package org.apache.geode.management.internal.api;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.UpdateCacheFunction;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationMutator;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationMutatorFactory;
import org.apache.geode.management.internal.exceptions.EntityExistsException;
import org.apache.geode.management.internal.exceptions.NoMembersException;

public class LocatorClusterManagementService implements ClusterManagementService {
  private static Logger logger = LogService.getLogger();
  private InternalCache cache;
  private ConfigurationPersistenceService persistenceService;

  public LocatorClusterManagementService(InternalCache cache,
      ConfigurationPersistenceService persistenceService) {
    this.cache = cache;
    this.persistenceService = persistenceService;

  }

  @Override
  public APIResult createCacheElement(CacheElement config) {
    APIResult result = new APIResult();
    String group = "cluster";
    ConfigurationMutator configurationMutator =
        (new ConfigurationMutatorFactory()).generate(config);

    // exit early if config element already exists in cache config
    if (persistenceService != null) {
      CacheConfig currentPersistedConfig = persistenceService.getCacheConfig(group, true);
      if (configurationMutator.exists(config, currentPersistedConfig)) {
        throw new EntityExistsException("cache element " + config.getId() + " already exists.");
      }
    }

    // execute function on all members
    Set<DistributedMember> targetedMembers = findMembers(null, null);
    if (targetedMembers.size() == 0) {
      throw new NoMembersException("no members found to create cache element");
    }

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        new UpdateCacheFunction(),
        Arrays.asList(config, UpdateCacheFunction.CacheElementOperation.ADD),
        targetedMembers);
    functionResults
        .forEach(functionResult -> result.addMemberStatus(functionResult.getMemberIdOrName(),
            functionResult.isSuccessful() ? APIResult.Result.SUCCESS : APIResult.Result.FAILURE,
            functionResult.getStatusMessage()));

    // persist configuration in cache config
    if (persistenceService != null) {
      persistenceService.updateCacheConfig(group, cacheConfigForGroup -> {
        try {
          configurationMutator.add(config, cacheConfigForGroup);
          result.setClusterConfigPersisted(APIResult.Result.SUCCESS,
              "successfully persisted config for " + group);
        } catch (Exception e) {
          String message = "failed to update cluster config for " + group;
          logger.error(message, e);
          result.setClusterConfigPersisted(APIResult.Result.FAILURE, message);
          return null;
        }

        return cacheConfigForGroup;
      });
    }

    return result;
  }

  @Override
  public APIResult deleteCacheElement(CacheElement config) {
    throw new NotImplementedException();
  }

  @Override
  public APIResult updateCacheElement(CacheElement config) {
    throw new NotImplementedException();
  }

  private Set<DistributedMember> findMembers(String[] groups, String[] members) {
    return CliUtil.findMembers(groups, members, cache);
  }

  private List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector rc = CliUtil.executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }
}
