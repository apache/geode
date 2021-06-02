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

package org.apache.geode.management.internal.cli.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.query.management.configuration.QueryConfigService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class AlterQueryServiceCommand extends SingleGfshCommand {
  static final String COMMAND_NAME = "alter query-service";
  private static final String COMMAND_HELP = "Alter configuration parameters for the query service";
  static final String AUTHORIZER_NAME = "method-authorizer";
  private static final String METHOD_AUTHORIZER_NAME_HELP =
      "The name of the class to be used for OQL method authorization";
  static final String AUTHORIZER_PARAMETERS = "authorizer-parameters";
  private static final String AUTHORIZER_PARAMETERS_HELP =
      "A semicolon separated list of all parameter values for the specified method authorizer.";
  static final String FORCE_UPDATE = "force-update";
  private static final String FORCE_UPDATE_HELP =
      "Flag to indicate whether to forcibly update the currently configured authorizer, even when there are continuous queries registered (use with caution)";
  static final String NO_MEMBERS_FOUND_MESSAGE = "No members found.";
  public static final String PARTIAL_FAILURE_MESSAGE =
      "In the event of a partial failure of this command, re-running the command or restarting failing members should restore consistency.";
  static final String SPLITTING_REGEX = ";";

  private final AlterQueryServiceFunction alterQueryServiceFunction =
      new AlterQueryServiceFunction();

  @CliCommand(value = COMMAND_NAME, help = COMMAND_HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel execute(
      @CliOption(key = AUTHORIZER_NAME, help = METHOD_AUTHORIZER_NAME_HELP,
          mandatory = true) String methodAuthorizerName,
      @CliOption(key = AUTHORIZER_PARAMETERS, help = AUTHORIZER_PARAMETERS_HELP,
          optionContext = "splittingRegex=" + SPLITTING_REGEX) String[] authorizerParameters,
      @CliOption(key = FORCE_UPDATE, help = FORCE_UPDATE_HELP, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean forceUpdate) {

    ResultModel result;
    Set<String> parametersSet = new HashSet<>();
    QueryConfigService queryConfigService = getQueryConfigService();
    if (authorizerParameters != null) {
      parametersSet.addAll(Arrays.asList(authorizerParameters));
    }
    populateMethodAuthorizer(methodAuthorizerName, parametersSet, queryConfigService);

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.isEmpty()) {
      result = ResultModel.createInfo(NO_MEMBERS_FOUND_MESSAGE);
    } else {
      String footer = null;
      Object[] args = new Object[] {forceUpdate, methodAuthorizerName, parametersSet};
      List<CliFunctionResult> functionResults =
          executeAndGetFunctionResult(alterQueryServiceFunction, args, targetMembers);

      if (functionResults.stream().anyMatch(CliFunctionResult::isSuccessful)
          && functionResults.stream().anyMatch(functionResult -> !functionResult.isSuccessful())) {
        footer = PARTIAL_FAILURE_MESSAGE;
      }

      result = ResultModel.createMemberStatusResult(functionResults, null, footer, false, true);
    }

    result.setConfigObject(queryConfigService);

    return result;
  }

  void populateMethodAuthorizer(String methodAuthorizerName, Set<String> parameterSet,
      QueryConfigService queryConfigService) {
    QueryConfigService.MethodAuthorizer methodAuthorizer =
        new QueryConfigService.MethodAuthorizer();
    methodAuthorizer.setClassName(methodAuthorizerName);

    if (!parameterSet.isEmpty()) {
      List<QueryConfigService.MethodAuthorizer.Parameter> parameterList = new ArrayList<>();

      for (String value : parameterSet) {
        QueryConfigService.MethodAuthorizer.Parameter parameter =
            new QueryConfigService.MethodAuthorizer.Parameter();
        parameter.setParameterValue(value);
        parameterList.add(parameter);
      }

      methodAuthorizer.setParameters(parameterList);
    }

    queryConfigService.setMethodAuthorizer(methodAuthorizer);
  }

  QueryConfigService getQueryConfigService() {
    ConfigurationPersistenceService configService = getConfigurationPersistenceService();

    if (configService != null) {
      CacheConfig cacheConfig = configService.getCacheConfig(null);

      if (cacheConfig != null) {
        QueryConfigService queryConfigService = cacheConfig
            .findCustomCacheElement(QueryConfigService.ELEMENT_ID, QueryConfigService.class);

        if (queryConfigService != null) {
          return queryConfigService;
        }
      }
    }

    return new QueryConfigService();
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    if (configObject instanceof QueryConfigService) {
      List<CacheElement> elements = config.getCustomCacheElements();
      elements.removeIf(e -> e instanceof QueryConfigService);
      elements.add((QueryConfigService) configObject);

      return true;
    }

    return false;
  }
}
