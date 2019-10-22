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

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.query.management.configuration.QueryConfigService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class AlterQueryServiceCommand extends SingleGfshCommand {

  static final String COMMAND_NAME = "alter query-service";
  static final String COMMAND_HELP =
      "Alter configuration parameters for the query service";
  static final String METHOD_AUTHORIZER_NAME = "method-authorizer";
  static final String METHOD_AUTHORIZER_NAME_HELP =
      "The name of the class to be used for OQL method authorization";
  static final String AUTHORIZER_PARAMETERS = "authorizer-parameters";
  static final String AUTHORIZER_PARAMETERS_HELP =
      "A comma separated list of all parameter values for the specified method authorizer. Requires '--"
          + METHOD_AUTHORIZER_NAME + "' option to be used";
  static final String SINGLE_AUTHORIZER_PARAMETER = "authorizer-parameter";
  static final String SINGLE_AUTHORIZER_PARAMETER_HELP =
      "A single parameter to be used by the specified method authorizer. Requires '--"
          + METHOD_AUTHORIZER_NAME + "' option to be used";
  static final String DEPRECATED_PROPERTY_ERROR = "Deprecated System Property: \"" + GEMFIRE_PREFIX
      + "QueryService.allowUntrustedMethodInvocation\" is set to TRUE. In order to use a MethodInvocationAuthorizer, this property must be FALSE or undefined.";
  static final String PARAMETERS_WITHOUT_AUTHORIZER_MESSAGE =
      "The '--" + AUTHORIZER_PARAMETERS + "' and '--" + SINGLE_AUTHORIZER_PARAMETER
          + "' options require '--"
          + METHOD_AUTHORIZER_NAME + "' to be specified";
  static final String SINGLE_PARAM_AND_PARAMETERS_SPECIFIED_MESSAGE =
      "Only one of '--" + AUTHORIZER_PARAMETERS + "' and '--" + SINGLE_AUTHORIZER_PARAMETER
          + "' options can be specified.";
  static final String NO_ARGUMENTS_MESSAGE =
      "No arguments were provided. No changes have been applied.";
  static final String NO_MEMBERS_FOUND_MESSAGE = "No members found.";
  static final String SECURITY_NOT_ENABLED_MESSAGE =
      "Integrated security is not enabled for this distributed system. Updating the method authorizer requires integrated security to be enabled.";

  @CliCommand(value = COMMAND_NAME, help = COMMAND_HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel execute(
      @CliOption(key = METHOD_AUTHORIZER_NAME,
          help = METHOD_AUTHORIZER_NAME_HELP) String methodAuthorizerName,
      @CliOption(key = AUTHORIZER_PARAMETERS,
          help = AUTHORIZER_PARAMETERS_HELP) String[] authorizerParameters,
      @CliOption(key = SINGLE_AUTHORIZER_PARAMETER,
          help = SINGLE_AUTHORIZER_PARAMETER_HELP) String singleAuthorizerParameter) {
    ResultModel result;

    QueryConfigService queryConfigService = getQueryConfigService();
    Set<String> parametersSet = null;
    if (methodAuthorizerName != null) {
      if (!isSecurityEnabled()) {
        return ResultModel.createError(SECURITY_NOT_ENABLED_MESSAGE);
      }
      if (Boolean.parseBoolean(
          System.getProperty(GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation"))) {
        return ResultModel.createError(DEPRECATED_PROPERTY_ERROR);
      }
      if (authorizerParameters != null) {
        parametersSet = new HashSet<>(Arrays.asList(authorizerParameters));
      } else if (singleAuthorizerParameter != null) {
        parametersSet = new HashSet<>(Collections.singleton(singleAuthorizerParameter));
      }
      populateMethodAuthorizer(methodAuthorizerName, parametersSet, queryConfigService);
    }

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      Object[] args = new Object[] {methodAuthorizerName, parametersSet};
      List<CliFunctionResult> functionResults =
          executeAndGetFunctionResult(new AlterQueryServiceFunction(), args,
              targetMembers);
      result =
          ResultModel.createMemberStatusResult(functionResults, null, null, false, false);
    } else {
      result = ResultModel.createInfo(NO_MEMBERS_FOUND_MESSAGE);
    }
    result.setConfigObject(queryConfigService);
    return result;
  }

  boolean isSecurityEnabled() {
    return ((InternalCache) CacheFactory.getAnyInstance()).getSecurityService()
        .isIntegratedSecurity();
  }

  void populateMethodAuthorizer(String methodAuthorizerName, Set<String> parameterSet,
      QueryConfigService queryConfigService) {
    QueryConfigService.MethodAuthorizer methodAuthorizer =
        new QueryConfigService.MethodAuthorizer();
    methodAuthorizer.setClassName(methodAuthorizerName);

    if (parameterSet != null && parameterSet.size() != 0) {
      List<QueryConfigService.MethodAuthorizer.Parameter> parameters = new ArrayList<>();
      for (String param : parameterSet) {
        QueryConfigService.MethodAuthorizer.Parameter parameter =
            new QueryConfigService.MethodAuthorizer.Parameter();
        parameter.setParameterValue(param);
        parameters.add(parameter);
      }
      methodAuthorizer.setParameters(parameters);
    }
    queryConfigService.setMethodAuthorizer(methodAuthorizer);
  }

  QueryConfigService getQueryConfigService() {
    ConfigurationPersistenceService configService = getConfigurationPersistenceService();
    if (configService != null) {
      CacheConfig cacheConfig = configService.getCacheConfig(null);
      if (cacheConfig != null) {
        QueryConfigService queryConfigService = cacheConfig.findCustomCacheElement(
            QueryConfigService.ELEMENT_ID, QueryConfigService.class);
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



  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      if (Arrays.stream(parseResult.getArguments()).allMatch(Objects::isNull)) {
        return ResultModel.createError(NO_ARGUMENTS_MESSAGE);
      }

      Object authorizerName = parseResult.getParamValue(METHOD_AUTHORIZER_NAME);
      Object authorizerParams = parseResult.getParamValue(AUTHORIZER_PARAMETERS);
      Object singleAuthorizerParam = parseResult.getParamValue(SINGLE_AUTHORIZER_PARAMETER);

      if ((singleAuthorizerParam != null || authorizerParams != null) && authorizerName == null) {
        return ResultModel.createError(PARAMETERS_WITHOUT_AUTHORIZER_MESSAGE);
      }

      if (singleAuthorizerParam != null && authorizerParams != null) {
        return ResultModel.createError(SINGLE_PARAM_AND_PARAMETERS_SPECIFIED_MESSAGE);
      }

      return new ResultModel();
    }
  }
}
