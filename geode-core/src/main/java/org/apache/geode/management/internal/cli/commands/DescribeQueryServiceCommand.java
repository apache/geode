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

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.query.management.configuration.QueryConfigService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DescribeQueryServiceFunction;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DescribeQueryServiceCommand extends SingleGfshCommand {

  static final String COMMAND_NAME = "describe query-service";
  private static final String COMMAND_HELP =
      "Describes the clusters query service";
  static final String METHOD_AUTHORIZER_DATA_SECTION = "MethodAuthorizerName";
  static final String METHOD_AUTHORIZER_TABLE_SECTION = "MethodAuthorizerParameters";
  static final String PARAMETERS_COLUMN_NAME = "Parameters";
  public static final String ALL_METHODS_ALLOWED =
      "Security is not enabled. All methods will be authorized.";
  public static final String METHOD_AUTH_INFO_SECTION = "MethodAuthDescriptionHeader";
  public static final String FUNCTION_FAILED_ON_ALL_MEMBERS = "Function was not successful";
  public static final String NO_CLUSTER_CONFIG_AND_NO_MEMBERS =
      "No cluster config found and no distributed members found.";
  public static final String AUTHORIZER_CLASS_NAME = "Class Name";

  @CliCommand(value = COMMAND_NAME, help = COMMAND_HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel execute() {

    QueryConfigService queryConfigService = getQueryConfigService();

    if (queryConfigService != null) {
      return constructResultModelFromQueryService(queryConfigService);
    } else {
      Set<DistributedMember> targetMembers = findMembers(null, null);
      if (targetMembers.size() > 0) {
        for (DistributedMember member : targetMembers) {
          CliFunctionResult result =
              executeFunctionAndGetFunctionResult(new DescribeQueryServiceFunction(), null, member);
          if (result.isSuccessful()) {
            queryConfigService = (QueryConfigService) result.getResultObject();
            if (queryConfigService != null) {
              return constructResultModelFromQueryService(queryConfigService);
            }
          }
        }
        return ResultModel.createError(FUNCTION_FAILED_ON_ALL_MEMBERS);
      }
    }
    return ResultModel.createError(NO_CLUSTER_CONFIG_AND_NO_MEMBERS);
  }

  QueryConfigService getQueryConfigService() {
    InternalConfigurationPersistenceService configService = getConfigurationPersistenceService();
    if (configService != null) {
      CacheConfig cacheConfig = configService.getCacheConfig(null);
      if (cacheConfig != null) {
        return cacheConfig.findCustomCacheElement(QueryConfigService.ELEMENT_ID,
            QueryConfigService.class);
      }
    }
    return null;
  }

  ResultModel constructResultModelFromQueryService(QueryConfigService queryConfigurationService) {
    ResultModel result = new ResultModel();

    addMethodAuthorizerToResultModel(queryConfigurationService, result);

    return result;
  }

  void addMethodAuthorizerToResultModel(QueryConfigService queryConfigurationService,
      ResultModel result) {
    InfoResultModel authHeaderInfoModel = result.addInfo(METHOD_AUTH_INFO_SECTION);
    authHeaderInfoModel.addLine("-- Method Authorizer --");

    QueryConfigService.MethodAuthorizer methodAuthorizer =
        queryConfigurationService.getMethodAuthorizer();

    if (methodAuthorizer != null) {
      if (isSecurityEnabled()) {
        DataResultModel dataResultModel = result.addData(METHOD_AUTHORIZER_DATA_SECTION);
        dataResultModel
            .addData(AUTHORIZER_CLASS_NAME, methodAuthorizer.getClassName());

        List<QueryConfigService.MethodAuthorizer.Parameter> parameters =
            methodAuthorizer.getParameters();
        if (parameters.size() > 0) {
          TabularResultModel parametersTableModel =
              result.addTable(METHOD_AUTHORIZER_TABLE_SECTION);
          parametersTableModel.setColumnHeader(PARAMETERS_COLUMN_NAME);
          parameters.stream().map(p -> p.getParameterValue()).forEach(parametersTableModel::addRow);
        }
      } else {
        authHeaderInfoModel.addLine(ALL_METHODS_ALLOWED);
      }
    }
  }

  boolean isSecurityEnabled() {
    return ((InternalCache) CacheFactory.getAnyInstance()).getSecurityService()
        .isIntegratedSecurity();
  }
}
