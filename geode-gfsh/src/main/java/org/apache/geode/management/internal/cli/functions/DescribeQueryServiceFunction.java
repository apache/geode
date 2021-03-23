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

package org.apache.geode.management.internal.cli.functions;


import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl;
import org.apache.geode.cache.query.management.configuration.QueryConfigService;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class DescribeQueryServiceFunction extends CliFunction {

  private static final long serialVersionUID = 8283480284191516847L;
  static final String QUERY_SERVICE_NOT_FOUND_MESSAGE =
      "QueryConfigurationService not found on member.";

  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.DescribeQueryServiceFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext context) {
    QueryConfigurationServiceImpl queryConfigurationService = getQueryConfigurationService();

    if (queryConfigurationService != null) {
      return new CliFunctionResult(context.getMemberName(),
          translateQueryServiceObjectIntoQueryConfigService(queryConfigurationService));
    } else {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          QUERY_SERVICE_NOT_FOUND_MESSAGE);
    }
  }

  QueryConfigurationServiceImpl getQueryConfigurationService() {
    return (QueryConfigurationServiceImpl) ((InternalCache) CacheFactory.getAnyInstance())
        .getService(org.apache.geode.cache.query.internal.QueryConfigurationService.class);
  }

  QueryConfigService translateQueryServiceObjectIntoQueryConfigService(
      QueryConfigurationServiceImpl queryServiceObject) {
    QueryConfigService queryConfigService = new QueryConfigService();

    MethodInvocationAuthorizer methodAuthorizer = queryServiceObject.getMethodAuthorizer();
    if (methodAuthorizer != null) {
      QueryConfigService.MethodAuthorizer methodAuthorizerConfig =
          new QueryConfigService.MethodAuthorizer();
      methodAuthorizerConfig.setClassName(methodAuthorizer.getClass().getName());

      queryConfigService.setMethodAuthorizer(methodAuthorizerConfig);
    }
    return queryConfigService;
  }
}
