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

import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.internal.QueryConfigurationService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class AlterQueryServiceFunction extends CliFunction<Object[]> {
  private static final long serialVersionUID = 7155576168386556341L;
  public static final String AUTHORIZER_UPDATED_MESSAGE =
      "Updated MethodInvocationAuthorizer. New authorizer is: ";
  static final String AUTHORIZER_PARAMETERS_MESSAGE = " with parameters: ";
  public static final String EMPTY_AUTHORIZER_ERROR =
      "MethodInvocationAuthorizer class name must not be empty.";
  public static final String SECURITY_NOT_ENABLED_MESSAGE =
      "Integrated security is not enabled for this distributed system. Updating the method authorizer requires integrated security to be enabled.";

  @SuppressWarnings("deprecation")
  public static final String DEPRECATED_PROPERTY_ERROR = "Deprecated System Property: \""
      + ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY
      + "\" is set to TRUE. In order to use a MethodInvocationAuthorizer, this property must be FALSE or undefined.";

  boolean isSecurityEnabled() {
    return ((InternalCache) CacheFactory.getAnyInstance()).getSecurityService()
        .isIntegratedSecurity();
  }

  @Override
  @SuppressWarnings({"unchecked", "deprecation"})
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    Set<String> parameterSet;
    boolean forceUpdate = (boolean) context.getArguments()[0];
    String authorizerName = (String) context.getArguments()[1];

    if (context.getArguments()[2] != null) {
      parameterSet = (Set<String>) context.getArguments()[2];
    } else {
      parameterSet = Collections.emptySet();
    }

    if (StringUtils.isEmpty(authorizerName)) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          EMPTY_AUTHORIZER_ERROR);
    }

    if (!isSecurityEnabled()) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          SECURITY_NOT_ENABLED_MESSAGE);
    }

    if (Boolean
        .parseBoolean(System.getProperty(ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY))) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          DEPRECATED_PROPERTY_ERROR);
    }

    try {
      Cache cache = context.getCache();
      ((InternalCache) cache).getService(QueryConfigurationService.class)
          .updateMethodAuthorizer(cache, forceUpdate, authorizerName, parameterSet);
    } catch (Exception ex) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          ex.getMessage());
    }

    String message = AUTHORIZER_UPDATED_MESSAGE + authorizerName + (parameterSet.size() > 0
        ? AUTHORIZER_PARAMETERS_MESSAGE + String.join(", ", parameterSet) : "");
    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        message);
  }
}
