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
package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.ERROR;
import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.OK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.ResourcePermission;

/**
 * @since GemFire 7.0
 */
public class UserFunctionExecution implements InternalFunction<Object[]> {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();
  public static final String ID = UserFunctionExecution.class.getName();

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public String getId() {
    return UserFunctionExecution.ID;
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.emptySet();
  }

  boolean loginRequired(SecurityService securityService) {
    try {
      // if the function is executed on a server with jmx-manager that user is already logged into
      // then we do not need to do login/logout here.
      Subject subject = securityService.getSubject();
      return subject == null || !subject.isAuthenticated();
    } catch (AuthenticationRequiredException e) {
      return true;
    }
  }

  Function<?> loadFunction(String functionId) {
    return FunctionService.getFunction(functionId);
  }

  String[] parseArguments(String argumentsString) {
    if (argumentsString != null && argumentsString.length() > 0) {
      return argumentsString.split(",");
    } else {
      return null;
    }
  }

  Set<String> parseFilters(String filterString) {
    if (filterString != null && filterString.length() > 0) {
      return Arrays.stream(filterString.split(",")).collect(Collectors.toSet());
    } else {
      return new HashSet<>();
    }
  }

  @SuppressWarnings("unchecked")
  ResultCollector<Object, List<Object>> parseResultCollector(String resultCollectorName)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    if (resultCollectorName != null && resultCollectorName.length() > 0) {
      return (ResultCollector<Object, List<Object>>) ClassPathLoader.getLatest()
          .forName(resultCollectorName)
          .newInstance();
    } else {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  Execution<Object, Object, List<Object>> buildExecution(Cache cache, String onRegion)
      throws RegionNotFoundException {
    Execution<Object, Object, List<Object>> execution;
    DistributedMember member = cache.getDistributedSystem().getDistributedMember();

    if (onRegion != null && onRegion.length() > 0) {
      Region<?, ?> region = cache.getRegion(onRegion);

      if (region == null) {
        throw new RegionNotFoundException(onRegion);
      }

      execution = FunctionService.onRegion(region);
    } else {
      execution = FunctionService.onMember(member);
    }

    return execution;
  }

  @Override
  public void execute(FunctionContext<Object[]> context) {
    Cache cache = ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
    DistributedMember member = cache.getDistributedSystem().getDistributedMember();

    Object[] args = context.getArguments();
    if (args == null) {
      context.getResultSender().lastResult(new CliFunctionResult(context.getMemberName(), ERROR,
          CliStrings.EXECUTE_FUNCTION__MSG__COULD_NOT_RETRIEVE_ARGUMENTS));
      return;
    }

    String functionId = ((String) args[0]);
    String filterString = ((String) args[1]);
    String resultCollectorName = ((String) args[2]);
    String argumentsString = ((String) args[3]);
    String onRegion = ((String) args[4]);
    Properties credentials = (Properties) args[5];
    SecurityService securityService = ((InternalCache) context.getCache()).getSecurityService();

    boolean loginSuccessful = false;
    try {

      // Authenticate If Needed
      if (loginRequired(securityService)) {
        securityService.login(credentials);
        loginSuccessful = true;
      }

      // Load User Function
      Function<?> function = loadFunction(functionId);
      if (function == null) {
        context.getResultSender()
            .lastResult(new CliFunctionResult(context.getMemberName(), ERROR,
                (CliStrings.format(
                    CliStrings.EXECUTE_FUNCTION__MSG__DOES_NOT_HAVE_FUNCTION_0_REGISTERED,
                    functionId))));
        return;
      }

      // Parse Arguments
      Set<String> filters = parseFilters(filterString);
      String[] functionArgs = parseArguments(argumentsString);
      ResultCollector<Object, List<Object>> resultCollectorInstance =
          parseResultCollector(resultCollectorName);

      // Security check
      function.getRequiredPermissions(onRegion, functionArgs).forEach(securityService::authorize);

      // Build & Configure Execution Context
      Execution<Object, Object, List<Object>> execution = buildExecution(cache, onRegion);
      if (execution == null) {
        context.getResultSender()
            .lastResult(new CliFunctionResult(context.getMemberName(), ERROR,
                CliStrings.format(
                    CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_EXECUTING_0_ON_MEMBER_1_ON_REGION_2_DETAILS_3,
                    functionId, member.getId(), onRegion,
                    CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_RETRIEVING_EXECUTOR)));
        return;
      }

      if (resultCollectorInstance != null) {
        execution = execution.withCollector(resultCollectorInstance);
      }

      if (functionArgs != null && functionArgs.length > 0) {
        execution = execution.setArguments(functionArgs);
      }

      if (filters.size() > 0) {
        execution = execution.withFilter(filters);
      }

      // Execute Function and gather results
      List<Object> results = null;
      boolean functionSuccess = true;
      List<String> resultMessage = new ArrayList<>();

      ResultCollector<Object, List<Object>> rc = execution.execute(function.getId());
      if (function.hasResult()) {
        results = rc.getResult();
      }

      if (results != null) {
        for (Object resultObj : results) {
          if (resultObj != null) {
            if (resultObj instanceof Exception) {
              resultMessage.add(((Exception) resultObj).getMessage());
              functionSuccess = false;
            } else {
              resultMessage.add(resultObj.toString());
            }
          }
        }
      }

      context.getResultSender().lastResult(new CliFunctionResult(context.getMemberName(),
          functionSuccess ? OK : ERROR, resultMessage.toString()));
    } catch (RegionNotFoundException regionNotFoundException) {
      context.getResultSender().lastResult(
          new CliFunctionResult(context.getMemberName(), ERROR, onRegion + " does not exist"));
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      context.getResultSender()
          .lastResult(new CliFunctionResult(context.getMemberName(), ERROR,
              CliStrings.format(
                  CliStrings.EXECUTE_FUNCTION__MSG__RESULT_COLLECTOR_0_NOT_FOUND_ERROR_1,
                  resultCollectorName, e.getMessage())));
    } catch (Exception e) {
      logger.error("error executing function " + functionId, e);
      context.getResultSender().lastResult(
          new CliFunctionResult(context.getMemberName(), ERROR, "Exception: " + e.getMessage()));
    } finally {
      if (loginSuccessful) {
        securityService.logout();
      }
    }
  }
}
