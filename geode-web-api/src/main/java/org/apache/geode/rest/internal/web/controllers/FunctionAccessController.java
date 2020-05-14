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
package org.apache.geode.rest.internal.web.controllers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.execute.NoResult;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.rest.internal.web.exception.GemfireRestException;
import org.apache.geode.rest.internal.web.util.ArrayUtils;
import org.apache.geode.rest.internal.web.util.JSONUtils;
import org.apache.geode.security.ResourcePermission;

/**
 * The FunctionsController class serving REST Requests related to the function execution
 *
 * @see org.springframework.stereotype.Controller
 * @since GemFire 8.0
 */
@Controller("functionController")
@Api(value = "functions", tags = "functions")
@RequestMapping(FunctionAccessController.REST_API_VERSION + "/functions")
@SuppressWarnings("unused")
public class FunctionAccessController extends AbstractBaseController {
  // Constant String value indicating the version of the REST API.
  static final String REST_API_VERSION = "/v1";
  private static final Logger logger = LogService.getLogger();

  /**
   * Gets the version of the REST API implemented by this @Controller.
   *
   * @return a String indicating the REST API version.
   */
  @Override
  protected String getRestApiVersion() {
    return REST_API_VERSION;
  }

  /**
   * list all registered functions in Gemfire data node
   *
   * @return result as a JSON document.
   */
  @RequestMapping(method = RequestMethod.GET, produces = {APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "list all functions",
      notes = "list all functions available in the GemFire cluster")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @ResponseBody
  @ResponseStatus(HttpStatus.OK)
  @PreAuthorize("@securityService.authorize('DATA', 'READ')")
  public ResponseEntity<?> list() {
    logger.debug("Listing all registered Functions in GemFire...");

    @SuppressWarnings("unchecked")
    final Map<String, Function<?>> registeredFunctions =
        (Map<String, Function<?>>) (Map<?, ?>) FunctionService.getRegisteredFunctions();
    String listFunctionsAsJson =
        JSONUtils.formulateJsonForListFunctionsCall(registeredFunctions.keySet());
    final HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri("functions"));
    return new ResponseEntity<>(listFunctionsAsJson, headers, HttpStatus.OK);
  }

  /**
   * Execute a function on Gemfire data node using REST API call. Arguments to the function are
   * passed as JSON string in the request body.
   *
   * @param functionId represents function to be executed
   * @param region list of regions on which function to be executed.
   * @param members list of nodes on which function to be executed.
   * @param groups list of groups on which function to be executed.
   * @param filter list of keys which the function will use to determine on which node to execute
   *        the function.
   * @param argsInBody function argument as a JSON document
   * @return result as a JSON document
   */
  @RequestMapping(method = RequestMethod.POST, value = "/{functionId:.+}",
      produces = {APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "execute function",
      notes = "Execute function with arguments on regions, members, or group(s). By default function will be executed on all nodes if none of (onRegion, onMembers, onGroups) specified")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 500, message = "if GemFire throws an error or exception"),
      @ApiResponse(code = 400,
          message = "if Function arguments specified as JSON document in the request body is invalid")})
  @ResponseBody
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity<String> execute(@PathVariable("functionId") String functionId,
      @RequestParam(value = "onRegion", required = false) String region,
      @RequestParam(value = "onMembers", required = false) final String[] members,
      @RequestParam(value = "onGroups", required = false) final String[] groups,
      @RequestParam(value = "filter", required = false) final String[] filter,
      @RequestBody(required = false) final String argsInBody) {

    Function<?> function = FunctionService.getFunction(functionId);

    // this exception will be handled by BaseControllerAdvice to eventually return a 404
    if (function == null) {
      throw new EntityNotFoundException(
          String.format("The function %s is not registered.", functionId));
    }

    Object[] args = null;
    if (argsInBody != null) {
      args = jsonToObjectArray(argsInBody);
    }

    // check for required permissions of the function
    Collection<ResourcePermission> requiredPermissions =
        function.getRequiredPermissions(region, args);
    for (ResourcePermission requiredPermission : requiredPermissions) {
      securityService.authorize(requiredPermission);
    }

    Execution<Object, ?, ?> execution;
    functionId = decode(functionId);

    if (StringUtils.hasText(region)) {
      execution = executeOnRegion(functionId, region, argsInBody);
    } else if (ArrayUtils.isNotEmpty(members)) {
      execution = executeOnMembers(functionId, members, argsInBody);
    } else if (ArrayUtils.isNotEmpty(groups)) {
      execution = executeOnGroups(functionId, groups, argsInBody);
    } else {
      execution = executeOnAllMembers(functionId, argsInBody);
    }

    if (!ArrayUtils.isEmpty(filter)) {
      logger.debug("Executing Function ({}) with filter ({})", functionId,
          ArrayUtils.toString(filter));

      Set<String> filter1 = ArrayUtils.asSet(filter);
      execution = execution.withFilter(filter1);
    }

    final ResultCollector<?, ?> results;

    try {
      if (args != null) {
        // execute function with specified arguments
        if (args.length == 1) {
          results = execution.setArguments(args[0]).execute(functionId);
        } else {
          results = execution.setArguments(args).execute(functionId);
        }
      } else {
        // execute function with no args
        results = execution.execute(functionId);
      }
    } catch (ClassCastException cce) {
      throw new GemfireRestException("Key is of an inappropriate type for this region!", cce);
    } catch (NullPointerException npe) {
      throw new GemfireRestException(
          "Specified key is null and this region does not permit null keys!", npe);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException("Server has encountered low memory condition!", lme);
    } catch (IllegalArgumentException ie) {
      throw new GemfireRestException("Input parameter is null! ", ie);
    } catch (FunctionException fe) {
      throw new GemfireRestException("Server has encountered error while executing the function!",
          fe);
    }

    try {
      final HttpHeaders headers = new HttpHeaders();
      headers.setLocation(toUri("functions", functionId));

      Object functionResult;
      if (results instanceof NoResult) {
        return new ResponseEntity<>("", headers, HttpStatus.OK);
      }
      functionResult = results.getResult();

      if (functionResult instanceof List<?>) {
        @SuppressWarnings("unchecked")
        String functionResultAsJson =
            JSONUtils.convertCollectionToJson((ArrayList<Object>) functionResult);
        return new ResponseEntity<>(functionResultAsJson, headers, HttpStatus.OK);
      } else {
        throw new GemfireRestException(
            "Function has returned results that could not be converted into Restful (JSON) format!");
      }
    } catch (FunctionException fe) {
      fe.printStackTrace();
      throw new GemfireRestException(
          "Server has encountered an error while processing function execution!", fe);
    }
  }

  private Execution<Object, ?, ?> executeOnAllMembers(String functionId, String argsInBody) {
    // Default case is to execute function on all existing data node in DS, document this.
    logger.debug("Executing Function ({}) with arguments ({}) on all Members...", functionId,
        ArrayUtils.toString(argsInBody));

    try {
      @SuppressWarnings("unchecked")
      Execution<Object, ?, ?> execution = FunctionService.onMembers(getAllMembersInDS());
      return execution;
    } catch (FunctionException fe) {
      throw new GemfireRestException(
          "Distributed system does not contain any valid data node to run the specified  function!",
          fe);
    }
  }

  private Execution<Object, ?, ?> executeOnGroups(String functionId, String[] groups,
      String argsInBody) {
    logger.debug("Executing Function ({}) with arguments ({}) on Groups ({})...", functionId,
        ArrayUtils.toString(argsInBody), ArrayUtils.toString(groups));

    try {
      @SuppressWarnings("unchecked")
      Execution<Object, ?, ?> execution = FunctionService.onMembers(groups);
      return execution;
    } catch (FunctionException fe) {
      throw new GemfireRestException("no member(s) are found belonging to the provided group(s)!",
          fe);
    }
  }

  private Execution<Object, ?, ?> executeOnMembers(String functionId, String[] members,
      String argsInBody) {
    logger.debug("Executing Function ({}) with arguments ({}) on Member ({})...", functionId,
        ArrayUtils.toString(argsInBody), ArrayUtils.toString(members));

    try {
      @SuppressWarnings("unchecked")
      Execution<Object, ?, ?> execution = FunctionService.onMembers(getMembers(members));
      return execution;
    } catch (FunctionException fe) {
      throw new GemfireRestException(
          "Could not found the specified members in distributed system!", fe);
    }
  }

  private Execution<Object, ?, ?> executeOnRegion(String functionId, String region,
      String argsInBody) {
    logger.debug("Executing Function ({}) with arguments ({}) on Region ({})...", functionId,
        ArrayUtils.toString(argsInBody), region);

    region = decode(region);
    try {
      @SuppressWarnings("unchecked")
      Execution<Object, ?, ?> execution = FunctionService.onRegion(getRegion(region));
      return execution;
    } catch (FunctionException fe) {
      throw new GemfireRestException(
          String.format("The Region identified by name (%1$s) could not found!", region), fe);
    }
  }
}
