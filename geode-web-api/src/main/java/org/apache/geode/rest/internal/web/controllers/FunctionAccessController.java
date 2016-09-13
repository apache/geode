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

package org.apache.geode.rest.internal.web.controllers;

import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.*;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.rest.internal.web.exception.GemfireRestException;
import org.apache.geode.rest.internal.web.util.ArrayUtils;
import org.apache.geode.rest.internal.web.util.JSONUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * The FunctionsController class serving REST Requests related to the function execution
 * @see org.springframework.stereotype.Controller
 * @since GemFire 8.0
 */

@Controller("functionController")
@Api(value = "functions", description = "Rest api for gemfire function execution")
@RequestMapping(FunctionAccessController.REST_API_VERSION + "/functions")
@SuppressWarnings("unused")
public class FunctionAccessController extends AbstractBaseController {
  private static final Logger logger = LogService.getLogger();

  // Constant String value indicating the version of the REST API.
  protected static final String REST_API_VERSION = "/v1";

  /**
   * Gets the version of the REST API implemented by this @Controller.
   * <p>
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
  @RequestMapping(method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_VALUE })
  @ApiOperation(
      value = "list all functions",
      notes = "list all functions available in the GemFire cluster",
      response = void.class
  )
  @ApiResponses({
      @ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")
  })
  @ResponseBody
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity<?> list() {

    if (logger.isDebugEnabled()) {
      logger.debug("Listing all registered Functions in GemFire...");
    }

    final Map<String, Function> registeredFunctions = FunctionService.getRegisteredFunctions();
    String listFunctionsAsJson = JSONUtils.formulateJsonForListFunctionsCall(registeredFunctions.keySet());
    final HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri("functions"));
    return new ResponseEntity<String>(listFunctionsAsJson, headers, HttpStatus.OK);
  }

  /**
   * Execute a function on Gemfire data node using REST API call.
   * Arguments to the function are passed as JSON string in the request body.
   *
   * @param functionId represents function to be executed
   * @param region     list of regions on which function to be executed.
   * @param members    list of nodes on which function to be executed.
   * @param groups     list of groups on which function to be executed.
   * @param filter     list of keys which the function will use to determine on which node to execute the function.
   * @param argsInBody function argument as a JSON document
   * @return result as a JSON document
   */
  @RequestMapping(method = RequestMethod.POST, value = "/{functionId}", produces = { MediaType.APPLICATION_JSON_VALUE })
  @ApiOperation(
      value = "execute function",
      notes = "Execute function with arguments on regions, members, or group(s). By default function will be executed on all nodes if none of (onRegion, onMembers, onGroups) specified",
      response = void.class
  )
  @ApiResponses({
      @ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 500, message = "if GemFire throws an error or exception"),
      @ApiResponse(code = 400, message = "if Function arguments specified as JSON document in the request body is invalid")
  })
  @ResponseBody
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity<String> execute(@PathVariable("functionId") String functionId,
      @RequestParam(value = "onRegion", required = false) String region,
      @RequestParam(value = "onMembers", required = false) final String[] members,
      @RequestParam(value = "onGroups", required = false) final String[] groups,
      @RequestParam(value = "filter", required = false) final String[] filter,
      @RequestBody(required = false) final String argsInBody
  ) {
    Execution function = null;
    functionId = decode(functionId);

    if (StringUtils.hasText(region)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Function ({}) with arguments ({}) on Region ({})...", functionId,
            ArrayUtils.toString(argsInBody), region);
      }

      region = decode(region);
      try {
        function = FunctionService.onRegion(getRegion(region));
      } catch (FunctionException fe) {
        throw new GemfireRestException(String.format("The Region identified by name (%1$s) could not found!", region), fe);
      }
    } else if (ArrayUtils.isNotEmpty(members)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Function ({}) with arguments ({}) on Member ({})...", functionId,
            ArrayUtils.toString(argsInBody), ArrayUtils.toString(members));
      }
      try {
        function = FunctionService.onMembers(getMembers(members));
      } catch (FunctionException fe) {
        throw new GemfireRestException("Could not found the specified members in distributed system!", fe);
      }
    } else if (ArrayUtils.isNotEmpty(groups)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Function ({}) with arguments ({}) on Groups ({})...", functionId,
            ArrayUtils.toString(argsInBody), ArrayUtils.toString(groups));
      }
      try {
        function = FunctionService.onMembers(groups);
      } catch (FunctionException fe) {
        throw new GemfireRestException("no member(s) are found belonging to the provided group(s)!", fe);
      }
    } else {
      //Default case is to execute function on all existing data node in DS, document this.
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Function ({}) with arguments ({}) on all Members...", functionId,
            ArrayUtils.toString(argsInBody));
      }

      try {
        function = FunctionService.onMembers(getAllMembersInDS());
      } catch (FunctionException fe) {
        throw new GemfireRestException("Distributed system does not contain any valid data node to run the specified  function!", fe);
      }
    }

    if (!ArrayUtils.isEmpty(filter)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Function ({}) with filter ({})", functionId,
            ArrayUtils.toString(filter));
      }
      Set filter1 = ArrayUtils.asSet(filter);
      function = function.withFilter(filter1);
    }

    final ResultCollector<?, ?> results;

    try {
      if (argsInBody != null) {
        Object[] args = jsonToObjectArray(argsInBody);

        //execute function with specified arguments
        if (args.length == 1) {
          results = function.withArgs(args[0]).execute(functionId);
        } else {
          results = function.withArgs(args).execute(functionId);
        }
      } else {
        //execute function with no args
        results = function.execute(functionId);
      }
    } catch (ClassCastException cce) {
      throw new GemfireRestException("Key is of an inappropriate type for this region!", cce);
    } catch (NullPointerException npe) {
      throw new GemfireRestException("Specified key is null and this region does not permit null keys!", npe);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException("Server has encountered low memory condition!", lme);
    } catch (IllegalArgumentException ie) {
      throw new GemfireRestException("Input parameter is null! ", ie);
    } catch (FunctionException fe) {
      throw new GemfireRestException("Server has encountered error while executing the function!", fe);
    }

    try {
      Object functionResult = results.getResult();

      if (functionResult instanceof List<?>) {
        final HttpHeaders headers = new HttpHeaders();
        headers.setLocation(toUri("functions", functionId));

        try {
          @SuppressWarnings("unchecked")
          String functionResultAsJson = JSONUtils.convertCollectionToJson((ArrayList<Object>) functionResult);
          return new ResponseEntity<String>(functionResultAsJson, headers, HttpStatus.OK);
        } catch (JSONException e) {
          throw new GemfireRestException("Could not convert function results into Restful (JSON) format!", e);
        }
      } else {
        throw new GemfireRestException("Function has returned results that could not be converted into Restful (JSON) format!");
      }
    } catch (FunctionException fe) {
      fe.printStackTrace();
      throw new GemfireRestException("Server has encountered an error while processing function execution!", fe);
    }
  }
}

