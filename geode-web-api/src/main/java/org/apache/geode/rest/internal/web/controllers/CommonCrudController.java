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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.execute.util.FindRestEnabledServersFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.rest.internal.web.controllers.support.RestServersResultCollector;
import org.apache.geode.rest.internal.web.exception.GemfireRestException;
import org.apache.geode.rest.internal.web.util.ArrayUtils;
import org.apache.geode.rest.internal.web.util.JSONUtils;

/**
 * The CommonCrudController serves REST Requests related to listing regions, listing keys in region,
 * delete keys or delete all data in region.
 *
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public abstract class CommonCrudController extends AbstractBaseController {

  private static final Logger logger = LogService.getLogger();

  /**
   * List all available resources (Regions) in the GemFire cluster
   *
   * @return JSON document containing result
   */
  @RequestMapping(method = RequestMethod.GET, produces = {APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "list all resources (Regions)",
      notes = "List all available resources (Regions) in the Geode cluster")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'READ')")
  public ResponseEntity<?> regions() {
    logger.debug("Listing all resources (Regions) in Geode...");
    final HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri());
    final Set<Region<?, ?>> regions = new HashSet<>();
    for (InternalRegion region : getCache().getApplicationRegions()) {
      regions.add(region);
    }
    String listRegionsAsJson = JSONUtils.formulateJsonForListRegions(regions, "regions");
    return new ResponseEntity<>(listRegionsAsJson, headers, HttpStatus.OK);
  }

  /**
   * List all keys for the given region in the GemFire cluster
   *
   * @param region gemfire region
   * @return JSON document containing result
   */
  @RequestMapping(method = RequestMethod.GET, value = "/{region}/keys",
      produces = {APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "list all keys", notes = "List all keys in region")
  @ApiResponses({@ApiResponse(code = 200, message = "OK"),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region does not exist"),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception")})
  @PreAuthorize("@securityService.authorize('DATA', 'READ', #region)")
  public ResponseEntity<?> keys(@PathVariable("region") String region) {
    logger.debug("Reading all Keys in Region ({})...", region);

    region = decode(region);

    Object[] keys = getKeys(region, null);

    String listKeysAsJson = JSONUtils.formulateJsonForListKeys(keys, "keys");
    final HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri(region, "keys"));
    return new ResponseEntity<>(listKeysAsJson, headers, HttpStatus.OK);
  }

  /**
   * Delete data for single key or specific keys in region
   *
   * @param region gemfire region
   * @return JSON document containing result
   */
  @RequestMapping(method = RequestMethod.DELETE, value = "/{region}/**",
      produces = {APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "delete data for key(s)",
      notes = "Delete data for one or more keys in a region. The keys, ** in the endpoint, are a comma separated list.")
  @ApiResponses({@ApiResponse(code = 200, message = "OK"),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region or key(s) does not exist"),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception")})
  public ResponseEntity<?> delete(@PathVariable("region") String region,
      HttpServletRequest request) {
    String[] keys = parseKeys(request, region);
    securityService.authorize("WRITE", region, keys);
    logger.debug("Delete data for key {} on region {}", ArrayUtils.toString((Object[]) keys),
        region);

    region = decode(region);

    deleteValues(region, keys);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  /**
   * Delete all data in region
   *
   * @param region gemfire region
   * @return JSON document containing result
   */
  @RequestMapping(method = RequestMethod.DELETE, value = "/{region}")
  @ApiOperation(value = "delete all data", notes = "Delete all data in the region")
  @ApiResponses({@ApiResponse(code = 200, message = "OK"),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region does not exist"),
      @ApiResponse(code = 500, message = "if GemFire throws an error or exception")})
  @PreAuthorize("@securityService.authorize('DATA', 'WRITE', #region)")
  public ResponseEntity<?> delete(@PathVariable("region") String region) {
    logger.debug("Deleting all data in Region ({})...", region);

    region = decode(region);

    deleteValues(region);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  /**
   * Ping is not secured so that it may not be used to determine a valid username/password
   */
  @RequestMapping(method = {RequestMethod.GET, RequestMethod.HEAD}, value = "/ping")
  @ApiOperation(value = "Check Rest service status ",
      notes = "Check whether gemfire REST service is up and running!")
  @ApiResponses({@ApiResponse(code = 200, message = "OK"),
      @ApiResponse(code = 500, message = "if GemFire throws an error or exception")})
  public ResponseEntity<?> ping() {
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @RequestMapping(method = {RequestMethod.GET}, value = "/servers",
      produces = {APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "fetch all REST enabled servers in the DS",
      notes = "Find all gemfire node where developer REST service is up and running!")
  @ApiResponses({@ApiResponse(code = 200, message = "OK"),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 500, message = "if GemFire throws an error or exception")})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  public ResponseEntity<?> servers() {
    logger.debug("Executing function to get REST enabled gemfire nodes in the DS!");

    Execution<?, ?, ?> function;
    try {
      function = FunctionService.onMembers(getAllMembersInDS());
    } catch (FunctionException fe) {
      throw new GemfireRestException(
          "Distributed system does not contain any valid data node that can host REST service!",
          fe);
    }

    try {
      final ResultCollector<?, ?> results =
          function.withCollector(new RestServersResultCollector<>())
              .execute(FindRestEnabledServersFunction.FIND_REST_ENABLED_SERVERS_FUNCTION_ID);
      Object functionResult = results.getResult();

      if (functionResult instanceof List<?>) {
        final HttpHeaders headers = new HttpHeaders();
        headers.setLocation(toUri("servers"));
        @SuppressWarnings("unchecked")
        final ArrayList<Object> functionResultList = (ArrayList<Object>) functionResult;
        String functionResultAsJson =
            JSONUtils.convertCollectionToJson(functionResultList);
        return new ResponseEntity<>(functionResultAsJson, headers, HttpStatus.OK);
      } else {
        throw new GemfireRestException(
            "Function has returned results that could not be converted into Restful (JSON) format!");
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
  }
}
