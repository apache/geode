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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

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
  @Operation(summary = "list all resources (Regions)",
      description = "List all available resources (Regions) in the Geode cluster")
  @ApiResponses({@ApiResponse(responseCode = "200", description = "OK."),
      @ApiResponse(responseCode = "401", description = "Invalid Username or Password."),
      @ApiResponse(responseCode = "403", description = "Insufficient privileges for operation."),
      @ApiResponse(responseCode = "500", description = "GemFire throws an error or exception.")})
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
  @Operation(summary = "list all keys", description = "List all keys in region")
  @ApiResponses({@ApiResponse(responseCode = "200", description = "OK"),
      @ApiResponse(responseCode = "401", description = "Invalid Username or Password."),
      @ApiResponse(responseCode = "403", description = "Insufficient privileges for operation."),
      @ApiResponse(responseCode = "404", description = "Region does not exist"),
      @ApiResponse(responseCode = "500", description = "GemFire throws an error or exception")})
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
   * Delete data for one or more keys from a region
   *
   * @param region gemfire region
   * @return JSON document containing result
   */
  @RequestMapping(method = RequestMethod.DELETE, value = "/{region}/{keys}",
      produces = {APPLICATION_JSON_UTF8_VALUE})
  @Operation(summary = "delete data for key(s)",
      description = "Delete data for one or more keys in a region. Deprecated in favor of /{region}?keys=.")
  @ApiResponses({@ApiResponse(responseCode = "200", description = "OK"),
      @ApiResponse(responseCode = "401", description = "Invalid Username or Password."),
      @ApiResponse(responseCode = "403", description = "Insufficient privileges for operation."),
      @ApiResponse(responseCode = "404", description = "Region or key(s) does not exist"),
      @ApiResponse(responseCode = "500", description = "GemFire throws an error or exception")})
  public ResponseEntity<?> delete(@PathVariable("region") String region,
      @PathVariable("keys") String[] keys) {
    region = decode(region);
    return deleteRegionKeys(region, keys);
  }

  private ResponseEntity<?> deleteRegionKeys(String region, String[] keys) {
    securityService.authorize("WRITE", region, keys);
    logger.debug("Delete data for keys {} on region {}", ArrayUtils.toString((Object[]) keys),
        region);
    deleteValues(region, keys);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  /**
   * Delete all data in region or just the given keys
   *
   * @param region gemfire region
   * @param encodedKeys optional comma separated list of keys
   * @return JSON document containing result
   */
  @RequestMapping(method = RequestMethod.DELETE, value = "/{region}")
  @Operation(summary = "delete all data or the specified keys",
      description = "Delete all in the region or just the specified keys")
  @ApiResponses({@ApiResponse(responseCode = "200", description = "OK"),
      @ApiResponse(responseCode = "401", description = "Invalid Username or Password."),
      @ApiResponse(responseCode = "403", description = "Insufficient privileges for operation."),
      @ApiResponse(responseCode = "404", description = "Region does not exist"),
      @ApiResponse(responseCode = "500", description = "if GemFire throws an error or exception")})
  public ResponseEntity<?> deleteAllOrGivenKeys(@PathVariable("region") String region,
      @RequestParam(value = "keys", required = false) final String[] encodedKeys) {
    logger.debug("Deleting all data in Region ({})...", region);

    region = decode(region);
    if (encodedKeys == null || encodedKeys.length == 0) {
      return deleteAllRegionData(region);
    } else {
      String[] decodedKeys = decode(encodedKeys);
      return deleteRegionKeys(region, decodedKeys);
    }
  }

  private ResponseEntity<?> deleteAllRegionData(String region) {
    securityService.authorize("DATA", "WRITE", region);
    logger.debug("Deleting all data in Region ({})...", region);
    deleteValues(region);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  /**
   * Ping is not secured so that it may not be used to determine a valid username/password
   */
  @RequestMapping(method = {RequestMethod.GET, RequestMethod.HEAD}, value = "/ping")
  @Operation(summary = "Check Rest service status ",
      description = "Check whether gemfire REST service is up and running!")
  @ApiResponses({@ApiResponse(responseCode = "200", description = "OK"),
      @ApiResponse(responseCode = "500", description = "if GemFire throws an error or exception")})
  public ResponseEntity<?> ping() {
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @RequestMapping(method = {RequestMethod.GET}, value = "/servers",
      produces = {APPLICATION_JSON_UTF8_VALUE})
  @Operation(summary = "fetch all REST enabled servers in the DS",
      description = "Find all gemfire node where developer REST service is up and running!")
  @ApiResponses({@ApiResponse(responseCode = "200", description = "OK"),
      @ApiResponse(responseCode = "401", description = "Invalid Username or Password."),
      @ApiResponse(responseCode = "403", description = "Insufficient privileges for operation."),
      @ApiResponse(responseCode = "500", description = "if GemFire throws an error or exception")})
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
