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
import java.util.List;
import java.util.Map;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.rest.internal.web.controllers.support.JSONTypes;
import org.apache.geode.rest.internal.web.controllers.support.RegionData;
import org.apache.geode.rest.internal.web.controllers.support.RegionEntryData;
import org.apache.geode.rest.internal.web.exception.ResourceNotFoundException;
import org.apache.geode.rest.internal.web.util.ArrayUtils;

/**
 * The PdxBasedCrudController class serving REST Requests related to the REST CRUD operation on
 * region
 *
 * @see org.springframework.stereotype.Controller
 * @since GemFire 8.0
 */
@Controller("pdxCrudController")
@Api(value = "region", tags = "region")
@RequestMapping(PdxBasedCrudController.REST_API_VERSION)
@SuppressWarnings("unused")
public class PdxBasedCrudController extends CommonCrudController {

  private static final Logger logger = LogService.getLogger();

  static final String REST_API_VERSION = "/v1";

  private static final String DEFAULT_GETALL_RESULT_LIMIT = "50";

  @Override
  protected String getRestApiVersion() {
    return REST_API_VERSION;
  }

  /**
   * Creating entry into the region
   *
   * @param region region name where data will be created
   * @param key gemfire region key
   * @param json JSON document that is stored against the key
   * @return JSON document
   */
  @RequestMapping(method = RequestMethod.POST, value = "/{region}",
      consumes = MediaType.APPLICATION_JSON_VALUE, produces = {MediaType.APPLICATION_JSON_VALUE})
  @ApiOperation(value = "create entry", notes = "Create (put-if-absent) data in region")
  @ApiResponses({@ApiResponse(code = 201, message = "Created."),
      @ApiResponse(code = 400,
          message = "Data specified (JSON doc) in the request body is invalid."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region does not exist."),
      @ApiResponse(code = 409, message = "Key already exist in region."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'WRITE', #region)")
  public ResponseEntity<?> create(@PathVariable("region") String region,
      @RequestParam(value = "key", required = false) String key, @RequestBody final String json) {
    key = generateKey(key);

    logger.debug(
        "Posting (creating/putIfAbsent) JSON document ({}) to Region ({}) with Key ({})...", json,
        region, key);

    region = decode(region);
    Object existingPdxObj;

    // Check whether the user has supplied single JSON doc or Array of JSON docs
    final JSONTypes jsonType = validateJsonAndFindType(json);
    if (JSONTypes.JSON_ARRAY.equals(jsonType)) {
      existingPdxObj = postValue(region, key, convertJsonArrayIntoPdxCollection(json));
    } else {
      existingPdxObj = postValue(region, key, convert(json));
    }

    final HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri(region, key));

    if (existingPdxObj != null) {
      final RegionEntryData<Object> data = new RegionEntryData<>(region);
      data.add(existingPdxObj);
      headers.setContentType(MediaType.APPLICATION_JSON);
      return new ResponseEntity<RegionEntryData<?>>(data, headers, HttpStatus.CONFLICT);
    } else {
      return new ResponseEntity<String>(headers, HttpStatus.CREATED);
    }
  }

  /**
   * Read all or fixed number of data in a given Region
   *
   * @param region gemfire region name
   * @param limit total number of entries requested
   * @return JSON document
   */
  @RequestMapping(method = RequestMethod.GET, value = "/{region}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @ApiOperation(value = "read all data for region",
      notes = "Read all data for region. Use limit param to get fixed or limited number of entries.")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 400, message = "Bad request."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region does not exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'READ', #region)")
  public ResponseEntity<?> read(@PathVariable("region") String region,
      @RequestParam(value = "limit",
          defaultValue = DEFAULT_GETALL_RESULT_LIMIT) final String limit) {
    logger.debug("Reading all data in Region ({})...", region);

    region = decode(region);

    Map<Object, Object> valueObjs = null;
    final RegionData<Object> data = new RegionData<>(region);

    final HttpHeaders headers = new HttpHeaders();
    String keyList;
    int regionSize = getRegion(region).size();
    List<Object> keys = new ArrayList<>(regionSize);
    List<Object> values = new ArrayList<>(regionSize);

    for (Map.Entry<Object, Object> entry : getValues(region).entrySet()) {
      Object value = entry.getValue();
      if (value != null) {
        keys.add(entry.getKey());
        values.add(value);
      }
    }

    if ("ALL".equalsIgnoreCase(limit)) {
      data.add(values);
      keyList = StringUtils.collectionToDelimitedString(keys, ",");
    } else {
      try {
        int maxLimit = Integer.parseInt(limit);
        if (maxLimit < 0) {
          String errorMessage =
              String.format("Negative limit param (%1$s) is not valid!", maxLimit);
          return new ResponseEntity<>(convertErrorAsJson(errorMessage), HttpStatus.BAD_REQUEST);
        }

        int mapSize = keys.size();
        if (maxLimit > mapSize) {
          maxLimit = mapSize;
        }
        data.add(values.subList(0, maxLimit));

        keyList = StringUtils.collectionToDelimitedString(keys.subList(0, maxLimit), ",");

      } catch (NumberFormatException e) {
        // limit param is not specified in proper format. set the HTTPHeader
        // for BAD_REQUEST
        String errorMessage = String.format("limit param (%1$s) is not valid!", limit);
        return new ResponseEntity<>(convertErrorAsJson(errorMessage), HttpStatus.BAD_REQUEST);
      }
    }

    headers.set("Content-Location", toUri(region, keyList).toASCIIString());
    return new ResponseEntity<RegionData<?>>(data, headers, HttpStatus.OK);
  }

  /**
   * Reading data for set of keys
   *
   * @param region gemfire region name
   * @param keys string containing comma separated keys
   * @return JSON document
   */
  @RequestMapping(method = RequestMethod.GET, value = "/{region}/{keys}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @ApiOperation(value = "read data for specific keys",
      notes = "Read data for specific set of keys in region.")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 400, message = "Bad Request."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region does not exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('READ', #region, #keys)")
  public ResponseEntity<?> read(@PathVariable("region") String region,
      @PathVariable("keys") final String[] keys,
      @RequestParam(value = "ignoreMissingKey", required = false) final String ignoreMissingKey) {
    logger.debug("Reading data for keys ({}) in Region ({})", ArrayUtils.toString(keys), region);

    final HttpHeaders headers = new HttpHeaders();
    region = decode(region);

    if (keys.length == 1) {
      /* GET op on single key */
      Object value = getValue(region, keys[0]);
      // if region.get(K) return null (i.e INVLD or TOMBSTONE case) We consider 404, NOT Found case
      if (value == null) {
        throw new ResourceNotFoundException(String
            .format("Key (%1$s) does not exist for region (%2$s) in cache!", keys[0], region));
      }

      final RegionEntryData<Object> data = new RegionEntryData<>(region);
      headers.set("Content-Location", toUri(region, keys[0]).toASCIIString());
      data.add(value);
      return new ResponseEntity<RegionData<?>>(data, headers, HttpStatus.OK);

    } else {
      // fail fast for the case where ignoreMissingKey param is not specified correctly.
      if (ignoreMissingKey != null && !(ignoreMissingKey.equalsIgnoreCase("true")
          || ignoreMissingKey.equalsIgnoreCase("false"))) {
        String errorMessage = String.format(
            "ignoreMissingKey param (%1$s) is not valid. valid usage is ignoreMissingKey=true!",
            ignoreMissingKey);
        return new ResponseEntity<>(convertErrorAsJson(errorMessage), HttpStatus.BAD_REQUEST);
      }

      if (!("true".equalsIgnoreCase(ignoreMissingKey))) {
        List<String> unknownKeys = checkForMultipleKeysExist(region, keys);
        if (unknownKeys.size() > 0) {
          String unknownKeysAsStr = StringUtils.collectionToDelimitedString(unknownKeys, ",");
          String erroString = String.format("Requested keys (%1$s) not exist in region (%2$s)",
              StringUtils.collectionToDelimitedString(unknownKeys, ","), region);
          return new ResponseEntity<>(convertErrorAsJson(erroString), headers,
              HttpStatus.BAD_REQUEST);
        }
      }

      final Map<Object, Object> valueObjs = getValues(region, keys);

      // Do we need to remove null values from Map..?
      // To Remove null value entries from map.
      // valueObjs.values().removeAll(Collections.singleton(null));

      // currently we are not removing keys having value null from the result.
      String keyList = StringUtils.collectionToDelimitedString(valueObjs.keySet(), ",");
      headers.set("Content-Location", toUri(region, keyList).toASCIIString());
      final RegionData<Object> data = new RegionData<>(region);
      data.add(valueObjs.values());
      return new ResponseEntity<RegionData<?>>(data, headers, HttpStatus.OK);
    }
  }

  /**
   * Update data for a key or set of keys
   *
   * @param region gemfire data region
   * @param keys keys for which update operation is requested
   * @param opValue type of update (put, replace, cas etc)
   * @param json new data for the key(s)
   * @return JSON document
   */
  @RequestMapping(method = RequestMethod.PUT, value = "/{region}/{keys}",
      consumes = {MediaType.APPLICATION_JSON_VALUE}, produces = {MediaType.APPLICATION_JSON_VALUE})
  @ApiOperation(value = "update data for key",
      notes = "Update or insert (put) data for key in region."
          + "op=REPLACE, update (replace) data with key if and only if the key exists in region"
          + "op=CAS update (compare-and-set) value having key with a new value if and only if the \"@old\" value sent matches the current value for the key in region")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 400, message = "Bad Request."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404,
          message = "Region does not exist or if key is not mapped to some value for REPLACE or CAS."),
      @ApiResponse(code = 409,
          message = "For CAS, @old value does not match to the current value in region"),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('WRITE', #region, #keys)")
  public ResponseEntity<?> update(@PathVariable("region") String region,
      @PathVariable("keys") final String[] keys,
      @RequestParam(value = "op", defaultValue = "PUT") final String opValue,
      @RequestBody final String json) {
    logger.debug("updating key(s) for region ({}) ", region);

    region = decode(region);

    if (keys.length > 1) {
      // putAll case
      return updateMultipleKeys(region, keys, json);
    } else {
      // put case
      return updateSingleKey(region, keys[0], json, opValue);
    }
  }

  @RequestMapping(method = RequestMethod.HEAD, value = "/{region}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @ApiOperation(value = "Get total number of entries",
      notes = "Get total number of entries into the specified region")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 400, message = "Bad request."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region does not exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'READ', #region)")
  public ResponseEntity<?> size(@PathVariable("region") String region) {
    logger.debug("Determining the number of entries in Region ({})...", region);

    region = decode(region);

    final HttpHeaders headers = new HttpHeaders();

    headers.set("Resource-Count", String.valueOf(getRegion(region).size()));
    return new ResponseEntity<RegionData<?>>(headers, HttpStatus.OK);
  }

}
