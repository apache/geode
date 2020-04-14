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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

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

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.rest.internal.web.controllers.support.JSONTypes;
import org.apache.geode.rest.internal.web.controllers.support.RegionData;
import org.apache.geode.rest.internal.web.controllers.support.RegionEntryData;
import org.apache.geode.rest.internal.web.controllers.support.UpdateOp;
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
      consumes = APPLICATION_JSON_UTF8_VALUE, produces = {APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "create entry", notes = "Create (put-if-absent) data in region."
      + " The key is not decoded so if the key contains special characters use PUT/{region}?keys=EncodedKey&op=CREATE.")
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
    region = decode(region);
    return create(region, key, json, false);
  }

  private ResponseEntity<?> create(String region, String key, String json,
      boolean keyInQueryParam) {
    logger.debug("Create JSON document ({}) in Region ({}) with Key ({})...", json, region, key);

    Object existingPdxObj;

    // Check whether the user has supplied single JSON doc or Array of JSON docs
    final JSONTypes jsonType = validateJsonAndFindType(json);
    if (JSONTypes.JSON_ARRAY.equals(jsonType)) {
      existingPdxObj = postValue(region, key, convertJsonArrayIntoPdxCollection(json));
    } else {
      existingPdxObj = postValue(region, key, convert(json));
    }

    final HttpHeaders headers = new HttpHeaders();
    if (keyInQueryParam) {
      headers.setLocation(toUriWithKeys(new String[] {encode(key)}, region));
    } else {
      headers.setLocation(toUri(region, key));
    }

    if (existingPdxObj != null) {
      final RegionEntryData<Object> data = new RegionEntryData<>(region);
      data.add(existingPdxObj);
      headers.setContentType(APPLICATION_JSON_UTF8);
      return new ResponseEntity<RegionEntryData<?>>(data, headers, HttpStatus.CONFLICT);
    } else {
      return new ResponseEntity<String>(headers, HttpStatus.CREATED);
    }
  }

  /**
   * For the given region either gets all the region's data (with an optional limit),
   * or gets the region's data for the given keys (optionally ignoring missing keys).
   *
   * @param region gemfire region name
   * @param limit total number of entries requested
   * @param encodedKeys an optional comma separated list of encoded keys to read
   * @param ignoreMissingKey if true and reading more than one key then if a key is missing ignore
   * @return JSON document
   */
  @RequestMapping(method = RequestMethod.GET, value = "/{region}",
      produces = APPLICATION_JSON_UTF8_VALUE)
  @ApiOperation(value = "read all data for region or the specified keys",
      notes = "If reading all data for region then the limit parameter can be used to give the maximum number of values to return."
          + " If reading specif keys then the ignoredMissingKey parameter can be used to not fail if a key is missing.")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 400, message = "Bad request."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region does not exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'READ', #region)")
  public ResponseEntity<?> read(@PathVariable("region") String region,
      @RequestParam(value = "limit",
          defaultValue = DEFAULT_GETALL_RESULT_LIMIT) final String limit,
      @RequestParam(value = "keys", required = false) final String[] encodedKeys,
      @RequestParam(value = "ignoreMissingKey", required = false) final String ignoreMissingKey) {
    logger.debug("Reading all data in Region ({})...", region);
    region = decode(region);
    if (encodedKeys == null || encodedKeys.length == 0) {
      return getAllRegionData(region, limit);
    } else {
      String[] decodedKeys = decode(encodedKeys);
      return getRegionKeys(region, ignoreMissingKey, decodedKeys, true);
    }
  }

  private ResponseEntity<?> getAllRegionData(String region, String limit) {
    securityService.authorize("DATA", "READ", region);
    logger.debug("Reading all data in Region ({})...", region);
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
      keyList = StringUtils.collectionToCommaDelimitedString(keys);
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

        keyList = StringUtils.collectionToCommaDelimitedString(keys.subList(0, maxLimit));

      } catch (NumberFormatException e) {
        // limit param is not specified in proper format. set the HTTPHeader
        // for BAD_REQUEST
        String errorMessage = String.format("limit param (%1$s) is not valid!", limit);
        return new ResponseEntity<>(convertErrorAsJson(errorMessage), HttpStatus.BAD_REQUEST);
      }
    }

    headers.set(HttpHeaders.CONTENT_LOCATION, toUri(region, keyList).toASCIIString());
    return new ResponseEntity<RegionData<?>>(data, headers, HttpStatus.OK);
  }

  /**
   * Reading data for set of keys
   *
   * @param region gemfire region name
   * @param keys optional list of keys to read
   * @param ignoreMissingKey if true and reading more than one key then if a key is missing ignore
   * @return JSON document
   */
  @RequestMapping(method = RequestMethod.GET, value = "/{region}/{keys}",
      produces = APPLICATION_JSON_UTF8_VALUE)
  @ApiOperation(value = "read data for specific keys",
      notes = "Read data for specif set of keys in a region. Deprecated in favor of /{region}?keys=.")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 400, message = "Bad Request."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "Region does not exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  public ResponseEntity<?> read(@PathVariable("region") String region,
      @PathVariable("keys") final String[] keys,
      @RequestParam(value = "ignoreMissingKey", required = false) final String ignoreMissingKey) {
    region = decode(region);
    return getRegionKeys(region, ignoreMissingKey, keys, false);
  }

  private ResponseEntity<?> getRegionKeys(String region, String ignoreMissingKey, String[] keys,
      boolean keysInQueryParam) {
    logger.debug("Reading data for keys ({}) in Region ({})", ArrayUtils.toString(keys), region);
    securityService.authorize("READ", region, keys);
    final HttpHeaders headers = new HttpHeaders();
    if (keys.length == 1) {
      /* GET op on single key */
      Object value = getValue(region, keys[0]);
      // if region.get(K) return null (i.e INVLD or TOMBSTONE case) We consider 404, NOT Found case
      if (value == null) {
        throw new ResourceNotFoundException(String
            .format("Key (%1$s) does not exist for region (%2$s) in cache!", keys[0], region));
      }

      final RegionEntryData<Object> data = new RegionEntryData<>(region);
      URI uri;
      if (keysInQueryParam) {
        String[] encodedKeys = encode(keys);
        String encodedRegion = encode(region);
        uri = this.toUriWithKeys(encodedKeys, encodedRegion);
      } else {
        uri = toUri(region, keys[0]);
      }
      headers.set(HttpHeaders.CONTENT_LOCATION, uri.toASCIIString());
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

      final Map<Object, Object> valueObjs = getValues(region, keys);
      // valueObjs will have as its keys all of "keys".
      // valueObjs will have a null value if the key did not exist.
      // So if ignoreMissingKey is false we can use "null" values to detect the missing keys.
      if (!("true".equalsIgnoreCase(ignoreMissingKey))) {
        List<String> unknownKeys = new ArrayList<>();
        // use "keys" to iterate so we get the original key ordering from user.
        for (String key : keys) {
          if (valueObjs.get(key) == null) {
            unknownKeys.add(key);
          }
        }
        if (!unknownKeys.isEmpty()) {
          String unknownKeysAsStr = StringUtils.collectionToCommaDelimitedString(unknownKeys);
          String errorString = String.format("Requested keys (%1$s) do not exist in region (%2$s)",
              unknownKeysAsStr, region);
          return new ResponseEntity<>(convertErrorAsJson(errorString), headers,
              HttpStatus.BAD_REQUEST);
        }
      }

      // The dev rest api was already released with null values being returned
      // for non-existent keys.
      // Order the keys in the result after the array of keys given to this method.
      // Previous code returned them in random order which the result harder to test and use.

      URI uri;
      if (keysInQueryParam) {
        String[] encodedKeys = encode(keys);
        String encodedRegion = encode(region);
        uri = this.toUriWithKeys(encodedKeys, encodedRegion);
      } else {
        String keyList = StringUtils.arrayToCommaDelimitedString(keys);
        uri = toUri(region, keyList);
      }

      headers.set(HttpHeaders.CONTENT_LOCATION, uri.toASCIIString());
      final RegionData<Object> data = new RegionData<>(region);
      // add the values in the same order as the original keys
      // the code used to use valueObj.values() which used "hash" ordering.
      for (String key : keys) {
        data.add(valueObjs.get(key));
      }
      return new ResponseEntity<RegionData<?>>(data, headers, HttpStatus.OK);
    }
  }

  /**
   * Update data for a key or set of keys
   *
   * @param region gemfire data region
   * @param keys comma seperated list of keys
   * @param opValue type of update (put, replace, cas etc)
   * @param json new data for the key(s)
   * @return JSON document
   */
  @RequestMapping(method = RequestMethod.PUT, value = "/{region}/{keys}",
      consumes = {APPLICATION_JSON_UTF8_VALUE}, produces = {
          APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "update data for key",
      notes = "Update or insert (put) data for keys in a region."
          + " Deprecated in favor of /{region}?keys=."
          + " If op=REPLACE, update (replace) data with key if and only if the key exists in the region."
          + " If op=CAS update (compare-and-set) value having key with a new value if and only if the \"@old\" value sent matches the current value for the key in the region.")
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
      @PathVariable("keys") String[] keys,
      @RequestParam(value = "op", defaultValue = "PUT") final String opValue,
      @RequestBody final String json, HttpServletRequest request) {
    logger.debug("updating key(s) for region ({}) ", region);

    region = decode(region);
    if (!validOp(opValue)) {
      String errorMessage = String.format(
          "The op parameter (%1$s) is not valid. Valid values are PUT, REPLACE, or CAS.",
          opValue);
      return new ResponseEntity<>(convertErrorAsJson(errorMessage), HttpStatus.BAD_REQUEST);
    }
    if (keys.length > 1) {
      updateMultipleKeys(region, keys, json);
      HttpHeaders headers = new HttpHeaders();
      headers.setLocation(toUri(region, StringUtils.arrayToCommaDelimitedString(keys)));
      return new ResponseEntity<>(headers, HttpStatus.OK);
    } else {
      // put case
      Object existingValue = updateSingleKey(region, keys[0], json, opValue);
      final HttpHeaders headers = new HttpHeaders();
      headers.setLocation(toUri(region, keys[0]));
      return new ResponseEntity<>(existingValue, headers,
          (existingValue == null ? HttpStatus.OK : HttpStatus.CONFLICT));
    }
  }

  private boolean validOp(String opValue) {
    try {
      UpdateOp.valueOf(opValue.toUpperCase());
      return true;
    } catch (IllegalArgumentException ex) {
      return false;
    }
  }

  /**
   * Update data for a key or set of keys
   *
   * @param encodedRegion gemfire data region
   * @param encodedKeys comma separated list of keys
   * @param opValue type of update (put, replace, cas etc)
   * @param json new data for the key(s)
   * @return JSON document
   */
  @RequestMapping(method = RequestMethod.PUT, value = "/{region}",
      consumes = {APPLICATION_JSON_UTF8_VALUE}, produces = {
          APPLICATION_JSON_UTF8_VALUE})
  @ApiOperation(value = "update data for key(s)",
      notes = "Update or insert (put) data for keys in a region."
          + " The keys are a comma separated list."
          + " If multiple keys are given then put (create or update) the data for each key."
          + " The op parameter is ignored if more than one key is given."
          + " If op=PUT, the default, create or update data for the given key."
          + " If op=CREATE, create data for the given key if and only if the key does not exit in the region."
          + " If op=REPLACE, update (replace) data for the given key if and only if the key exists in the region."
          + " If op=CAS, update (compare-and-set) value having key with a new value if and only if the \"@old\" value sent matches the current value for the key in the region.")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 201, message = "For op=CREATE on success."),
      @ApiResponse(code = 400, message = "Bad Request."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404,
          message = "Region does not exist or if key is not mapped to some value for REPLACE or CAS."),
      @ApiResponse(code = 409,
          message = "For op=CREATE, key already exist in region. For op=CAS, @old value does not match to the current value in region."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  public ResponseEntity<?> updateKeys(@PathVariable("region") final String encodedRegion,
      @RequestParam(value = "keys") final String[] encodedKeys,
      @RequestParam(value = "op", defaultValue = "PUT") final String opValue,
      @RequestBody final String json) {

    String decodedRegion = decode(encodedRegion);
    String[] decodedKeys = decode(encodedKeys);
    if (!validOp(opValue) && !opValue.equalsIgnoreCase("CREATE")) {
      String errorMessage = String.format(
          "The op parameter (%1$s) is not valid. Valid values are PUT, CREATE, REPLACE, or CAS.",
          opValue);
      return new ResponseEntity<>(convertErrorAsJson(errorMessage), HttpStatus.BAD_REQUEST);
    }

    if (decodedKeys.length > 1) {
      // putAll case
      logger.debug("updating keys ({}) for region ({}) op={}", decodedKeys, decodedRegion, opValue);
      securityService.authorize("WRITE", decodedRegion, decodedKeys);
      updateMultipleKeys(decodedRegion, decodedKeys, json);
      HttpHeaders headers = new HttpHeaders();
      headers.setLocation(toUriWithKeys(encodedKeys, encodedRegion));
      return new ResponseEntity<>(headers, HttpStatus.OK);
    } else if (opValue.equalsIgnoreCase("CREATE")) {
      securityService.authorize("DATA", "WRITE", decodedRegion);
      return create(decodedRegion, decodedKeys[0], json, true);
    } else {
      // put case
      logger.debug("updating keys ({}) for region ({}) op={}", decodedKeys, decodedRegion, opValue);
      securityService.authorize("WRITE", decodedRegion, decodedKeys);
      Object existingValue = updateSingleKey(decodedRegion, decodedKeys[0], json, opValue);
      final HttpHeaders headers = new HttpHeaders();
      headers.setLocation(toUriWithKeys(encodedKeys, encodedRegion));
      return new ResponseEntity<>(existingValue, headers,
          (existingValue == null ? HttpStatus.OK : HttpStatus.CONFLICT));
    }
  }

  @RequestMapping(method = RequestMethod.HEAD, value = "/{region}",
      produces = APPLICATION_JSON_UTF8_VALUE)
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
