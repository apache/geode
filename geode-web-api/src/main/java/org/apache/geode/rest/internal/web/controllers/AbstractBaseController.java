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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import org.apache.geode.SerializationException;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.LeaseExpiredException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.JSONFormatterException;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.rest.internal.web.controllers.support.CacheProvider;
import org.apache.geode.rest.internal.web.controllers.support.JSONTypes;
import org.apache.geode.rest.internal.web.controllers.support.UpdateOp;
import org.apache.geode.rest.internal.web.exception.DataTypeNotSupportedException;
import org.apache.geode.rest.internal.web.exception.GemfireRestException;
import org.apache.geode.rest.internal.web.exception.MalformedJsonException;
import org.apache.geode.rest.internal.web.exception.RegionNotFoundException;
import org.apache.geode.rest.internal.web.exception.ResourceNotFoundException;
import org.apache.geode.rest.internal.web.security.RestSecurityService;
import org.apache.geode.rest.internal.web.util.ArrayUtils;
import org.apache.geode.rest.internal.web.util.IdentifiableUtils;
import org.apache.geode.rest.internal.web.util.JSONUtils;
import org.apache.geode.rest.internal.web.util.NumberUtils;
import org.apache.geode.rest.internal.web.util.ValidationUtils;
import org.apache.geode.util.internal.GeodeConverter;

/**
 * AbstractBaseController class contains common functionalities required for other controllers.
 *
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public abstract class AbstractBaseController implements InitializingBean {

  private static final String NEW_META_DATA_PROPERTY = "@new";
  private static final String OLD_META_DATA_PROPERTY = "@old";
  private static final String TYPE_META_DATA_PROPERTY = "@type";
  private static final String UTF_8 = "UTF-8";
  private static final String DEFAULT_ENCODING = UTF_8;
  private static final Logger logger = LogService.getLogger();
  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0L);

  @SuppressWarnings("deprecation")
  protected static final String APPLICATION_JSON_UTF8_VALUE = MediaType.APPLICATION_JSON_UTF8_VALUE;
  @SuppressWarnings("deprecation")
  protected static final MediaType APPLICATION_JSON_UTF8 = MediaType.APPLICATION_JSON_UTF8;

  @Autowired
  protected RestSecurityService securityService;
  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private CacheProvider cacheProvider;

  @Override
  public void afterPropertiesSet() {
    JSONUtils.setObjectMapper(objectMapper);
  }

  protected InternalCacheForClientAccess getCache() {
    InternalCacheForClientAccess cache = cacheProvider.getCache();
    Assert.state(cache != null, "The Gemfire Cache reference was not properly initialized");
    return cache;
  }

  URI toUri(final String... pathSegments) {
    return ServletUriComponentsBuilder.fromCurrentContextPath().path(getRestApiVersion())
        .pathSegment(pathSegments).build().toUri();
  }

  URI toUriWithKeys(String[] keys, final String... pathSegments) {
    return ServletUriComponentsBuilder.fromCurrentContextPath().path(getRestApiVersion())
        .pathSegment(pathSegments)
        .queryParam("keys", StringUtils.arrayToCommaDelimitedString(keys))
        .build(true).toUri();
  }

  protected abstract String getRestApiVersion();

  String validateQuery(String queryInUrl, String queryInBody) {

    if (!(StringUtils.hasText(queryInUrl) || StringUtils.hasText(queryInBody))) {
      throw new GemfireRestException("could not process null value specified in query String");
    }
    return (StringUtils.hasText(queryInUrl) ? decode(queryInUrl) : queryInBody);
  }

  String encode(String value) {
    if (value == null) {
      throw new GemfireRestException("could not process null value specified in query String");
    }
    return encode(value, DEFAULT_ENCODING);
  }

  String decode(final String value) {
    if (value == null) {
      throw new GemfireRestException("could not process null value specified in query String");
    }

    return decode(value, DEFAULT_ENCODING);
  }

  String[] decode(String[] values) {
    String[] result = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = decode(values[i]);
    }
    return result;
  }

  String[] encode(String[] values) {
    String[] result = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = encode(values[i]);
    }
    return result;
  }

  protected PdxInstance convert(final String json) {
    try {
      return (StringUtils.hasText(json) ? JSONFormatter.fromJSON(json) : null);
    } catch (JSONFormatterException jpe) {
      throw new MalformedJsonException("Json doc specified is either not supported or invalid!",
          jpe);
    }
  }

  protected String convert(final PdxInstance pdxObj) {
    try {
      return (pdxObj != null ? JSONFormatter.toJSON(pdxObj) : null);
    } catch (JSONFormatterException jpe) {
      throw new GemfireRestException("Requested data could not convert into REST format(JSON)!",
          jpe);
    }
  }

  protected String convert(final Iterable<PdxInstance> pdxObjs) {
    final StringBuilder buffer = new StringBuilder("[");
    int count = 0;

    for (final PdxInstance pdxObj : pdxObjs) {
      final String json = convert(pdxObj);

      if (StringUtils.hasText(json)) {
        buffer.append(count++ > 0 ? ", " : "").append(json);
      }
    }

    buffer.append("]");

    return buffer.toString();
  }

  @SuppressWarnings("unchecked")
  private <T> T casValue(String regionNamePath, String key, String jsonData) {
    try {
      JsonNode jsonObject = objectMapper.readTree(jsonData);
      JsonNode oldValue = jsonObject.get("@old");
      JsonNode newValue = jsonObject.get("@new");

      if (oldValue == null || newValue == null) {
        throw new MalformedJsonException("Json doc specified in request body is invalid!");
      }

      return (T) casValue(regionNamePath, key, convert(oldValue.toString()),
          convert(newValue.toString()));

    } catch (IOException je) {
      throw new MalformedJsonException("Json doc specified in request body is invalid!", je);
    }
  }

  ResponseEntity<String> processQueryResponse(Query query, Object[] args, Object queryResult) {
    if (queryResult instanceof Collection) {
      @SuppressWarnings("unchecked")
      final Collection<Object> queryResultCollection = (Collection<Object>) queryResult;
      Collection<Object> processedResults = new ArrayList<>(queryResultCollection.size());
      for (Object result : queryResultCollection) {
        processedResults.add(securityService.postProcess(null, null, result, false));
      }
      String queryResultAsJson = JSONUtils.convertCollectionToJson(processedResults);

      final HttpHeaders headers = new HttpHeaders();
      headers.setLocation(toUri("queries", query.getQueryString()));
      return new ResponseEntity<>(queryResultAsJson, headers, HttpStatus.OK);
    } else {
      throw new GemfireRestException(
          "Server has encountered error while generating query result into restful format(JSON)!");
    }
  }

  Collection<PdxInstance> convertJsonArrayIntoPdxCollection(final String jsonArray) {
    try {
      JsonNode array = objectMapper.readTree(jsonArray);
      if (!array.isArray()) {
        throw new MalformedJsonException(
            "Json document specified in request body is not an array!");
      }

      Collection<PdxInstance> pdxInstances = new ArrayList<>();

      for (int index = 0; index < array.size(); index++) {
        JsonNode object = array.get(index);
        String element = objectMapper.writeValueAsString(object);

        PdxInstance pi = convert(element);
        pdxInstances.add(pi);
      }
      return pdxInstances;

    } catch (IOException je) {
      throw new MalformedJsonException("Json document specified in request body is not valid!", je);
    }
  }

  private Object casValue(final String regionNamePath, final Object key, final Object oldValue,
      final Object newValue) {
    final Region<Object, Object> region = getRegion(regionNamePath);
    try {
      return (region.replace(key, oldValue, newValue) ? null : region.get(key));
    } catch (UnsupportedOperationException use) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not support the requested operation!",
              regionNamePath),
          use);
    } catch (ClassCastException cce) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration does not allow to store specified key or value type in this region!",
          regionNamePath), cce);
    } catch (NullPointerException npe) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not allow null keys or values!",
              regionNamePath),
          npe);
    } catch (IllegalArgumentException iae) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration prevents specified data from being stored in it!",
          regionNamePath), iae);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", toe);
    } catch (CacheWriterException cwe) {
      throw new GemfireRestException(
          "Server has encountered CacheWriter error while processing this request!", cwe);
    } catch (PartitionedRegionStorageException prse) {
      throw new GemfireRestException(
          "Requested operation could not be completed on a partitioned region!", prse);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException(
          "Server has detected low memory while processing this request!", lme);
    }
  }

  private void replaceValue(final String regionNamePath, final Object key,
      final PdxInstance value) {
    try {
      if (getRegion(regionNamePath).replace(key, value) == null) {
        throw new ResourceNotFoundException(String.format("No resource at (%1$s) exists!",
            toUri(regionNamePath, String.valueOf(key))));
      }
    } catch (UnsupportedOperationException use) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not support the requested operation!",
              regionNamePath),
          use);
    } catch (ClassCastException cce) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration does not allow to store specified key or value type in this region!",
          regionNamePath), cce);
    } catch (NullPointerException npe) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not allow null keys or values!",
              regionNamePath),
          npe);
    } catch (IllegalArgumentException iae) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration prevents specified data from being stored in it!",
          regionNamePath), iae);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", toe);
    } catch (CacheWriterException cwe) {
      throw new GemfireRestException(
          "Server has encountered CacheWriter error while processing this request!", cwe);
    } catch (PartitionedRegionStorageException prse) {
      throw new GemfireRestException(
          "Requested operation could not be completed on a partitioned region!", prse);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException(
          "Server has detected low memory while processing this request!", lme);
    }
  }

  protected void replaceValue(final String regionNamePath, final Object key, final Object value) {
    // still do we need to worry for race condition..?
    try {
      if (getRegion(regionNamePath).replace(key, value) == null) {
        throw new ResourceNotFoundException(String.format("No resource at (%1$s) exists!",
            toUri(regionNamePath, String.valueOf(key))));
      }
    } catch (UnsupportedOperationException use) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not support the requested operation!",
              regionNamePath),
          use);
    } catch (ClassCastException cce) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration does not allow to store specified key or value type in this region!",
          regionNamePath), cce);
    } catch (NullPointerException npe) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not allow null keys or values!",
              regionNamePath),
          npe);
    } catch (IllegalArgumentException iae) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration prevents specified data from being stored in it!",
          regionNamePath), iae);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", toe);
    } catch (CacheWriterException cwe) {
      throw new GemfireRestException(
          "Server has encountered CacheWriter error while processing this request!", cwe);
    } catch (PartitionedRegionStorageException prse) {
      throw new GemfireRestException(
          "Requested operation could not be completed on a partitioned region!", prse);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException(
          "Server has detected low memory while processing this request!", lme);
    }
  }

  private void putValue(final String regionNamePath, final Object key, final Object value) {
    try {
      getRegion(regionNamePath).put(key, value);
    } catch (NullPointerException npe) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not allow null keys or values!",
              regionNamePath),
          npe);
    } catch (ClassCastException cce) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration does not allow to store specified key or value type in this region!",
          regionNamePath), cce);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", toe);
    } catch (CacheWriterException cwe) {
      throw new GemfireRestException(
          "Server has encountered CacheWriter error while processing this request!", cwe);
    } catch (PartitionedRegionStorageException prse) {
      throw new GemfireRestException(
          "Requested operation could not be completed on a partitioned region!", prse);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException(
          "Server has detected low memory while processing this request!", lme);
    }
  }

  private void deleteQueryId(final String regionNamePath, final String key) {
    getQueryStore(regionNamePath).remove(key);
  }

  void deleteNamedQuery(final String regionNamePath, final String key) {
    // Check whether query ID exist in region or not
    checkForQueryIdExist(regionNamePath, key);
    deleteQueryId(regionNamePath, key);
  }

  void checkForQueryIdExist(String region, String key) {
    if (!getQueryStore(region).containsKey(key)) {
      throw new ResourceNotFoundException(String.format("Named query (%1$s) does not exist!", key));
    }
  }

  Region<String, String> getQueryStore(final String namePath) {
    return ValidationUtils.returnValueThrowOnNull(getCache().getInternalRegion(namePath),
        new GemfireRestException(String.format("Query store (%1$s) does not exist!", namePath)));
  }

  protected String getQueryIdValue(final String regionNamePath, final String key) {
    Assert.notNull(key, "queryId cannot be null!");
    try {
      return getQueryStore(regionNamePath).get(key);
    } catch (NullPointerException npe) {
      throw new GemfireRestException("NULL query ID or query string is not supported!", npe);
    } catch (IllegalArgumentException iae) {
      throw new GemfireRestException("Server has not allowed to perform the requested operation!",
          iae);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException te) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", te);
    }
  }

  void updateNamedQuery(final String regionNamePath, final String key, final String value) {
    try {
      getQueryStore(regionNamePath).put(key, value);
    } catch (NullPointerException npe) {
      throw new GemfireRestException("NULL query ID or query string is not supported!", npe);
    } catch (ClassCastException cce) {
      throw new GemfireRestException("specified queryId or query string is not supported!", cce);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", toe);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException(
          "Server has detected low memory while processing this request!", lme);
    }
  }

  @SuppressWarnings("unchecked")
  <T> T createNamedQuery(final String regionNamePath, final String key, final String value) {
    try {
      return (T) getQueryStore(regionNamePath).putIfAbsent(key, value);
    } catch (UnsupportedOperationException use) {
      throw new GemfireRestException("Requested operation is not supported!", use);
    } catch (ClassCastException cce) {
      throw new GemfireRestException("Specified queryId or query string is not supported!", cce);
    } catch (NullPointerException npe) {
      throw new GemfireRestException("NULL query ID or query string is not supported!", npe);
    } catch (IllegalArgumentException iae) {
      throw new GemfireRestException(
          "Configuration does not allow to perform the requested operation!", iae);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", toe);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException(
          "Server has detected low memory while processing this request!", lme);
    }
  }

  private void putPdxValues(final String regionNamePath, final Map<Object, PdxInstance> map) {
    try {
      getRegion(regionNamePath).putAll(map);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException("low memory condition is detected.", lme);
    }
  }

  private void putValues(final String regionNamePath, final Map<Object, Object> values) {
    getRegion(regionNamePath).putAll(values);
  }

  protected void putValues(final String regionNamePath, String[] keys, List<?> values) {
    Map<Object, Object> map = new HashMap<>();
    if (keys.length != values.size()) {
      throw new GemfireRestException("Bad request, Keys and Value size does not match");
    }
    for (int i = 0; i < keys.length; i++) {
      Object domainObj = introspectAndConvert(values.get(i));
      map.put(keys[i], domainObj);
    }

    if (!map.isEmpty()) {
      putValues(regionNamePath, map);
    }
  }

  @SuppressWarnings("unchecked")
  <T> T postValue(final String regionNamePath, final Object key, final Object value) {
    try {
      return (T) getRegion(regionNamePath).putIfAbsent(key, value);
    } catch (UnsupportedOperationException use) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not support the requested operation!",
              regionNamePath),
          use);
    } catch (ClassCastException cce) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration does not allow to store specified key or value type in this region!",
          regionNamePath), cce);
    } catch (NullPointerException npe) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not allow null keys or values!",
              regionNamePath),
          npe);
    } catch (IllegalArgumentException iae) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration prevents specified data from being stored in it!",
          regionNamePath), iae);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", toe);
    } catch (CacheWriterException cwe) {
      throw new GemfireRestException(
          "Server has encountered CacheWriter error while processing this request!", cwe);
    } catch (PartitionedRegionStorageException prse) {
      throw new GemfireRestException(
          "Requested operation could not be completed on a partitioned region!", prse);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException(
          "Server has detected low memory while processing this request!", lme);
    }
  }

  protected Object getActualTypeValue(final String value, final String valueType) {
    Object actualValue = value;
    if (valueType != null) {
      try {
        actualValue = GeodeConverter.convertToActualType(value, valueType);
      } catch (IllegalArgumentException ie) {
        throw new GemfireRestException(ie.getMessage(), ie);
      }
    }
    return actualValue;
  }

  String generateKey(final String existingKey) {
    return generateKey(existingKey, null);
  }

  private String generateKey(final String existingKey, final Object domainObject) {
    Object domainObjectId = IdentifiableUtils.getId(domainObject);
    String newKey;

    if (StringUtils.hasText(existingKey)) {
      newKey = existingKey;
      if (domainObject != null && NumberUtils.isNumeric(newKey) && domainObjectId == null) {
        final Long newId = IdentifiableUtils.createId(NumberUtils.parseLong(newKey));
        if (newKey.equals(newId.toString())) {
          IdentifiableUtils.setId(domainObject, newId);
        }
      }
    } else if (domainObjectId != null) {
      final Long domainObjectIdAsLong = NumberUtils.longValue(domainObjectId);
      if (domainObjectIdAsLong != null) {
        final Long newId = IdentifiableUtils.createId(domainObjectIdAsLong);
        if (!domainObjectIdAsLong.equals(newId)) {
          IdentifiableUtils.setId(domainObject, newId);
        }
        newKey = String.valueOf(newId);
      } else {
        newKey = String.valueOf(domainObjectId);
      }
    } else {
      domainObjectId = IdentifiableUtils.createId();
      newKey = String.valueOf(domainObjectId);
      IdentifiableUtils.setId(domainObject, domainObjectId);
    }

    return newKey;
  }

  private String encode(final String value, final String encoding) {
    try {
      return URLEncoder.encode(value, encoding);
    } catch (UnsupportedEncodingException e) {
      throw new GemfireRestException("Server has encountered unsupported encoding!");
    }
  }

  private String decode(final String value, final String encoding) {
    try {
      return URLDecoder.decode(value, encoding);
    } catch (UnsupportedEncodingException e) {
      throw new GemfireRestException("Server has encountered unsupported encoding!");
    }
  }

  @SuppressWarnings("unchecked")
  protected <T> Region<Object, T> getRegion(final String namePath) {
    return (Region<Object, T>) ValidationUtils
        .returnValueThrowOnNull(getCache().getRegion(namePath), new RegionNotFoundException(
            String.format("The Region identified by name (%1$s) could not be found!", namePath)));
  }

  private void checkForKeyExist(String region, String key) {
    if (!getRegion(region).containsKey(key)) {
      throw new ResourceNotFoundException(
          String.format("Key (%1$s) does not exist for region (%2$s) in cache!", key, region));
    }
  }

  protected Object[] getKeys(final String regionNamePath, Object[] keys) {
    return (!(keys == null || keys.length == 0) ? keys
        : getRegion(regionNamePath).keySet().toArray());
  }

  protected <T> Map<Object, T> getValues(final String regionNamePath, Object... keys) {
    try {
      final Region<Object, T> region = getRegion(regionNamePath);
      final Map<Object, T> entries = region.getAll(Arrays.asList(getKeys(regionNamePath, keys)));
      for (Object key : entries.keySet()) {
        @SuppressWarnings("unchecked")
        final T value =
            (T) securityService.postProcess(regionNamePath, key, entries.get(key), false);
        entries.put(key, value);
      }
      return entries;
    } catch (SerializationException se) {
      throw new DataTypeNotSupportedException(
          "The resource identified could not convert into the supported content characteristics (JSON)!",
          se);
    }
  }

  protected <T> Map<Object, T> getValues(final String regionNamePath, String... keys) {
    return getValues(regionNamePath, (Object[]) keys);
  }

  protected <T extends PdxInstance> Collection<T> getPdxValues(final String regionNamePath,
      final Object... keys) {
    final Region<Object, T> region = getRegion(regionNamePath);
    final Map<Object, T> entries = region.getAll(Arrays.asList(getKeys(regionNamePath, keys)));

    return entries.values();
  }

  private void deleteValue(final String regionNamePath, final Object key) {
    getRegion(regionNamePath).remove(key);
  }

  @SafeVarargs
  final <T> void deleteValues(final String regionNamePath, final T... keys) {
    // Check whether all keys exist in cache or not
    for (final T key : keys) {
      checkForKeyExist(regionNamePath, key.toString());
    }

    for (final T key : keys) {
      deleteValue(regionNamePath, key);
    }
  }

  void deleteValues(String regionNamePath) {
    try {
      getRegion(regionNamePath).clear();
    } catch (UnsupportedOperationException ue) {
      throw new GemfireRestException("Requested operation not allowed on partition region", ue);
    }
  }

  private boolean isForm(final Map<Object, Object> rawDataBinding) {
    return (!rawDataBinding.containsKey(TYPE_META_DATA_PROPERTY)
        && rawDataBinding.containsKey(OLD_META_DATA_PROPERTY)
        && rawDataBinding.containsKey(NEW_META_DATA_PROPERTY));
  }

  @SuppressWarnings("unchecked")
  private <T> T introspectAndConvert(final T value) {
    if (value instanceof Map) {
      final Map<Object, Object> rawDataBinding = (Map<Object, Object>) value;

      if (isForm(rawDataBinding)) {
        rawDataBinding.put(OLD_META_DATA_PROPERTY,
            introspectAndConvert(rawDataBinding.get(OLD_META_DATA_PROPERTY)));
        rawDataBinding.put(NEW_META_DATA_PROPERTY,
            introspectAndConvert(rawDataBinding.get(NEW_META_DATA_PROPERTY)));

        return (T) rawDataBinding;
      } else {
        final String typeValue = (String) rawDataBinding.get(TYPE_META_DATA_PROPERTY);
        if (typeValue == null) {
          return (T) new Object();
        }
        // Added for the primitive types put. Not supporting primitive types
        if (NumberUtils.isPrimitiveOrObject(typeValue)) {
          final Object primitiveValue = rawDataBinding.get("@value");
          try {
            return (T) GeodeConverter.convertToActualType(primitiveValue.toString(), typeValue);
          } catch (IllegalArgumentException e) {
            throw new GemfireRestException(
                "Server has encountered error (illegal or inappropriate arguments).", e);
          }
        } else {
          Assert.state(
              ClassUtils.isPresent(typeValue,
                  Thread.currentThread().getContextClassLoader()),
              String.format("Class (%1$s) could not be found!", typeValue));

          return (T) objectMapper.convertValue(rawDataBinding, ClassUtils.resolveClassName(
              typeValue, Thread.currentThread().getContextClassLoader()));
        }
      }
    }

    return value;
  }

  String convertErrorAsJson(String errorMessage) {
    return ("{" + "\"cause\"" + ":" + "\"" + errorMessage + "\"" + "}");
  }

  String convertErrorAsJson(Throwable t) {
    return String.format("{\"message\" : \"%1$s\"}", t.getMessage());
  }

  private Map<?, ?> convertJsonToMap(final String jsonString) {
    Map<String, String> map;

    // convert JSON string to Map
    try {
      map = objectMapper.readValue(jsonString, new TypeReference<HashMap<String, String>>() {});
    } catch (JsonParseException e) {
      throw new MalformedJsonException(
          "Bind params specified as JSON document in the request is incorrect!", e);
    } catch (JsonMappingException e) {
      throw new MalformedJsonException(
          "Server unable to process bind params specified as JSON document in the request!", e);
    } catch (IOException e) {
      throw new GemfireRestException("Server has encountered error while process this request!", e);
    }
    return map;
  }

  private Object jsonToObject(final String jsonString) {
    return introspectAndConvert(convertJsonToMap(jsonString));
  }

  Object[] jsonToObjectArray(final String arguments) {
    JsonNode node;
    try {
      node = objectMapper.readTree(arguments);
    } catch (IOException e) {
      throw new MalformedJsonException("Json document specified in request body is not valid!");
    }

    if (node.isArray()) {
      try {
        Object[] args = new Object[node.size()];
        for (int index = 0; index < node.size(); index++) {
          args[index] = jsonToObject(objectMapper.writeValueAsString(node.get(index)));
        }
        return args;
      } catch (JsonProcessingException je) {
        throw new MalformedJsonException("Json document specified in request body is not valid!",
            je);
      }
    } else if (node.isObject()) {
      return new Object[] {jsonToObject(arguments)};
    } else {
      throw new MalformedJsonException("Json document specified in request body is not valid!");
    }
  }

  /**
   * @return if the opValue is CAS then the existingValue; otherwise null
   */
  String updateSingleKey(final String region, final String key, final String json,
      final String opValue) {

    final JSONTypes jsonType = validateJsonAndFindType(json);

    final UpdateOp op = UpdateOp.valueOf(opValue.toUpperCase());
    String existingValue = null;

    switch (op) {
      case CAS:
        PdxInstance existingPdxObj = casValue(region, key, json);
        existingValue = convert(existingPdxObj);
        break;

      case REPLACE:
        replaceValue(region, key, convert(json));
        break;

      case PUT:
      default:
        if (JSONTypes.JSON_ARRAY.equals(jsonType)) {
          putValue(region, key, convertJsonArrayIntoPdxCollection(json));
        } else {
          putValue(region, key, convert(json));
        }
    }
    return existingValue;
  }


  void updateMultipleKeys(final String region, final String[] keys,
      final String json) {

    JsonNode jsonArr;
    try {
      jsonArr = objectMapper.readTree(json);
    } catch (IOException e) {
      throw new MalformedJsonException("JSON document specified in the request is incorrect", e);
    }

    if (!jsonArr.isArray() || jsonArr.size() != keys.length) {
      throw new MalformedJsonException(
          "Each key must have corresponding value (JSON document) specified in the request");
    }

    Map<Object, PdxInstance> map = new HashMap<>();
    for (int i = 0; i < keys.length; i++) {
      if (logger.isDebugEnabled()) {
        logger.debug("Updating (put) Json document ({}) having key ({}) in Region ({})", json,
            keys[i], region);
      }

      try {
        PdxInstance pdxObj = convert(objectMapper.writeValueAsString(jsonArr.get(i)));
        map.put(keys[i], pdxObj);
      } catch (JsonProcessingException e) {
        throw new MalformedJsonException(
            String.format("JSON document at index (%1$s) in the request body is incorrect", i), e);
      }
    }

    if (!CollectionUtils.isEmpty(map)) {
      putPdxValues(region, map);
    }
  }

  JSONTypes validateJsonAndFindType(String json) {
    try {
      JsonParser jp = new JsonFactory().createParser(json);
      JsonToken token = jp.nextToken();

      if (token == JsonToken.START_OBJECT) {
        return JSONTypes.JSON_OBJECT;
      } else if (token == JsonToken.START_ARRAY) {
        return JSONTypes.JSON_ARRAY;
      } else {
        return JSONTypes.UNRECOGNIZED_JSON;
      }
    } catch (IOException je) {
      throw new MalformedJsonException("JSON document specified in the request is incorrect", je);
    }
  }

  protected QueryService getQueryService() {
    return getCache().getQueryService();
  }

  protected <T> T getValue(final String regionNamePath, final Object key) {
    return getValue(regionNamePath, key, true);
  }

  protected <T> T getValue(final String regionNamePath, final Object key, boolean postProcess) {
    Assert.notNull(key, "The Cache Region key to read the value for cannot be null!");

    Region<?, ?> r = getRegion(regionNamePath);
    try {
      Object value = r.get(key);
      if (postProcess) {
        @SuppressWarnings("unchecked")
        final T v = (T) securityService.postProcess(regionNamePath, key, value, false);
        return v;
      } else {
        @SuppressWarnings("unchecked")
        final T v = (T) value;
        return v;
      }
    } catch (SerializationException se) {
      throw new DataTypeNotSupportedException(
          "The resource identified could not convert into the supported content characteristics (JSON)!",
          se);
    } catch (NullPointerException npe) {
      throw new GemfireRestException(
          String.format("Resource (%1$s) configuration does not allow null keys!", regionNamePath),
          npe);
    } catch (IllegalArgumentException iae) {
      throw new GemfireRestException(String.format(
          "Resource (%1$s) configuration does not allow requested operation on specified key!",
          regionNamePath), iae);
    } catch (LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!",
          lee);
    } catch (TimeoutException te) {
      throw new GemfireRestException(
          "Server has encountered timeout error while processing this request!", te);
    } catch (CacheLoaderException cle) {
      throw new GemfireRestException(
          "Server has encountered CacheLoader error while processing this request!", cle);
    } catch (PartitionedRegionStorageException prse) {
      throw new GemfireRestException("CacheLoader could not be invoked on partitioned region!",
          prse);
    }

  }

  protected Set<DistributedMember> getMembers(final String... memberIdNames) {
    ValidationUtils.returnValueThrowOnNull(memberIdNames,
        new GemfireRestException("No member found to run function"));
    final Set<DistributedMember> targetedMembers = new HashSet<>(ArrayUtils.length(memberIdNames));
    final List<String> memberIdNameList = Arrays.asList(memberIdNames);

    InternalCache cache = getCache();
    Set<DistributedMember> distMembers = cache.getDistributedSystem().getAllOtherMembers();

    // Add the local node to list
    distMembers.add(cache.getDistributedSystem().getDistributedMember());
    for (DistributedMember member : distMembers) {
      if (memberIdNameList.contains(member.getId())
          || memberIdNameList.contains(member.getName())) {
        targetedMembers.add(member);
      }
    }
    return targetedMembers;
  }

  Set<DistributedMember> getAllMembersInDS() {
    InternalCache cache = getCache();
    Set<DistributedMember> distMembers = cache.getDistributedSystem().getAllOtherMembers();
    final Set<DistributedMember> targetedMembers = new HashSet<>();

    // find valid data nodes, i.e non locator, non-admin, non-loner nodes
    for (DistributedMember member : distMembers) {
      InternalDistributedMember idm = (InternalDistributedMember) member;
      if (idm.getVmKind() == ClusterDistributionManager.NORMAL_DM_TYPE) {
        targetedMembers.add(member);
      }
    }
    // Add the local node to list
    targetedMembers.add(cache.getDistributedSystem().getDistributedMember());
    return targetedMembers;
  }
}
