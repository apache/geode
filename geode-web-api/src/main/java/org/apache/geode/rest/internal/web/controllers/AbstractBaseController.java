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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.JSONFormatterException;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.rest.internal.web.controllers.support.JSONTypes;
import org.apache.geode.rest.internal.web.controllers.support.UpdateOp;
import org.apache.geode.rest.internal.web.exception.DataTypeNotSupportedException;
import org.apache.geode.rest.internal.web.exception.GemfireRestException;
import org.apache.geode.rest.internal.web.exception.MalformedJsonException;
import org.apache.geode.rest.internal.web.exception.RegionNotFoundException;
import org.apache.geode.rest.internal.web.exception.ResourceNotFoundException;
import org.apache.geode.rest.internal.web.util.ArrayUtils;
import org.apache.geode.rest.internal.web.util.IdentifiableUtils;
import org.apache.geode.rest.internal.web.util.JSONUtils;
import org.apache.geode.rest.internal.web.util.NumberUtils;
import org.apache.geode.rest.internal.web.util.ValidationUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * AbstractBaseController class contains common functionalities required for other controllers. 
 * @since GemFire 8.0
 */

@SuppressWarnings("unused")
public abstract class AbstractBaseController {
  
  private static final Logger logger = LogService.getLogger();
  
  protected static final String NEW_META_DATA_PROPERTY = "@new";
  protected static final String OLD_META_DATA_PROPERTY = "@old";
  protected static final String TYPE_META_DATA_PROPERTY = "@type";
  
  protected static final String UTF_8 = "UTF-8";
  protected static final String DEFAULT_ENCODING = UTF_8;
  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0l);
  
  //private Cache cache = GemFireCacheImpl.getExisting(null);
  
  @Autowired
  private ObjectMapper objectMapper;

  @PostConstruct
  private void init() {
    JSONUtils.setObjectMapper(objectMapper);
  }

  protected Cache getCache(){
    Cache cache = GemFireCacheImpl.getExisting();
    Assert.state(cache != null, "The Gemfire Cache reference was not properly initialized");
    return cache;
  }
  
  protected URI toUri(final String... pathSegments) {
    return ServletUriComponentsBuilder.fromCurrentContextPath().path(getRestApiVersion()).pathSegment(pathSegments)
      .build().toUri();
  }
  
  protected abstract String getRestApiVersion();
  
  protected String validateQuery(String queryInUrl, String queryInBody){
    
    if (!(StringUtils.hasText(queryInUrl) || StringUtils.hasText(queryInBody))) {
      throw new GemfireRestException("could not process null value specified in query String");
    }
    return (StringUtils.hasText(queryInUrl) ? decode(queryInUrl) : queryInBody);
  }
  
  protected String decode(final String value) {
    if(value == null) {
      throw new GemfireRestException("could not process null value specified in query String"); 
    }
    
    return decode(value, DEFAULT_ENCODING);
  }

  protected PdxInstance convert(final String json){
    try{
      return (StringUtils.hasText(json) ? JSONFormatter.fromJSON(json) : null);
    }catch(JSONFormatterException jpe){
      throw new MalformedJsonException("Json doc specified is either not supported or invalid!", jpe);
    }
  }
  
  protected String convert(final PdxInstance pdxObj){
    try {
      return (pdxObj != null ? JSONFormatter.toJSON(pdxObj) : null);
    } catch(JSONFormatterException jpe) {
      throw new GemfireRestException("Requested data could not convert into REST format(JSON)!", jpe);
    }
  }
  
  protected String convert(final Iterable<PdxInstance> pdxObjs){
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
  protected <T> T casValue(String regionNamePath, String key, String jsonData){
    JSONObject jsonObject;
    try {
      jsonObject = new JSONObject(jsonData);
      String oldValue = jsonObject.get("@old").toString();
      String newValue = jsonObject.get("@new").toString();
  
      return (T) casValue(regionNamePath, key, convert(oldValue), convert(newValue));
      
    } catch (JSONException je) {
      throw new MalformedJsonException("Json doc specified in request body is invalid!", je);
    }              
  }
  
  public ResponseEntity<String> processQueryResponse (Object  queryResult, String queryId) throws JSONException {
    if(queryResult instanceof Collection<?>){
      Collection<Object> result = (Collection<Object>) queryResult;
      String queryResultAsJson =  JSONUtils.convertCollectionToJson(result);
      
      final HttpHeaders headers = new HttpHeaders();
      headers.setLocation(toUri("queries", queryId));    
      return new ResponseEntity<>(queryResultAsJson, headers, HttpStatus.OK);
    }else {
      throw new GemfireRestException("Server has encountered error while generating query result into restful format(JSON)!");
    }
  }
  
  protected Collection<PdxInstance> convertJsonArrayIntoPdxCollection(final String jsonArray) {
    JSONArray jsonArr = null;
    try {
      jsonArr = new JSONArray(jsonArray);
      Collection<PdxInstance> pdxInstances = new ArrayList<PdxInstance>();
      
      
      for(int index=0; index < jsonArr.length(); index++){
        //String element = jsonArr.getJSONObject(i).toString();
        //String element  = jsonArr.getString(i);
        Object object= jsonArr.get(index);
        String element = object.toString();
        
        PdxInstance pi = convert(element);
        pdxInstances.add(pi);
      }
      return pdxInstances;
    
    } catch (JSONException je) {
      throw new MalformedJsonException("Json document specified in request body is not valid!", je);
    }
  }
  
  
  /*
  protected PdxInstance convertJsonIntoPdxCollection(final String jsonArray) {
    JSONArray jsonArr = null;
    
      PdxInstance pi = convert(jsonArray);
      System.out.println("Successfully converted into PdxInstance..!!");
      return pi;
    
  }
  */
  
  protected Object casValue(final String regionNamePath, final Object key, final Object oldValue, final Object newValue) {
    final Region<Object, Object> region = getRegion(regionNamePath);
    try {
      return (region.replace(key, oldValue, newValue) ? null : region.get(key));
    } catch(UnsupportedOperationException use){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not support the requested operation!", regionNamePath), use);
    }catch(ClassCastException cce){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow to store specified key or value type in this region!", regionNamePath), cce);
    }catch(NullPointerException npe){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow null keys or values!", regionNamePath), npe);
    }catch(IllegalArgumentException iae){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration prevents specified data from being stored in it!", regionNamePath), iae);
    }catch(org.apache.geode.distributed.LeaseExpiredException lee){
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    }catch(TimeoutException toe){
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", toe);
    }catch(CacheWriterException cwe){
      throw new GemfireRestException("Server has encountered CacheWriter error while processing this request!", cwe);
    }catch(PartitionedRegionStorageException prse){
      throw new GemfireRestException("Requested operation could not be completed on a partitioned region!", prse);
    }catch(LowMemoryException lme){
      throw new GemfireRestException("Server has detected low memory while processing this request!", lme);
    } 
  }
    
  protected void replaceValue(final String regionNamePath, final Object key, final PdxInstance value) {
    try {
      if(getRegion(regionNamePath).replace(key, value) == null){
        throw new ResourceNotFoundException(String.format("No resource at (%1$s) exists!", toUri(regionNamePath, String.valueOf(key))));
      }
    }catch(UnsupportedOperationException use){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not support the requested operation!", regionNamePath), use);
    }catch(ClassCastException cce){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow to store specified key or value type in this region!", regionNamePath), cce);
    }catch(NullPointerException npe){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow null keys or values!", regionNamePath), npe);
    }catch(IllegalArgumentException iae){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration prevents specified data from being stored in it!", regionNamePath), iae);
    }catch(org.apache.geode.distributed.LeaseExpiredException lee){
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    }catch(TimeoutException toe){
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", toe);
    }catch(CacheWriterException cwe){
      throw new GemfireRestException("Server has encountered CacheWriter error while processing this request!", cwe);
    }catch(PartitionedRegionStorageException prse){
      throw new GemfireRestException("Requested operation could not be completed on a partitioned region!", prse);
    }catch(LowMemoryException lme){
      throw new GemfireRestException("Server has detected low memory while processing this request!", lme);
    }
  }     
  
  protected void replaceValue(final String regionNamePath, final Object key, final Object value) {
    //still do we need to worry for race condition..?
    try {
      if(getRegion(regionNamePath).replace(key, value) == null){
        throw new ResourceNotFoundException(String.format("No resource at (%1$s) exists!", toUri(regionNamePath, String.valueOf(key))));
      }
    }catch(UnsupportedOperationException use){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not support the requested operation!", regionNamePath), use);
    }catch(ClassCastException cce){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow to store specified key or value type in this region!", regionNamePath), cce);
    }catch(NullPointerException npe){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow null keys or values!", regionNamePath), npe);
    }catch(IllegalArgumentException iae){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration prevents specified data from being stored in it!", regionNamePath), iae);
    }catch(org.apache.geode.distributed.LeaseExpiredException lee){
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    }catch(TimeoutException toe){
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", toe);
    }catch(CacheWriterException cwe){
      throw new GemfireRestException("Server has encountered CacheWriter error while processing this request!", cwe);
    }catch(PartitionedRegionStorageException prse){
      throw new GemfireRestException("Requested operation could not be completed on a partitioned region!", prse);
    }catch(LowMemoryException lme){
      throw new GemfireRestException("Server has detected low memory while processing this request!", lme);
    }
  }
  
  protected void putValue(final String regionNamePath, final Object key, final Object value){
    try {
      getRegion(regionNamePath).put(key, value);
    } catch (NullPointerException npe) {
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow null keys or values!", regionNamePath), npe);
    } catch (ClassCastException cce) {
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow to store specified key or value type in this region!", regionNamePath), cce);
    } catch (org.apache.geode.distributed.LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", toe);
    } catch (CacheWriterException cwe) {
      throw new GemfireRestException("Server has encountered CacheWriter error while processing this request!", cwe);
    } catch (PartitionedRegionStorageException prse) {
      throw new GemfireRestException("Requested operation could not be completed on a partitioned region!", prse);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException("Server has detected low memory while processing this request!", lme);
    }
  }
  
  protected void deleteQueryId(final String regionNamePath, final String key){
    getQueryStore(regionNamePath).remove(key);
  }
  
  protected void deleteNamedQuery(final String regionNamePath, final String key){
    //Check whether query ID exist in region or not
    checkForQueryIdExist(regionNamePath, key);
    deleteQueryId(regionNamePath, key);  
  }
  
  protected void checkForQueryIdExist(String region, String key) {
    if(!getQueryStore(region).containsKey(key)) {
      throw new ResourceNotFoundException(String.format("Named query (%1$s) does not exist!", key));
    }
  }
  
  protected Region<String, String> getQueryStore(final String namePath) {
    return  ValidationUtils.returnValueThrowOnNull(getCache().<String, String>getRegion(namePath),
      new GemfireRestException(String.format("Query store does not exist!",
        namePath)));
  }
  
  protected String getQueryIdValue(final String regionNamePath, final String key) {
    Assert.notNull(key, "queryId cannot be null!");
    try {
      return  getQueryStore(regionNamePath).get(key);
    } catch(NullPointerException npe) {
      throw new GemfireRestException("NULL query ID or query string is not supported!", npe);
    } catch(IllegalArgumentException iae) {
      throw new GemfireRestException("Server has not allowed to perform the requested operation!", iae);
    } catch(org.apache.geode.distributed.LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    } catch(TimeoutException te) {
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", te);
    } 
  }
  
  protected void updateNamedQuery(final String regionNamePath, final String key, final String value){
    try {
      getQueryStore(regionNamePath).put(key, value);
    } catch (NullPointerException npe) {
      throw new GemfireRestException("NULL query ID or query string is not supported!", npe);
    } catch (ClassCastException cce) {
      throw new GemfireRestException("specified queryId or query string is not supported!", cce);
    } catch (org.apache.geode.distributed.LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    } catch (TimeoutException toe) {
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", toe);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException("Server has detected low memory while processing this request!", lme);
    }
  }
  
  @SuppressWarnings("unchecked")
  protected <T> T createNamedQuery(final String regionNamePath, final String key, final String value){
    try {
      return (T) getQueryStore(regionNamePath).putIfAbsent(key, value);
    } catch(UnsupportedOperationException use){
      throw new GemfireRestException("Requested operation is not supported!", use);
    } catch(ClassCastException cce){
      throw new GemfireRestException("Specified queryId or query string is not supported!", cce);
    } catch(NullPointerException npe){
      throw new GemfireRestException("NULL query ID or query string is not supported!", npe);
    } catch(IllegalArgumentException iae){
      throw new GemfireRestException("Configuration does not allow to perform the requested operation!", iae);
    } catch(org.apache.geode.distributed.LeaseExpiredException lee){
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    } catch(TimeoutException toe){
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", toe);
    } catch(LowMemoryException lme){
      throw new GemfireRestException("Server has detected low memory while processing this request!", lme);
    }
  }
  
  protected void putPdxValues(final String regionNamePath, final Map<Object, PdxInstance> map){
    try {
    getRegion(regionNamePath).putAll(map);
    } catch (LowMemoryException lme) {
      throw new GemfireRestException("low memory condition is detected.", lme);
    }
  }
  
  protected void putValues(final String regionNamePath, final Map<Object, Object> values) {
    getRegion(regionNamePath).putAll(values);
  }
  
  protected void putValues(final String regionNamePath, String[] keys, List<?> values) {
    Map<Object, Object> map = new HashMap<Object, Object>();
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
  protected <T> T postValue(final String regionNamePath, final Object key, final Object value){ 
    try {
      return (T) getRegion(regionNamePath).putIfAbsent(key, value);
    }catch(UnsupportedOperationException use){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not support the requested operation!", regionNamePath), use);
    }catch(ClassCastException cce){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow to store specified key or value type in this region!", regionNamePath), cce);
    }catch(NullPointerException npe){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow null keys or values!", regionNamePath), npe);
    }catch(IllegalArgumentException iae){
      throw new GemfireRestException(String.format("Resource (%1$s) configuration prevents specified data from being stored in it!", regionNamePath), iae);
    }catch(org.apache.geode.distributed.LeaseExpiredException lee){
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    }catch(TimeoutException toe){
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", toe);
    }catch(CacheWriterException cwe){
      throw new GemfireRestException("Server has encountered CacheWriter error while processing this request!", cwe);
    }catch(PartitionedRegionStorageException prse){
      throw new GemfireRestException("Requested operation could not be completed on a partitioned region!", prse);
    }catch(LowMemoryException lme){
      throw new GemfireRestException("Server has detected low memory while processing this request!", lme);
    }
  }
 
  protected Object getActualTypeValue(final String value, final String valueType){
    Object actualValue = value;
    if(valueType != null){
      try {
        actualValue  = NumberUtils.convertToActualType(value, valueType);
      } catch (IllegalArgumentException ie) {
        throw new GemfireRestException(ie.getMessage(), ie);
      } 
    }
    return actualValue;
  }
  
  protected String generateKey(final String existingKey){
    return generateKey(existingKey, null);
  }
  
  protected String generateKey(final String existingKey, final Object domainObject) {
    Object domainObjectId = IdentifiableUtils.getId(domainObject);
    String newKey;

    if (StringUtils.hasText(existingKey)) {
      newKey = existingKey;
      if (NumberUtils.isNumeric(newKey) && domainObjectId == null) {
        final Long newId = IdentifiableUtils.createId(NumberUtils.parseLong(newKey));
        if (newKey.equals(newId.toString())) {
          IdentifiableUtils.setId(domainObject, newId);
        }
      }
    }
    else if (domainObjectId != null) {
      final Long domainObjectIdAsLong = NumberUtils.longValue(domainObjectId);
      if (domainObjectIdAsLong != null) {
        final Long newId = IdentifiableUtils.createId(domainObjectIdAsLong);
        if (!domainObjectIdAsLong.equals(newId)) {
          IdentifiableUtils.setId(domainObject, newId);
        }
        newKey = String.valueOf(newId);
      }
      else {
        newKey = String.valueOf(domainObjectId);
      }
    }
    else {
      domainObjectId = IdentifiableUtils.createId();
      newKey = String.valueOf(domainObjectId);
      IdentifiableUtils.setId(domainObject, domainObjectId);
    }

    return newKey;
  }
  
  protected String decode(final String value, final String encoding) {
    try {
      return URLDecoder.decode(value, encoding);
    }
    catch (UnsupportedEncodingException e) {
      throw new GemfireRestException("Server has encountered unsupported encoding!");
    }
  }
  
  @SuppressWarnings("unchecked")
  protected <T> Region<Object, T> getRegion(final String namePath) {
    return (Region<Object, T>) ValidationUtils.returnValueThrowOnNull(getCache().getRegion(namePath),
      new RegionNotFoundException(String.format("The Region identified by name (%1$s) could not be found!",
        namePath)));
  }
  
  protected void checkForKeyExist(String region, String key) {
    if(!getRegion(region).containsKey(key)) {
      throw new ResourceNotFoundException(String.format("Key (%1$s) does not exist for region (%2$s) in cache!", key, region));
    }
  }
  
  protected List<String> checkForMultipleKeysExist(String region, String... keys) {
    List<String> unknownKeys = new ArrayList<String>();
    for (int index=0; index < keys.length; index++) {
      if(!getRegion(region).containsKey(keys[index])) {
        //throw new ResourceNotFoundException(String.format("Key [(%1$s)] does not exist for region [(%2$s)] in cache!", key, region));
        unknownKeys.add(keys[index]);
      }
    }
    return unknownKeys;
  }
  
  protected Object[] getKeys(final String regionNamePath, Object[] keys) {
    return (!(keys == null || keys.length == 0) ? keys : getRegion(regionNamePath).keySet().toArray());
  }
  
  protected <T> Map<Object, T> getValues(final String regionNamePath, Object... keys)  {
    try{ 
      final Region<Object, T> region = getRegion(regionNamePath);
      final Map<Object, T> entries = region.getAll(Arrays.asList(getKeys(regionNamePath, keys)));
      return entries;
    } catch(SerializationException se) {
      throw new DataTypeNotSupportedException("The resource identified could not convert into the supported content characteristics (JSON)!", se);
    }
  }
  protected <T> Map<Object, T> getValues(final String regionNamePath, String... keys) {
    return getValues(regionNamePath, (Object[])keys);
  }  
  
  protected <T extends PdxInstance> Collection<T> getPdxValues(final String regionNamePath, final Object... keys) {
    final Region<Object, T> region = getRegion(regionNamePath);
    final Map<Object, T> entries = region.getAll(Arrays.asList(getKeys(regionNamePath, keys)));
    
    return entries.values();
  }
  
  protected void deleteValue(final String regionNamePath, final Object key){
    getRegion(regionNamePath).remove(key);
  }
  
  protected void deleteValues(final String regionNamePath, final Object...keys){
    //Check whether all keys exist in cache or not
    for(final Object key : keys){
      checkForKeyExist(regionNamePath, key.toString());
    }
    
    for(final Object key : keys){
      deleteValue(regionNamePath, key);
    }
  }
  
  protected void deleteValues(String regionNamePath){
    try{
      getRegion(regionNamePath).clear();
    }catch(UnsupportedOperationException ue){
      throw new GemfireRestException("Requested operation not allowed on partition region", ue);
    }
  }
  
  private boolean isForm(final Map<Object, Object> rawDataBinding) {
    return (!rawDataBinding.containsKey(TYPE_META_DATA_PROPERTY)
      && rawDataBinding.containsKey(OLD_META_DATA_PROPERTY)
      && rawDataBinding.containsKey(NEW_META_DATA_PROPERTY));
  }
  
  @SuppressWarnings("unchecked")
  protected <T> T introspectAndConvert(final T value) {
    if (value instanceof Map) {
      final Map rawDataBinding = (Map) value;

      if (isForm(rawDataBinding)) {
        rawDataBinding.put(OLD_META_DATA_PROPERTY, introspectAndConvert(rawDataBinding.get(OLD_META_DATA_PROPERTY)));
        rawDataBinding.put(NEW_META_DATA_PROPERTY, introspectAndConvert(rawDataBinding.get(NEW_META_DATA_PROPERTY)));

        return (T) rawDataBinding;
      }
      else {
        final String typeValue = (String) rawDataBinding.get(TYPE_META_DATA_PROPERTY);
        
        //Added for the primitive types put. Not supporting primitive types 
        if(NumberUtils.isPrimitiveOrObject(typeValue.toString())){ 
          final Object primitiveValue = rawDataBinding.get("@value");
          try {
            return (T) NumberUtils.convertToActualType(primitiveValue.toString(), typeValue);
          } catch (IllegalArgumentException e) {
            throw new GemfireRestException("Server has encountered error (illegal or inappropriate arguments).", e);  
          }   
        } else {
          
          Assert.state(typeValue != null, "The class type of the object to persist in GemFire must be specified in JSON content using the '@type' property!");
          Assert.state(ClassUtils.isPresent(String.valueOf(typeValue), Thread.currentThread().getContextClassLoader()),
            String.format("Class (%1$s) could not be found!", typeValue));
          
          return (T) objectMapper.convertValue(rawDataBinding, ClassUtils.resolveClassName(String.valueOf(typeValue),
            Thread.currentThread().getContextClassLoader()));
        }
      }
    }

    return value;
  }

  protected String convertErrorAsJson(String errorMessage) {
    return ("{" + "\"cause\"" + ":" + "\"" + errorMessage + "\"" + "}");
  }

  protected String convertErrorAsJson(Throwable t) {
    StringWriter writer = new StringWriter();
    t.printStackTrace(new PrintWriter(writer));
    String returnString = writer.toString();
    returnString = returnString.replace("\n"," ");
    returnString = returnString.replace("\t"," ");
    return String.format("{\"message\" : \"%1$s\", \"stackTrace\" : \"%2$s\"}", t.getMessage(), returnString);
  }

  protected Map<?,?> convertJsonToMap(final String jsonString) {
    Map<String,String> map = new HashMap<String,String>();
    
    //convert JSON string to Map
    try {
      map = objectMapper.readValue(jsonString, new TypeReference<HashMap<String,String>>(){});
    } catch (JsonParseException e) {
      throw new MalformedJsonException("Bind params specified as JSON document in the request is incorrect!", e);
    } catch (JsonMappingException e) {
      throw new MalformedJsonException("Server unable to process bind params specified as JSON document in the request!", e);
    } catch (IOException e) {
      throw new GemfireRestException("Server has encountered error while process this request!", e);
    }
    return map;
 }
  
  protected Object jsonToObject(final String jsonString) {
    return introspectAndConvert(convertJsonToMap(jsonString));  
  }
  
  protected Object[] jsonToObjectArray(final String arguments) {
    final JSONTypes jsonType = validateJsonAndFindType(arguments);
    if(JSONTypes.JSON_ARRAY.equals(jsonType)){
      try {
        JSONArray jsonArray  = new JSONArray(arguments);
        Object[] args =  new Object[jsonArray.length()];
        for(int index=0; index < jsonArray.length(); index++) {
          args[index] = jsonToObject(jsonArray.get(index).toString());  
        }
        return args;
      } catch (JSONException je) {
        throw new MalformedJsonException("Json document specified in request body is not valid!", je);
      }
    } else if (JSONTypes.JSON_OBJECT.equals(jsonType)) {
      return new Object[] { jsonToObject(arguments) };
    } else {
      throw new MalformedJsonException("Json document specified in request body is not valid!");
    } 
  }
  
  public ResponseEntity<String> updateSingleKey(final String region, final String key, final String json, final String opValue){    
    
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
        if(JSONTypes.JSON_ARRAY.equals(jsonType)){
          putValue(region, key, convertJsonArrayIntoPdxCollection(json));
          //putValue(region, key, convertJsonIntoPdxCollection(json));
        }else {
          putValue(region, key, convert(json));
        }
    }
        
    final HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri(region, key));
    return new ResponseEntity<String>(existingValue, headers, (existingValue == null ? HttpStatus.OK : HttpStatus.CONFLICT));        
  }
  
  
  public ResponseEntity<String> updateMultipleKeys(final String region, final String[] keys, final String json){
    
    JSONArray jsonArr = null;
    try {
      jsonArr = new JSONArray(json);  
    } catch (JSONException e) {
      throw new MalformedJsonException("JSON document specified in the request is incorrect", e);
    }
  
    if(jsonArr.length() != keys.length){
      throw new MalformedJsonException("Each key must have corresponding value (JSON document) specified in the request");
    }
    
    Map<Object, PdxInstance> map = new HashMap<Object, PdxInstance>();    
    for(int i=0; i<keys.length; i++){
      if (logger.isDebugEnabled()) {
        logger.debug("Updating (put) Json document ({}) having key ({}) in Region ({})", json, keys[i], region);
      }
        
      try {
        PdxInstance pdxObj = convert( jsonArr.getJSONObject(i).toString());    
        map.put(keys[i], pdxObj);
      } catch (JSONException e) {
        throw new MalformedJsonException(String.format("JSON document at index (%1$s) in the request body is incorrect", i), e);
      }
    }
     
    if(!CollectionUtils.isEmpty(map)){ 
      putPdxValues(region, map);
    }
    
    HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri(region, StringUtils.arrayToCommaDelimitedString(keys)));
    return new ResponseEntity<String>(headers, HttpStatus.OK);
  }
  
  public JSONTypes validateJsonAndFindType(String json){
    try {
      Object jsonObj = new JSONTokener(json).nextValue();
    
      if (jsonObj instanceof JSONObject){
        return JSONTypes.JSON_OBJECT;    
      }else if (jsonObj instanceof JSONArray){  
        return JSONTypes.JSON_ARRAY;  
      }else {
        return JSONTypes.UNRECOGNIZED_JSON;
      }
    } catch (JSONException je) {
      throw new MalformedJsonException("JSON document specified in the request is incorrect", je);
    }    
  }
  
  protected QueryService getQueryService() {
    return getCache().getQueryService();
  }

  @SuppressWarnings("unchecked")
  protected <T> T getValue(final String regionNamePath, final Object key) {
    Assert.notNull(key, "The Cache Region key to read the value for cannot be null!");
  
    Region r = getRegion(regionNamePath);
    try {
      Object value = r.get(key);
      return (T) value;
    } catch(SerializationException se) {
      throw new DataTypeNotSupportedException("The resource identified could not convert into the supported content characteristics (JSON)!", se);
    } catch(NullPointerException npe) {
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow null keys!", regionNamePath), npe);
    } catch(IllegalArgumentException iae) {
      throw new GemfireRestException(String.format("Resource (%1$s) configuration does not allow requested operation on specified key!", regionNamePath), iae);
    } catch(org.apache.geode.distributed.LeaseExpiredException lee) {
      throw new GemfireRestException("Server has encountered error while processing this request!", lee);
    } catch(TimeoutException te) {
      throw new GemfireRestException("Server has encountered timeout error while processing this request!", te);
    } catch(CacheLoaderException cle){
      throw new GemfireRestException("Server has encountered CacheLoader error while processing this request!", cle);
    } catch(PartitionedRegionStorageException prse) {
      throw new GemfireRestException("CacheLoader could not be invoked on partitioned region!", prse);
    }
    
  }
 
  protected Set<DistributedMember> getMembers(final String... memberIdNames) {
    
    ValidationUtils.returnValueThrowOnNull(memberIdNames, new GemfireRestException("No member found to run function"));
    final Set<DistributedMember> targetedMembers = new HashSet<DistributedMember>(ArrayUtils.length(memberIdNames));
    final List<String> memberIdNameList = Arrays.asList(memberIdNames);
    GemFireCacheImpl c = (GemFireCacheImpl)getCache();
    Set<DistributedMember> distMembers =  c.getDistributedSystem().getAllOtherMembers();
   
    //Add the local node to list
    distMembers.add(c.getDistributedSystem().getDistributedMember());  
    for (DistributedMember member : distMembers) {
      if (memberIdNameList.contains(member.getId()) || memberIdNameList.contains(member.getName())) {
        targetedMembers.add(member);
      }
    }
    return targetedMembers;
  }

  protected Set<DistributedMember> getAllMembersInDS() {
    GemFireCacheImpl c = (GemFireCacheImpl)getCache();
    Set<DistributedMember> distMembers =  c.getDistributedSystem().getAllOtherMembers();
    final Set<DistributedMember> targetedMembers = new HashSet<DistributedMember>();
    
    //find valid data nodes, i.e non locator, non-admin, non-loner nodes
    for (DistributedMember member : distMembers) {
      InternalDistributedMember idm = (InternalDistributedMember)member;
      if (idm.getVmKind() == DistributionManager.NORMAL_DM_TYPE) {
        targetedMembers.add(member);
      }
    }
    //Add the local node to list
    targetedMembers.add(c.getDistributedSystem().getDistributedMember());
    return targetedMembers;
  }
}
