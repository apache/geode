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
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.query.internal.Undefined;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.commands.DataCommands;
import com.gemstone.gemfire.management.internal.cli.domain.DataCommandRequest;
import com.gemstone.gemfire.management.internal.cli.domain.DataCommandResult;
import com.gemstone.gemfire.management.internal.cli.domain.DataCommandResult.SelectResultRow;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.multistep.CLIMultiStepHelper;
import com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.util.JsonUtil;
import com.gemstone.gemfire.pdx.PdxInstance;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;

/***
 * 
 * since 7.0
 */

public class DataCommandFunction extends FunctionAdapter implements  InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 1L;
  private boolean optimizeForWrite = false;
  
  protected static final String SELECT_STEP_DISPLAY = "SELECT_DISPLAY";
  protected static final String SELECT_STEP_MOVE = "SELECT_PAGE_MOVE";
  protected static final String SELECT_STEP_END = "SELECT_END";
  protected static final String SELECT_STEP_EXEC = "SELECT_EXEC";
  private static final int NESTED_JSON_LENGTH = 20;

  @Override
  public String getId() {
    return DataCommandFunction.class.getName();
  }

  @Override

  public boolean hasResult() {
    return true;
  }

  @Override 

  public boolean isHA() {
    return false;
  }

  @Override
  /**
   * Read only function
   */
  public boolean optimizeForWrite() {
    return optimizeForWrite;
  }  
  

  public void setOptimizeForWrite(boolean optimizeForWrite) {
    this.optimizeForWrite = optimizeForWrite;
  }

  @Override
  public void execute(FunctionContext functionContext) {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      DataCommandRequest request =(DataCommandRequest) functionContext.getArguments();
      if(logger.isDebugEnabled()){
        logger.debug("Executing function : \n{}\n on member {}", request, System.getProperty("memberName"));
      }
      DataCommandResult result = null;
      if(request.isGet())
        result = get(request);
      else if(request.isLocateEntry())
        result = locateEntry(request);
      else if(request.isPut())
        result = put(request);
      else if(request.isRemove())
        result = remove(request);
      else if(request.isSelect())
        result = select(request);
      if (logger.isDebugEnabled()) {
        logger.debug("Result is {}", result);
      }
      functionContext.getResultSender().lastResult(result);
    } catch (CacheClosedException e) {
      e.printStackTrace();
      functionContext.getResultSender().sendException(e);
    } catch (Exception e) {
      e.printStackTrace();
      functionContext.getResultSender().sendException(e);
    }
  }
  
  

  public DataCommandResult remove(DataCommandRequest request) {
    String key = request.getKey();   
    String keyClass = request.getKeyClass();
    String regionName = request.getRegionName();
    String removeAllKeys = request.getRemoveAllKeys();
    return remove(key,keyClass,regionName,removeAllKeys);
  }

  public DataCommandResult get(DataCommandRequest request) {
    String key = request.getKey();              
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    Boolean loadOnCacheMiss = request.isLoadOnCacheMiss();
    return get(key, keyClass, valueClass, regionName, loadOnCacheMiss);
  }
  
  public DataCommandResult locateEntry(DataCommandRequest request) {
    String key = request.getKey();              
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    boolean recursive = request.isRecursive();
    return locateEntry(key,keyClass,valueClass,regionName,recursive);
  }


  public DataCommandResult put(DataCommandRequest request) {
    String key = request.getKey();
    String value = request.getValue();
    boolean putIfAbsent = request.isPutIfAbsent();
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    return put(key,value,putIfAbsent,keyClass,valueClass,regionName);
  }
  
  public DataCommandResult select(DataCommandRequest request) {
    String query = request.getQuery();    
    return select(query);
  }
  
  /**
   * To catch trace output
   */
  public static class WrappedIndexTrackingQueryObserver extends IndexTrackingQueryObserver{
    
    @Override
    public void reset() {
      //NOOP
    }
    
    public void reset2() {
      super.reset();
    }
  }
  
  @SuppressWarnings("rawtypes")
  private DataCommandResult select(String queryString) {

    Cache cache = CacheFactory.getAnyInstance();
    AtomicInteger nestedObjectCount = new AtomicInteger(0);
    if (queryString != null && !queryString.isEmpty()) {
      QueryService qs = cache.getQueryService();

      // TODO : Find out if is this optimised use. Can you have something equivalent of parsed queries with names
      // where name can be retrieved to avoid parsing every-time
      Query query = qs.newQuery(queryString);
      DefaultQuery tracedQuery = (DefaultQuery)query;
      WrappedIndexTrackingQueryObserver queryObserver=null;
      String queryVerboseMsg = null;
      long startTime=-1;
      if(tracedQuery.isTraced()){
        startTime = NanoTimer.getTime();
        queryObserver = new WrappedIndexTrackingQueryObserver();
        QueryObserverHolder.setInstance(queryObserver);
      }
      List<SelectResultRow> list = new ArrayList<SelectResultRow>();

      try {
        Object results = query.execute();
        if(tracedQuery.isTraced()){
          queryVerboseMsg = getLogMessage(queryObserver, startTime,queryString);
          queryObserver.reset2();
        }
        if (results instanceof SelectResults) {
          SelectResults selectResults = (SelectResults) results;
          for (Iterator iter = selectResults.iterator(); iter.hasNext();) {
            Object object = iter.next();
            if (object instanceof Struct) {
              StructImpl impl = (StructImpl) object;
              GfJsonObject jsonStruct = getJSONForStruct(impl, nestedObjectCount);
              if(logger.isDebugEnabled())
                logger.debug("SelectResults : Adding select json string : {}", jsonStruct);
              list.add(new SelectResultRow(DataCommandResult.ROW_TYPE_STRUCT_RESULT, jsonStruct.toString()));
            } else {
              if (JsonUtil.isPrimitiveOrWrapper(object.getClass())) {
                if(logger.isDebugEnabled())
                  logger.debug("SelectResults : Adding select primitive : {}", object);
                list.add(new SelectResultRow(DataCommandResult.ROW_TYPE_PRIMITIVE, object));
              } else {
                if(logger.isDebugEnabled())
                  logger.debug("SelectResults : Bean Results class is {}", object.getClass());
                String str = toJson(object);
                GfJsonObject jsonBean;
                try {
                  jsonBean = new GfJsonObject(str);
                } catch (GfJsonException e) {
                  logger.fatal(e.getMessage(), e);
                  jsonBean = new GfJsonObject();
                  try {
                    jsonBean.put("msg", e.getMessage());
                  } catch (GfJsonException e1) {
                  }
                }
                if(logger.isDebugEnabled())
                  logger.debug("SelectResults : Adding bean json string : {}", jsonBean);
                list.add(new SelectResultRow(DataCommandResult.ROW_TYPE_BEAN, jsonBean.toString()));
              }
            }
          }
        } else {
          if(logger.isDebugEnabled())
            logger.debug("BeanResults : Bean Results class is {}", results.getClass());
          String str = toJson(results);
          GfJsonObject jsonBean;
          try {
            jsonBean = new GfJsonObject(str);
          } catch (GfJsonException e) {
            e.printStackTrace();
            jsonBean = new GfJsonObject();
            try {
              jsonBean.put("msg", e.getMessage());
            } catch (GfJsonException e1) {
            }
          }
          if(logger.isDebugEnabled())
            logger.debug("BeanResults : Adding bean json string : {}", jsonBean);
          list.add(new SelectResultRow(DataCommandResult.ROW_TYPE_BEAN, jsonBean.toString()));
        }        
        return DataCommandResult.createSelectResult(queryString, list, queryVerboseMsg, null, null, true);

      } catch (FunctionDomainException e) {
        logger.warn(e.getMessage(), e);
        return DataCommandResult.createSelectResult(queryString, null, queryVerboseMsg, e, e.getMessage(), false);
      } catch (TypeMismatchException e) {
        logger.warn(e.getMessage(), e);
        return DataCommandResult.createSelectResult(queryString, null, queryVerboseMsg, e, e.getMessage(), false);
      } catch (NameResolutionException e) {
        logger.warn(e.getMessage(), e);
        return DataCommandResult.createSelectResult(queryString, null, queryVerboseMsg, e, e.getMessage(), false);
      } catch (QueryInvocationTargetException e) {
        logger.warn(e.getMessage(), e);
        return DataCommandResult.createSelectResult(queryString, null, queryVerboseMsg, e, e.getMessage(), false);
      } catch (GfJsonException e) {
        logger.warn(e.getMessage(), e);
        return DataCommandResult.createSelectResult(queryString, null, queryVerboseMsg, e, e.getMessage(), false);
      }finally{
        if(queryObserver!=null){          
          QueryObserverHolder.reset();
        }
      }
    } else {
      return DataCommandResult
          .createSelectInfoResult(null, null, -1, null, CliStrings.QUERY__MSG__QUERY_EMPTY, false);
    }
  }
  
  private String toJson(Object object){
    if(object instanceof Undefined){
      return "{\"Value\":\"UNDEFINED\"}";
    }else if (object instanceof PdxInstance)
      return pdxToJson((PdxInstance)object);
    else
      return JsonUtil.objectToJsonNestedChkCDep(object, NESTED_JSON_LENGTH);
  }

  private GfJsonObject getJSONForStruct(StructImpl impl, AtomicInteger ai) throws GfJsonException {
    String fields[] = impl.getFieldNames();
    Object[] values = impl.getFieldValues();    
    GfJsonObject jsonObject = new GfJsonObject();
    for(int i=0;i<fields.length;i++){
      Object value = values[i];
      if(value!=null){
        if (JsonUtil.isPrimitiveOrWrapper(value.getClass())) {
          jsonObject.put(fields[i], value);
        } else {
          //jsonObject.put(fields[i], value.getClass().getCanonicalName());
          jsonObject.put(fields[i],toJson(value));
        }
      }else{
        jsonObject.put(fields[i], "null");
      }
    }
    return jsonObject;
  }

  @SuppressWarnings({ "rawtypes" })
  public DataCommandResult remove(String key, String keyClass, String regionName, String removeAllKeys) {
    
    Cache cache = CacheFactory.getAnyInstance();
    
    if(regionName==null || regionName.isEmpty()){
      return DataCommandResult.createRemoveResult(key, null, null, CliStrings.REMOVE__MSG__REGIONNAME_EMPTY, false);
    }
    
    boolean allKeysFlag = (removeAllKeys==null || removeAllKeys.isEmpty());    
    if(allKeysFlag && (key==null || key.isEmpty())){
      return DataCommandResult.createRemoveResult(key, null, null, CliStrings.REMOVE__MSG__KEY_EMPTY,false);
    }     
     
    Region region = cache.getRegion(regionName); 
    if(region==null){
      return DataCommandResult.createRemoveInfoResult(key, null, null, CliStrings.format(CliStrings.REMOVE__MSG__REGION_NOT_FOUND,regionName), false);
    }else{      
      if(removeAllKeys==null){
        Object keyObject = null;
        try{
          keyObject = getClassObject(key,keyClass);
        }catch(ClassNotFoundException e){
          return DataCommandResult.createRemoveResult(key, null, null, "ClassNotFoundException " + keyClass, false); 
        }catch(IllegalArgumentException e){
          return DataCommandResult.createRemoveResult(key, null, null, "Error in converting JSON " + e.getMessage(), false); 
        }
        
        if(region.containsKey(keyObject)){
          Object value= region.remove(keyObject);
          if (logger.isDebugEnabled()) 
            logger.debug("Removed key {} successfully", key);
          //return DataCommandResult.createRemoveResult(key, value, null, null);
          Object array[] = getJSONForNonPrimitiveObject(value);
          DataCommandResult result = DataCommandResult.createRemoveResult(key,array[1] , null, null,true);
          if(array[0]!=null)
            result.setValueClass((String)array[0]);
          return result;
        }else{
          return DataCommandResult.createRemoveInfoResult(key, null, null, CliStrings.REMOVE__MSG__KEY_NOT_FOUND_REGION,false); 
        }
      } else {
        DataPolicy policy = region.getAttributes().getDataPolicy();
        if (!policy.withPartitioning()) {
          region.clear();
          if (logger.isDebugEnabled()) 
            logger.debug("Cleared all keys in the region - {}", regionName);
          return DataCommandResult.createRemoveInfoResult(key, null, null,
              CliStrings.format(CliStrings.REMOVE__MSG__CLEARED_ALL_CLEARS, regionName), true);
        } else {
          return DataCommandResult.createRemoveInfoResult(key, null, null,
              CliStrings.REMOVE__MSG__CLEAREALL_NOT_SUPPORTED_FOR_PARTITIONREGION, false);
        }
      }        
    }    
  }
  
  @SuppressWarnings({ "rawtypes" })
  public DataCommandResult get(String key, String keyClass, String valueClass, String regionName, Boolean loadOnCacheMiss) {
    
    Cache cache = CacheFactory.getAnyInstance();
    
    if(regionName==null || regionName.isEmpty()){
     return DataCommandResult.createGetResult(key, null, null, CliStrings.GET__MSG__REGIONNAME_EMPTY, false);
    }
    
    if(key==null || key.isEmpty()){
      return DataCommandResult.createGetResult(key, null, null, CliStrings.GET__MSG__KEY_EMPTY, false);
    }
    
    Region region = cache.getRegion(regionName);

    if(region==null){
      if (logger.isDebugEnabled()) 
        logger.debug("Region Not Found - {}", regionName);
      return DataCommandResult.createGetResult(key, null, null, CliStrings.format(CliStrings.GET__MSG__REGION_NOT_FOUND,regionName), false);
    }else{
      Object keyObject = null;
      try{
        keyObject = getClassObject(key,keyClass);
      }catch(ClassNotFoundException e){
        return DataCommandResult.createGetResult(key, null, null, "ClassNotFoundException " + keyClass, false); 
      }catch(IllegalArgumentException e){
        return DataCommandResult.createGetResult(key, null, null, "Error in converting JSON " + e.getMessage(), false); 
      }

      // TODO determine whether the following conditional logic (assigned to 'doGet') is safer or necessary
      //boolean doGet = (Boolean.TRUE.equals(loadOnCacheMiss) && region.getAttributes().getCacheLoader() != null);
      boolean doGet = Boolean.TRUE.equals(loadOnCacheMiss);

      if (doGet || region.containsKey(keyObject)) {
        Object value= region.get(keyObject);
        if (logger.isDebugEnabled()) 
          logger.debug("Get for key {} value {}", key, value);
        //return DataCommandResult.createGetResult(key, value, null, null);
        Object array[] = getJSONForNonPrimitiveObject(value);
        if(value!=null){         
          DataCommandResult result = DataCommandResult.createGetResult(key, array[1], null, null, true);
          if(array[0]!=null)
            result.setValueClass((String)array[0]);
          return result;
        }
        else{
          return DataCommandResult.createGetResult(key, array[1], null, null, false);
        }
      }else{
        if (logger.isDebugEnabled()) 
          logger.debug("Key is not present in the region {}", regionName);
        return DataCommandResult.createGetInfoResult(key, null, null, CliStrings.GET__MSG__KEY_NOT_FOUND_REGION, false); 
      }  
    }  
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public DataCommandResult locateEntry(String key, String keyClass, String valueClass, String regionPath, boolean recursive) {
    
    Cache cache = CacheFactory.getAnyInstance();
    
    if(regionPath==null || regionPath.isEmpty()){
     return DataCommandResult.createLocateEntryResult(key, null,null, CliStrings.LOCATE_ENTRY__MSG__REGIONNAME_EMPTY, false);
    }
    
    if(key==null || key.isEmpty()){
      return DataCommandResult.createLocateEntryResult(key, null, null, CliStrings.LOCATE_ENTRY__MSG__KEY_EMPTY, false);
    }
    
    List<Region> listofRegionStartingwithRegionPath = new ArrayList<Region>();
    
    if(recursive){
      // Recursively find the keys starting from the specified region path.
      List<String> regionPaths = getAllRegionPaths(cache, true);
      for (int i = 0; i < regionPaths.size(); i++) {
        String path = regionPaths.get(i);
        if (path.startsWith(regionPath) || path.startsWith(Region.SEPARATOR + regionPath)) {
          Region targetRegion = cache.getRegion(path);
          listofRegionStartingwithRegionPath.add(targetRegion);
        }
      }      
      if(listofRegionStartingwithRegionPath.size()==0){
        if (logger.isDebugEnabled()) 
          logger.debug("Region Not Found - {}", regionPath);
        return DataCommandResult.createLocateEntryResult(key, null, null, CliStrings.format(CliStrings.REMOVE__MSG__REGION_NOT_FOUND, regionPath) , false);
      }
    }else{
      Region region = cache.getRegion(regionPath);
      if(region==null){
        if (logger.isDebugEnabled()) 
          logger.debug("Region Not Found - {}", regionPath);
        return DataCommandResult.createLocateEntryResult(key, null, null, CliStrings.format(CliStrings.REMOVE__MSG__REGION_NOT_FOUND, regionPath) , false);
      }
      else        
        listofRegionStartingwithRegionPath.add(region);
    }
    
    Object keyObject = null;
    try{
      keyObject = getClassObject(key,keyClass);
    }catch(ClassNotFoundException e){
      logger.fatal(e.getMessage(), e);
      return DataCommandResult.createLocateEntryResult(key, null,null, "ClassNotFoundException " + keyClass, false); 
    }catch(IllegalArgumentException e){
      logger.fatal(e.getMessage(), e);
      return DataCommandResult.createLocateEntryResult(key, null, null, "Error in converting JSON " + e.getMessage(), false); 
    }
    
    Object value = null;
    DataCommandResult.KeyInfo keyInfo= null;
    keyInfo = new DataCommandResult.KeyInfo();
    DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    keyInfo.setHost(member.getHost());
    keyInfo.setMemberId(member.getId());
    keyInfo.setMemberName(member.getName());
    
    for(Region region : listofRegionStartingwithRegionPath){      
      if (region instanceof PartitionedRegion) {
        //Following code is adaptation of which.java of old Gfsh
        PartitionedRegion pr = (PartitionedRegion)region;
        Region localRegion = PartitionRegionHelper.getLocalData((PartitionedRegion)region);
        value = localRegion.get(keyObject);
        if(value!=null){
          DistributedMember primaryMember = PartitionRegionHelper.getPrimaryMemberForKey(region, keyObject);
          int bucketId = pr.getKeyInfo(keyObject).getBucketId();
          boolean isPrimary = member == primaryMember;
          keyInfo.addLocation(new Object[]{region.getFullPath(),true,getJSONForNonPrimitiveObject(value)[1],isPrimary,""+bucketId});          
        }else{
          if (logger.isDebugEnabled()) 
            logger.debug("Key is not present in the region {}", regionPath);
          return DataCommandResult.createLocateEntryInfoResult(key, null,null, CliStrings.LOCATE_ENTRY__MSG__KEY_NOT_FOUND_REGION, false);
        }
      }else{
        if(region.containsKey(keyObject)){
          value= region.get(keyObject);         
          if (logger.isDebugEnabled()) 
            logger.debug("Get for key {} value {} in region {}", key, value, region.getFullPath());
          if(value!=null)
            keyInfo.addLocation(new Object[]{region.getFullPath(),true,getJSONForNonPrimitiveObject(value)[1], false,null});
          else
            keyInfo.addLocation(new Object[]{region.getFullPath(),false,null, false,null});
        }else{
          if (logger.isDebugEnabled()) 
            logger.debug("Key is not present in the region {}", regionPath);
          keyInfo.addLocation(new Object[]{region.getFullPath(), false,null, false,null}); 
        }  
      }
    }
    
    if(keyInfo.hasLocation()){
      return DataCommandResult.createLocateEntryResult(key,keyInfo, null, null, true);
    }else{
      return DataCommandResult.createLocateEntryInfoResult(key, null,null, CliStrings.LOCATE_ENTRY__MSG__KEY_NOT_FOUND_REGION, false);
    }
    
  }
  
  @SuppressWarnings({ "rawtypes" })
  public DataCommandResult put(String key, String value, boolean putIfAbsent, String keyClass, String valueClass,
      String regionName) { 
    
    if(regionName==null || regionName.isEmpty()){
      return DataCommandResult.createPutResult(key, null, null, CliStrings.PUT__MSG__REGIONNAME_EMPTY, false);
    }
     
    if(key==null || key.isEmpty()){
       return DataCommandResult.createPutResult(key, null, null, CliStrings.PUT__MSG__KEY_EMPTY, false);
    }
     
    if(value==null || value.isEmpty()){
       return DataCommandResult.createPutResult(key, null, null, CliStrings.PUT__MSG__VALUE_EMPTY, false);
    }
    
    Cache cache = CacheFactory.getAnyInstance();
    Region region = cache.getRegion(regionName); 
    if(region==null){
      return DataCommandResult.createPutResult(key, null, null, CliStrings.format(CliStrings.PUT__MSG__REGION_NOT_FOUND, regionName) , false);
    }
    else{
      Object keyObject = null;
      Object valueObject = null;
      try{
        keyObject = getClassObject(key,keyClass);
      }catch(ClassNotFoundException e){
        return DataCommandResult.createPutResult(key, null, null, "ClassNotFoundException " + keyClass, false); 
      }catch(IllegalArgumentException e){
        return DataCommandResult.createPutResult(key, null, null, "Error in converting JSON " + e.getMessage(), false); 
      }
      
      try{
        valueObject = getClassObject(value,valueClass);
      }catch(ClassNotFoundException e){
        return DataCommandResult.createPutResult(key, null, null, "ClassNotFoundException " + valueClass, false); 
      }
      Object returnValue;
      if (putIfAbsent && region.containsKey(keyObject))
        returnValue = region.get(keyObject);
      else
        returnValue = region.put(keyObject,valueObject);
      Object array[] = getJSONForNonPrimitiveObject(returnValue);             
      DataCommandResult result = DataCommandResult.createPutResult(key, array[1], null, null, true);
      if(array[0]!=null)
        result.setValueClass((String)array[0]);
       return result;
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Object getClassObject(String string, String klassString) throws ClassNotFoundException, IllegalArgumentException{
    if(klassString==null || klassString.isEmpty())
      return string;
    else {
        Object o = null;
        Class klass =  ClassPathLoader.getLatest().forName(klassString);
        
        if(klass.equals(String.class))
          return string;
        
        if(JsonUtil.isPrimitiveOrWrapper(klass)){
          try{
            if(klass.equals(Byte.class)){
              o = Byte.parseByte(string);return o;
            }else if(klass.equals(Short.class)){
             o = Short.parseShort(string);return o; 
            }else if(klass.equals(Integer.class)){
              o = Integer.parseInt(string);return o; 
            }else if(klass.equals(Long.class)){
              o = Long.parseLong(string);return o;
            }else if(klass.equals(Double.class)){
              o = Double.parseDouble(string);return o;
            }else if(klass.equals(Boolean.class)){
              o = Boolean.parseBoolean(string);return o;
            }else if(klass.equals(Float.class)){
              o = Float.parseFloat(string);return o;
            }
            return o;
          }catch(NumberFormatException e){
            throw new IllegalArgumentException("Failed to convert input key to " + klassString + " Msg : " + e.getMessage());
          }                
        }
        
        try{
          o = getObjectFromJson(string, klass);
          return o;
        }catch(IllegalArgumentException e){         
          throw e;
        }
    }
  }
  
  @SuppressWarnings({ "rawtypes"})
  public static Object[] getJSONForNonPrimitiveObject(Object obj){
    Object[] array = new Object[2];
    if(obj==null){
      array[0] = null;array[1] ="<NULL>"; 
      return array;
    }
    else{     
      array[0] = obj.getClass().getCanonicalName();
      Class klass = obj.getClass();
      if(JsonUtil.isPrimitiveOrWrapper(klass))
        array[1] = obj;
      else if (obj instanceof PdxInstance){
        String str = pdxToJson((PdxInstance)obj);
        array[1] =  str;
      }else{
        GfJsonObject object = new GfJsonObject(obj,true);
        Iterator keysIterator = object.keys();
        while(keysIterator.hasNext()){
          String key = (String) keysIterator.next();
          Object value = object.get(key);
          if(GfJsonObject.isJSONKind(value)) {
            GfJsonObject jsonVal = new GfJsonObject(value);
            //System.out.println("Re-wrote inner object");
            try {
              if (jsonVal.has("type-class")) {
                object.put(key, jsonVal.get("type-class"));
              } else {
                // Its Map Value
                object.put(key, "a Map");
              }
            } catch (GfJsonException e) {
              throw new RuntimeException(e);
            }            
          } else if(value instanceof JSONArray) {
            //Its a collection either a set or list
            try {
              object.put(key, "a Collection");
            } catch (GfJsonException e) {
              throw new RuntimeException(e);
            }            
          }
        }
        String str =object.toString();        
        array[1] = str;
      }
      return array;
    }
  }
  
  private static String pdxToJson(PdxInstance obj) {
    if(obj!=null){
      try{
        GfJsonObject json = new GfJsonObject();
        for(String field : obj.getFieldNames()){
          Object fieldValue = obj.getField(field);
          if(fieldValue!=null){
            if(JsonUtil.isPrimitiveOrWrapper(fieldValue.getClass())){
              json.put(field, fieldValue);
            }else{
              json.put(field, fieldValue.getClass());
            }
          }
        }
        return json.toString();
      }catch(GfJsonException e){        
        return null;
      }
    }
    return null;
  }

  public static <V> V getObjectFromJson(String json, Class<V> klass){    
    String newString = json.replaceAll("'", "\"");
    if(newString.charAt(0)=='('){
      int len = newString.length();
      StringBuilder sb = new StringBuilder();
      sb.append("{").append(newString.substring(1, len-1)).append("}");
      newString=  sb.toString();
    }
    V v = JsonUtil.jsonToObject(newString,klass);
    return v;
  }
  
  
  //Copied from RegionUtil of old Gfsh
  /**
   * Returns a sorted list of all region full paths found in the specified
   * cache.
   * @param cache The cache to search.
   * @param recursive recursive search for sub-regions
   * @return Returns a sorted list of all region paths defined in the 
   *         distributed system.  
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static List getAllRegionPaths(Cache cache, boolean recursive)
  {
    ArrayList list = new ArrayList();
    if (cache == null) {
      return list;
    }
    
    // get a list of all root regions
  Set regions = cache.rootRegions();
  Iterator itor = regions.iterator();

  while (itor.hasNext()) {
    String regionPath = ((Region)itor.next()).getFullPath();

    Region region = cache.getRegion(regionPath);
    list.add(regionPath);
    Set subregionSet = region.subregions(true);
    if (recursive) {
      for (Iterator subIter = subregionSet.iterator(); subIter.hasNext(); ){
        list.add(((Region)subIter.next()).getFullPath());
      }
    }
  }
  Collections.sort(list);
  return list;
  }
  
  private static DataCommandResult cachedResult = null;
  
  public static class SelectDisplayStep extends CLIMultiStepHelper.LocalStep{
    
    public SelectDisplayStep(Object[] arguments) {
      super(SELECT_STEP_DISPLAY, arguments);
    }

    @Override
    public Result exec() {
      boolean interactive = (Boolean)commandArguments[2]; 
      GfJsonObject args = CLIMultiStepHelper.getStepArgs();      
      int startCount = args.getInt( DataCommandResult.QUERY_PAGE_START);
      int endCount = args.getInt( DataCommandResult.QUERY_PAGE_END);
      int rows = args.getInt(DataCommandResult.NUM_ROWS); //returns Zero if no rows added so it works.
      boolean flag = args.getBoolean(DataCommandResult.RESULT_FLAG);
      CommandResult commandResult = CLIMultiStepHelper.getDisplayResultFromArgs(args);
      Gfsh.println();
      while (commandResult.hasNextLine()) {
        Gfsh.println(commandResult.nextLine());
      }

      if (flag) {
        boolean paginationNeeded = (startCount < rows) && (endCount < rows) && interactive && flag;
        if (paginationNeeded) {
          while (true) {
            String message = ("Press n to move to next page, q to quit and p to previous page : ");
            try {
              String step = Gfsh.getCurrentInstance().interact(message);
              if ("n".equals(step)) {
                int nextStart = startCount + getPageSize();
                return CLIMultiStepHelper.createBannerResult(new String[] { DataCommandResult.QUERY_PAGE_START,
                    DataCommandResult.QUERY_PAGE_END,  }, new Object[] {
                    nextStart, (nextStart + getPageSize()) }, SELECT_STEP_MOVE);
              } else if ("p".equals(step)) {
                int nextStart = startCount - getPageSize();
                if (nextStart < 0)
                  nextStart = 0;
                return CLIMultiStepHelper.createBannerResult(new String[] { DataCommandResult.QUERY_PAGE_START,
                    DataCommandResult.QUERY_PAGE_END}, new Object[] {
                    nextStart, (nextStart + getPageSize())}, SELECT_STEP_MOVE);
              } else if ("q".equals(step))
                return CLIMultiStepHelper.createBannerResult(new String[] {  },
                    new Object[] {  }, SELECT_STEP_END);
              else
                Gfsh.println("Unknown option ");
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }        
      return CLIMultiStepHelper.createBannerResult(new String[] {},
          new Object[] {}, SELECT_STEP_END);      
    }
  }
 

  public static class SelectMoveStep extends CLIMultiStepHelper.RemoteStep {
    
    private static final long serialVersionUID = 1L;
    
    public SelectMoveStep(Object[] arguments) {
      super(SELECT_STEP_MOVE, arguments);
    }

    @Override
    public Result exec() {
      GfJsonObject args = CLIMultiStepHelper.getStepArgs();      
      int startCount = args.getInt(DataCommandResult.QUERY_PAGE_START);
      int endCount = args.getInt(DataCommandResult.QUERY_PAGE_END);      
      return cachedResult.pageResult(startCount, endCount, SELECT_STEP_DISPLAY);
    }
  };   

  public static class SelectExecStep extends CLIMultiStepHelper.RemoteStep {    
    
    private static final long serialVersionUID = 1L;

    public SelectExecStep(Object[] arguments) {
      super(SELECT_STEP_EXEC, arguments);
    }

    @Override
    public Result exec() {
      String remainingQuery = (String)commandArguments[0];
      boolean interactive = (Boolean)commandArguments[2]; 
      DataCommandResult result = _select(remainingQuery);
      int endCount = 0;
      cachedResult = result;
      if(interactive){          
        endCount = getPageSize();
      }else{
        if(result.getSelectResult()!=null)
          endCount = result.getSelectResult().size();
      }
      if (interactive)
        return result.pageResult(0, endCount, SELECT_STEP_DISPLAY);
      else
        return CLIMultiStepHelper.createBannerResult(new String[] {},
            new Object[] {}, SELECT_STEP_END);
    }
    
    /*private int getLimit(CompiledValue compiledQuery) {
      return compiledQuery instanceof CompiledSelect ?  ((CompiledSelect)compiledQuery).getLimitValue(): -1;
    }*/
    
    public DataCommandResult _select(String query) {
      Cache cache = CacheFactory.getAnyInstance();
      DataCommandResult dataResult = null;

      if (query == null || query.isEmpty()) {
        dataResult = DataCommandResult.createSelectInfoResult(null, null, -1, null,
            CliStrings.QUERY__MSG__QUERY_EMPTY, false);
        return dataResult;
      }
      
      //String query = querySB.toString().trim();      
      Object array[] = DataCommands.replaceGfshEnvVar(query, CommandExecutionContext.getShellEnv());
      query = (String) array[1];
      query = addLimit(query);
      
      @SuppressWarnings("deprecation")
      QCompiler compiler = new QCompiler();
      Set<String> regionsInQuery = null;
      try {
        CompiledValue compiledQuery = compiler.compileQuery(query);
        Set<String> regions = new HashSet<String>();
        compiledQuery.getRegionsInQuery(regions, null);

        // authorize data read on these regions
        for(String region:regions){
          GeodeSecurityUtil.authorizeRegionRead(region);
        }

        regionsInQuery = Collections.unmodifiableSet(regions);
        if (regionsInQuery.size() > 0) {
          Set<DistributedMember> members = DataCommands.getQueryRegionsAssociatedMembers(regionsInQuery, cache, false);
          if (members != null && members.size() > 0) {
            DataCommandFunction function = new DataCommandFunction();
            DataCommandRequest request = new DataCommandRequest();
            request.setCommand(CliStrings.QUERY);
            request.setQuery(query);
            dataResult = DataCommands.callFunctionForRegion(request, function, members);
            dataResult.setInputQuery(query);
            return (dataResult);
          } else {
            return (dataResult = DataCommandResult.createSelectInfoResult(null, null, -1, null,
                CliStrings.format(CliStrings.QUERY__MSG__REGIONS_NOT_FOUND, regionsInQuery.toString()), false));
          }
        } else {
          return (dataResult = DataCommandResult.createSelectInfoResult(null, null, -1, null,
              CliStrings.format(CliStrings.QUERY__MSG__INVALID_QUERY, "Region mentioned in query probably missing /"),
              false));
        }
      } catch (QueryInvalidException qe) {        
        logger.error("{} Failed Error {}", query, qe.getMessage(), qe);
        return (dataResult = DataCommandResult.createSelectInfoResult(null, null, -1, null,
            CliStrings.format(CliStrings.QUERY__MSG__INVALID_QUERY, qe.getMessage()), false));
      }
    }

    private String addLimit(String query) {
      if (StringUtils.containsIgnoreCase(query, " limit") || StringUtils.containsIgnoreCase(query, " count("))
        return query;
      return query + " limit " + getFetchSize();
    }
  };

  public static class SelectQuitStep extends CLIMultiStepHelper.RemoteStep{
    
    public SelectQuitStep(Object[] arguments) {
      super(SELECT_STEP_END, arguments);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public Result exec() {
      boolean interactive = (Boolean)commandArguments[2];
      GfJsonObject args = CLIMultiStepHelper.getStepArgs();      
      DataCommandResult dataResult = cachedResult;
      cachedResult = null;
      if(interactive)
        return CLIMultiStepHelper.createEmptyResult("END");
      else{
        CompositeResultData rd = dataResult.toSelectCommandResult();
        SectionResultData section = rd.addSection(CLIMultiStepHelper.STEP_SECTION);
        section.addData(CLIMultiStepHelper.NEXT_STEP_NAME, "END");
        return ResultBuilder.buildResult(rd);
      }
    }
  };
  
  public static int getPageSize() {
    int pageSize = -1;
    Map<String, String> session = null;
    if (CliUtil.isGfshVM()) {
      session = Gfsh.getCurrentInstance().getEnv();
    } else {
      session = CommandExecutionContext.getShellEnv();
    }
    if(session!=null){
      String size = session.get(Gfsh.ENV_APP_COLLECTION_LIMIT);
      if (size == null || size.isEmpty())
        pageSize = Gfsh.DEFAULT_APP_COLLECTION_LIMIT;
      else
        pageSize = Integer.parseInt(size);
    }
    if (pageSize == -1)
      pageSize = Gfsh.DEFAULT_APP_COLLECTION_LIMIT;
    return pageSize;
  }
  
  private static int getFetchSize() {
    return CommandExecutionContext.getShellFetchSize();
  }
  
  
  public static String getLogMessage(QueryObserver observer,
      long startTime, String query) {
    String usedIndexesString = null;
    String rowCountString = null;
    float time = 0.0f;

    if (startTime > 0L) {
      time = (NanoTimer.getTime() - startTime) / 1.0e6f;
    }

    if (observer != null && observer instanceof IndexTrackingQueryObserver) {
      IndexTrackingQueryObserver indexObserver = (IndexTrackingQueryObserver)observer;
      Map usedIndexes = indexObserver.getUsedIndexes();
      indexObserver.reset();
      StringBuffer buf = new StringBuffer();
      buf.append(" indexesUsed(");
      buf.append(usedIndexes.size());
      buf.append(")");
      if (usedIndexes.size() > 0) {
        buf.append(":");
        for (Iterator itr = usedIndexes.entrySet().iterator(); itr.hasNext();) {
          Map.Entry entry = (Map.Entry) itr.next();
          buf.append(entry.getKey().toString() + entry.getValue());
          if (itr.hasNext()) {
            buf.append(",");
          }
        }
      }
      usedIndexesString = buf.toString();
    } else if (DefaultQuery.QUERY_VERBOSE) {
      usedIndexesString = " indexesUsed(NA due to other observer in the way: "
          + observer.getClass().getName() + ")";
    }

    /*if (resultSize != -1){
      rowCountString = " rowCount = " + resultSize + ";";
    }*/
    return "Query Executed" +
    (startTime > 0L ? " in " + time + " ms;": ";") +
    (rowCountString != null ? rowCountString : "") +
    (usedIndexesString != null ? usedIndexesString : "") 
    /*+ " \"" + query + "\""*/;
  } 
 
}
