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

import org.apache.commons.lang.StringUtils;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.IndexTrackingQueryObserver;
import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.Undefined;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.commands.DataCommands;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.domain.DataCommandResult.SelectResultRow;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.multistep.CLIMultiStepHelper;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.CompositeResultData.SectionResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.JsonUtil;
import org.apache.geode.pdx.PdxInstance;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.json.JSONArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @since GemFire 7.0
 */
public class DataCommandFunction extends FunctionAdapter implements InternalEntity {
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

  /**
   * Read only function
   */
  @Override
  public boolean optimizeForWrite() {
    return optimizeForWrite;
  }

  public void setOptimizeForWrite(boolean optimizeForWrite) {
    this.optimizeForWrite = optimizeForWrite;
  }

  @Override
  public void execute(FunctionContext functionContext) {
    try {
      InternalCache cache = getCache();
      DataCommandRequest request = (DataCommandRequest) functionContext.getArguments();
      if (logger.isDebugEnabled()) {
        logger.debug("Executing function : \n{}\n on member {}", request,
            System.getProperty("memberName"));
      }
      DataCommandResult result = null;
      if (request.isGet()) {
        result = get(request, cache.getSecurityService());
      } else if (request.isLocateEntry()) {
        result = locateEntry(request);
      } else if (request.isPut()) {
        result = put(request);
      } else if (request.isRemove()) {
        result = remove(request);
      } else if (request.isSelect()) {
        result = select(request);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Result is {}", result);
      }
      functionContext.getResultSender().lastResult(result);
    } catch (Exception e) {
      logger.info("Exception occurred:", e);
      functionContext.getResultSender().sendException(e);
    }
  }


  private InternalCache getCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  public DataCommandResult remove(DataCommandRequest request) {
    String key = request.getKey();
    String keyClass = request.getKeyClass();
    String regionName = request.getRegionName();
    String removeAllKeys = request.getRemoveAllKeys();
    return remove(key, keyClass, regionName, removeAllKeys);
  }

  public DataCommandResult get(DataCommandRequest request, SecurityService securityService) {
    String key = request.getKey();
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    Boolean loadOnCacheMiss = request.isLoadOnCacheMiss();
    return get(request.getPrincipal(), key, keyClass, valueClass, regionName, loadOnCacheMiss,
        securityService);
  }

  public DataCommandResult locateEntry(DataCommandRequest request) {
    String key = request.getKey();
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    boolean recursive = request.isRecursive();
    return locateEntry(key, keyClass, valueClass, regionName, recursive);
  }

  public DataCommandResult put(DataCommandRequest request) {
    String key = request.getKey();
    String value = request.getValue();
    boolean putIfAbsent = request.isPutIfAbsent();
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    return put(key, value, putIfAbsent, keyClass, valueClass, regionName);
  }

  public DataCommandResult select(DataCommandRequest request) {
    String query = request.getQuery();
    return select(request.getPrincipal(), query);
  }

  /**
   * To catch trace output
   */
  public static class WrappedIndexTrackingQueryObserver extends IndexTrackingQueryObserver {

    @Override
    public void reset() {
      // NOOP
    }

    public void reset2() {
      super.reset();
    }
  }

  @SuppressWarnings("rawtypes")
  private DataCommandResult select(Object principal, String queryString) {

    InternalCache cache = getCache();
    AtomicInteger nestedObjectCount = new AtomicInteger(0);
    if (StringUtils.isEmpty(queryString)) {
      return DataCommandResult.createSelectInfoResult(null, null, -1, null,
          CliStrings.QUERY__MSG__QUERY_EMPTY, false);
    }

    QueryService qs = cache.getQueryService();

    // TODO : Find out if is this optimised use. Can you have something equivalent of parsed
    // queries with names where name can be retrieved to avoid parsing every-time
    Query query = qs.newQuery(queryString);
    DefaultQuery tracedQuery = (DefaultQuery) query;
    WrappedIndexTrackingQueryObserver queryObserver = null;
    String queryVerboseMsg = null;
    long startTime = -1;
    if (tracedQuery.isTraced()) {
      startTime = NanoTimer.getTime();
      queryObserver = new WrappedIndexTrackingQueryObserver();
      QueryObserverHolder.setInstance(queryObserver);
    }
    List<SelectResultRow> list = new ArrayList<>();

    try {
      Object results = query.execute();
      if (tracedQuery.isTraced()) {
        queryVerboseMsg = getLogMessage(queryObserver, startTime, queryString);
        queryObserver.reset2();
      }
      if (results instanceof SelectResults) {
        select_SelectResults((SelectResults) results, principal, list, nestedObjectCount);
      } else {
        select_NonSelectResults(results, list);
      }
      return DataCommandResult.createSelectResult(queryString, list, queryVerboseMsg, null, null,
          true);

    } catch (FunctionDomainException | GfJsonException | QueryInvocationTargetException
        | NameResolutionException | TypeMismatchException e) {
      logger.warn(e.getMessage(), e);
      return DataCommandResult.createSelectResult(queryString, null, queryVerboseMsg, e,
          e.getMessage(), false);
    } finally {
      if (queryObserver != null) {
        QueryObserverHolder.reset();
      }
    }
  }

  private void select_NonSelectResults(Object results, List<SelectResultRow> list) {
    if (logger.isDebugEnabled()) {
      logger.debug("BeanResults : Bean Results class is {}", results.getClass());
    }
    String str = toJson(results);
    GfJsonObject jsonBean;
    try {
      jsonBean = new GfJsonObject(str);
    } catch (GfJsonException e) {
      logger.info("Exception occurred:", e);
      jsonBean = new GfJsonObject();
      try {
        jsonBean.put("msg", e.getMessage());
      } catch (GfJsonException e1) {
        logger.warn("Ignored GfJsonException:", e1);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("BeanResults : Adding bean json string : {}", jsonBean);
    }
    list.add(new SelectResultRow(DataCommandResult.ROW_TYPE_BEAN, jsonBean.toString()));
  }

  private void select_SelectResults(SelectResults selectResults, Object principal,
      List<SelectResultRow> list, AtomicInteger nestedObjectCount) throws GfJsonException {
    for (Object object : selectResults) {
      // Post processing
      object = getCache().getSecurityService().postProcess(principal, null, null, object, false);

      if (object instanceof Struct) {
        StructImpl impl = (StructImpl) object;
        GfJsonObject jsonStruct = getJSONForStruct(impl, nestedObjectCount);
        if (logger.isDebugEnabled()) {
          logger.debug("SelectResults : Adding select json string : {}", jsonStruct);
        }
        list.add(
            new SelectResultRow(DataCommandResult.ROW_TYPE_STRUCT_RESULT, jsonStruct.toString()));
      } else if (JsonUtil.isPrimitiveOrWrapper(object.getClass())) {
        if (logger.isDebugEnabled()) {
          logger.debug("SelectResults : Adding select primitive : {}", object);
        }
        list.add(new SelectResultRow(DataCommandResult.ROW_TYPE_PRIMITIVE, object));
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("SelectResults : Bean Results class is {}", object.getClass());
        }
        String str = toJson(object);
        GfJsonObject jsonBean;
        try {
          jsonBean = new GfJsonObject(str);
        } catch (GfJsonException e) {
          logger.error(e.getMessage(), e);
          jsonBean = new GfJsonObject();
          try {
            jsonBean.put("msg", e.getMessage());
          } catch (GfJsonException e1) {
            logger.warn("Ignored GfJsonException:", e1);
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug("SelectResults : Adding bean json string : {}", jsonBean);
        }
        list.add(new SelectResultRow(DataCommandResult.ROW_TYPE_BEAN, jsonBean.toString()));
      }
    }
  }

  private String toJson(Object object) {
    if (object instanceof Undefined) {
      return "{\"Value\":\"UNDEFINED\"}";
    } else if (object instanceof PdxInstance) {
      return pdxToJson((PdxInstance) object);
    } else {
      return JsonUtil.objectToJsonNestedChkCDep(object, NESTED_JSON_LENGTH);
    }
  }

  private GfJsonObject getJSONForStruct(StructImpl impl, AtomicInteger ai) throws GfJsonException {
    String fields[] = impl.getFieldNames();
    Object[] values = impl.getFieldValues();
    GfJsonObject jsonObject = new GfJsonObject();
    for (int i = 0; i < fields.length; i++) {
      Object value = values[i];
      if (value != null) {
        if (JsonUtil.isPrimitiveOrWrapper(value.getClass())) {
          jsonObject.put(fields[i], value);
        } else {
          jsonObject.put(fields[i], toJson(value));
        }
      } else {
        jsonObject.put(fields[i], "null");
      }
    }
    return jsonObject;
  }

  @SuppressWarnings({"rawtypes"})
  public DataCommandResult remove(String key, String keyClass, String regionName,
      String removeAllKeys) {

    InternalCache cache = getCache();

    if (StringUtils.isEmpty(regionName)) {
      return DataCommandResult.createRemoveResult(key, null, null,
          CliStrings.REMOVE__MSG__REGIONNAME_EMPTY, false);
    }

    if (StringUtils.isEmpty(removeAllKeys) && (key == null)) {
      return DataCommandResult.createRemoveResult(null, null, null,
          CliStrings.REMOVE__MSG__KEY_EMPTY, false);
    }

    Region region = cache.getRegion(regionName);
    if (region == null) {
      return DataCommandResult.createRemoveInfoResult(key, null, null,
          CliStrings.format(CliStrings.REMOVE__MSG__REGION_NOT_FOUND, regionName), false);
    } else {
      if (removeAllKeys == null) {
        Object keyObject;
        try {
          keyObject = getClassObject(key, keyClass);
        } catch (ClassNotFoundException e) {
          return DataCommandResult.createRemoveResult(key, null, null,
              "ClassNotFoundException " + keyClass, false);
        } catch (IllegalArgumentException e) {
          return DataCommandResult.createRemoveResult(key, null, null,
              "Error in converting JSON " + e.getMessage(), false);
        }

        if (region.containsKey(keyObject)) {
          Object value = region.remove(keyObject);
          if (logger.isDebugEnabled()) {
            logger.debug("Removed key {} successfully", key);
          }
          // return DataCommandResult.createRemoveResult(key, value, null, null);
          Object array[] = getJSONForNonPrimitiveObject(value);
          DataCommandResult result =
              DataCommandResult.createRemoveResult(key, array[1], null, null, true);
          if (array[0] != null) {
            result.setValueClass((String) array[0]);
          }
          return result;
        } else {
          return DataCommandResult.createRemoveInfoResult(key, null, null,
              CliStrings.REMOVE__MSG__KEY_NOT_FOUND_REGION, false);
        }
      } else {
        DataPolicy policy = region.getAttributes().getDataPolicy();
        if (!policy.withPartitioning()) {
          region.clear();
          if (logger.isDebugEnabled()) {
            logger.debug("Cleared all keys in the region - {}", regionName);
          }
          return DataCommandResult.createRemoveInfoResult(key, null, null,
              CliStrings.format(CliStrings.REMOVE__MSG__CLEARED_ALL_CLEARS, regionName), true);
        } else {
          return DataCommandResult.createRemoveInfoResult(key, null, null,
              CliStrings.REMOVE__MSG__CLEAREALL_NOT_SUPPORTED_FOR_PARTITIONREGION, false);
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  public DataCommandResult get(Object principal, String key, String keyClass, String valueClass,
      String regionName, Boolean loadOnCacheMiss, SecurityService securityService) {

    InternalCache cache = getCache();

    if (StringUtils.isEmpty(regionName)) {
      return DataCommandResult.createGetResult(key, null, null,
          CliStrings.GET__MSG__REGIONNAME_EMPTY, false);
    }

    if (StringUtils.isEmpty(key)) {
      return DataCommandResult.createGetResult(key, null, null, CliStrings.GET__MSG__KEY_EMPTY,
          false);
    }

    Region region = cache.getRegion(regionName);

    if (region == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Region Not Found - {}", regionName);
      }
      return DataCommandResult.createGetResult(key, null, null,
          CliStrings.format(CliStrings.GET__MSG__REGION_NOT_FOUND, regionName), false);
    } else {
      Object keyObject;
      try {
        keyObject = getClassObject(key, keyClass);
      } catch (ClassNotFoundException e) {
        return DataCommandResult.createGetResult(key, null, null,
            "ClassNotFoundException " + keyClass, false);
      } catch (IllegalArgumentException e) {
        return DataCommandResult.createGetResult(key, null, null,
            "Error in converting JSON " + e.getMessage(), false);
      }

      // TODO determine whether the following conditional logic (assigned to 'doGet') is safer or
      // necessary
      boolean doGet = Boolean.TRUE.equals(loadOnCacheMiss);

      if (doGet || region.containsKey(keyObject)) {
        Object value = region.get(keyObject);

        // run it through post processor. region.get will return the deserialized object already, so
        // we don't need to
        // deserialize it anymore to pass it to the postProcessor
        value = securityService.postProcess(principal, regionName, keyObject, value, false);

        if (logger.isDebugEnabled()) {
          logger.debug("Get for key {} value {}", key, value);
        }
        // return DataCommandResult.createGetResult(key, value, null, null);
        Object array[] = getJSONForNonPrimitiveObject(value);
        if (value != null) {
          DataCommandResult result =
              DataCommandResult.createGetResult(key, array[1], null, null, true);
          if (array[0] != null) {
            result.setValueClass((String) array[0]);
          }
          return result;
        } else {
          return DataCommandResult.createGetResult(key, array[1], null, null, false);
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Key is not present in the region {}", regionName);
        }
        return DataCommandResult.createGetInfoResult(key, null, null,
            CliStrings.GET__MSG__KEY_NOT_FOUND_REGION, false);
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public DataCommandResult locateEntry(String key, String keyClass, String valueClass,
      String regionPath, boolean recursive) {

    InternalCache cache = getCache();

    if (StringUtils.isEmpty(regionPath)) {
      return DataCommandResult.createLocateEntryResult(key, null, null,
          CliStrings.LOCATE_ENTRY__MSG__REGIONNAME_EMPTY, false);
    }

    if (StringUtils.isEmpty(key)) {
      return DataCommandResult.createLocateEntryResult(key, null, null,
          CliStrings.LOCATE_ENTRY__MSG__KEY_EMPTY, false);
    }
    List<Region> listOfRegionsStartingWithRegionPath = new ArrayList<>();

    if (recursive) {
      // Recursively find the keys starting from the specified region path.
      List<String> regionPaths = getAllRegionPaths(cache, true);
      for (String path : regionPaths) {
        if (path.startsWith(regionPath) || path.startsWith(Region.SEPARATOR + regionPath)) {
          Region targetRegion = cache.getRegion(path);
          listOfRegionsStartingWithRegionPath.add(targetRegion);
        }
      }
      if (listOfRegionsStartingWithRegionPath.size() == 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("Region Not Found - {}", regionPath);
        }
        return DataCommandResult.createLocateEntryResult(key, null, null,
            CliStrings.format(CliStrings.REMOVE__MSG__REGION_NOT_FOUND, regionPath), false);
      }
    } else {
      Region region = cache.getRegion(regionPath);
      if (region == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Region Not Found - {}", regionPath);
        }
        return DataCommandResult.createLocateEntryResult(key, null, null,
            CliStrings.format(CliStrings.REMOVE__MSG__REGION_NOT_FOUND, regionPath), false);
      } else {
        listOfRegionsStartingWithRegionPath.add(region);
      }
    }

    Object keyObject;
    try {
      keyObject = getClassObject(key, keyClass);
    } catch (ClassNotFoundException e) {
      logger.error(e.getMessage(), e);
      return DataCommandResult.createLocateEntryResult(key, null, null,
          "ClassNotFoundException " + keyClass, false);
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage(), e);
      return DataCommandResult.createLocateEntryResult(key, null, null,
          "Error in converting JSON " + e.getMessage(), false);
    }

    Object value;
    DataCommandResult.KeyInfo keyInfo;
    keyInfo = new DataCommandResult.KeyInfo();
    DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    keyInfo.setHost(member.getHost());
    keyInfo.setMemberId(member.getId());
    keyInfo.setMemberName(member.getName());

    for (Region region : listOfRegionsStartingWithRegionPath) {
      if (region instanceof PartitionedRegion) {
        // Following code is adaptation of which.java of old Gfsh
        PartitionedRegion pr = (PartitionedRegion) region;
        Region localRegion = PartitionRegionHelper.getLocalData(region);
        value = localRegion.get(keyObject);
        if (value != null) {
          DistributedMember primaryMember =
              PartitionRegionHelper.getPrimaryMemberForKey(region, keyObject);
          int bucketId = pr.getKeyInfo(keyObject).getBucketId();
          boolean isPrimary = member == primaryMember;
          keyInfo.addLocation(new Object[] {region.getFullPath(), true,
              getJSONForNonPrimitiveObject(value)[1], isPrimary, "" + bucketId});
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Key is not present in the region {}", regionPath);
          }
          return DataCommandResult.createLocateEntryInfoResult(key, null, null,
              CliStrings.LOCATE_ENTRY__MSG__KEY_NOT_FOUND_REGION, false);
        }
      } else {
        if (region.containsKey(keyObject)) {
          value = region.get(keyObject);
          if (logger.isDebugEnabled()) {
            logger.debug("Get for key {} value {} in region {}", key, value, region.getFullPath());
          }
          if (value != null) {
            keyInfo.addLocation(new Object[] {region.getFullPath(), true,
                getJSONForNonPrimitiveObject(value)[1], false, null});
          } else {
            keyInfo.addLocation(new Object[] {region.getFullPath(), false, null, false, null});
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Key is not present in the region {}", regionPath);
          }
          keyInfo.addLocation(new Object[] {region.getFullPath(), false, null, false, null});
        }
      }
    }

    if (keyInfo.hasLocation()) {
      return DataCommandResult.createLocateEntryResult(key, keyInfo, null, null, true);
    } else {
      return DataCommandResult.createLocateEntryInfoResult(key, null, null,
          CliStrings.LOCATE_ENTRY__MSG__KEY_NOT_FOUND_REGION, false);
    }
  }

  @SuppressWarnings({"rawtypes"})
  public DataCommandResult put(String key, String value, boolean putIfAbsent, String keyClass,
      String valueClass, String regionName) {

    if (StringUtils.isEmpty(regionName)) {
      return DataCommandResult.createPutResult(key, null, null,
          CliStrings.PUT__MSG__REGIONNAME_EMPTY, false);
    }

    if (StringUtils.isEmpty(key)) {
      return DataCommandResult.createPutResult(key, null, null, CliStrings.PUT__MSG__KEY_EMPTY,
          false);
    }

    if (StringUtils.isEmpty(value)) {
      return DataCommandResult.createPutResult(key, null, null, CliStrings.PUT__MSG__VALUE_EMPTY,
          false);
    }

    InternalCache cache = getCache();
    Region region = cache.getRegion(regionName);
    if (region == null) {
      return DataCommandResult.createPutResult(key, null, null,
          CliStrings.format(CliStrings.PUT__MSG__REGION_NOT_FOUND, regionName), false);
    } else {
      Object keyObject;
      Object valueObject;
      try {
        keyObject = getClassObject(key, keyClass);
      } catch (ClassNotFoundException e) {
        return DataCommandResult.createPutResult(key, null, null,
            "ClassNotFoundException " + keyClass, false);
      } catch (IllegalArgumentException e) {
        return DataCommandResult.createPutResult(key, null, null,
            "Error in converting JSON " + e.getMessage(), false);
      }

      try {
        valueObject = getClassObject(value, valueClass);
      } catch (ClassNotFoundException e) {
        return DataCommandResult.createPutResult(key, null, null,
            "ClassNotFoundException " + valueClass, false);
      }
      Object returnValue;
      if (putIfAbsent && region.containsKey(keyObject)) {
        returnValue = region.get(keyObject);
      } else {
        returnValue = region.put(keyObject, valueObject);
      }
      Object array[] = getJSONForNonPrimitiveObject(returnValue);
      DataCommandResult result = DataCommandResult.createPutResult(key, array[1], null, null, true);
      if (array[0] != null) {
        result.setValueClass((String) array[0]);
      }
      return result;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Object getClassObject(String string, String klassString)
      throws ClassNotFoundException, IllegalArgumentException {
    if (StringUtils.isEmpty(klassString)) {
      return string;
    }
    Class klass = ClassPathLoader.getLatest().forName(klassString);

    if (klass.equals(String.class)) {
      return string;
    }

    if (JsonUtil.isPrimitiveOrWrapper(klass)) {
      try {
        if (klass.equals(Byte.class)) {
          return Byte.parseByte(string);
        } else if (klass.equals(Short.class)) {
          return Short.parseShort(string);
        } else if (klass.equals(Integer.class)) {
          return Integer.parseInt(string);
        } else if (klass.equals(Long.class)) {
          return Long.parseLong(string);
        } else if (klass.equals(Double.class)) {
          return Double.parseDouble(string);
        } else if (klass.equals(Boolean.class)) {
          return Boolean.parseBoolean(string);
        } else if (klass.equals(Float.class)) {
          return Float.parseFloat(string);
        }
        return null;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Failed to convert input key to " + klassString + " Msg : " + e.getMessage());
      }
    }

    return getObjectFromJson(string, klass);
  }

  @SuppressWarnings({"rawtypes"})
  public static Object[] getJSONForNonPrimitiveObject(Object obj) {
    Object[] array = new Object[2];
    if (obj == null) {
      array[0] = null;
      array[1] = "<NULL>";
      return array;
    } else {
      array[0] = obj.getClass().getCanonicalName();
      Class klass = obj.getClass();
      if (JsonUtil.isPrimitiveOrWrapper(klass)) {
        array[1] = obj;
      } else if (obj instanceof PdxInstance) {
        String str = pdxToJson((PdxInstance) obj);
        array[1] = str;
      } else {
        GfJsonObject object = new GfJsonObject(obj, true);
        Iterator keysIterator = object.keys();
        while (keysIterator.hasNext()) {
          String key = (String) keysIterator.next();
          Object value = object.get(key);
          if (GfJsonObject.isJSONKind(value)) {
            GfJsonObject jsonVal = new GfJsonObject(value);
            // System.out.println("Re-wrote inner object");
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
          } else if (value instanceof JSONArray) {
            // Its a collection either a set or list
            try {
              object.put(key, "a Collection");
            } catch (GfJsonException e) {
              throw new RuntimeException(e);
            }
          }
        }
        String str = object.toString();
        array[1] = str;
      }
      return array;
    }
  }

  private static String pdxToJson(PdxInstance obj) {
    if (obj != null) {
      try {
        GfJsonObject json = new GfJsonObject();
        for (String field : obj.getFieldNames()) {
          Object fieldValue = obj.getField(field);
          if (fieldValue != null) {
            if (JsonUtil.isPrimitiveOrWrapper(fieldValue.getClass())) {
              json.put(field, fieldValue);
            } else {
              json.put(field, fieldValue.getClass());
            }
          }
        }
        return json.toString();
      } catch (GfJsonException e) {
        return null;
      }
    }
    return null;
  }

  public static <V> V getObjectFromJson(String json, Class<V> klass) {
    String newString = json.replaceAll("'", "\"");
    if (newString.charAt(0) == '(') {
      int len = newString.length();
      StringBuilder sb = new StringBuilder();
      sb.append("{").append(newString.substring(1, len - 1)).append("}");
      newString = sb.toString();
    }
    return JsonUtil.jsonToObject(newString, klass);
  }



  /**
   * Returns a sorted list of all region full paths found in the specified cache.
   * 
   * @param cache The cache to search.
   * @param recursive recursive search for sub-regions
   * @return Returns a sorted list of all region paths defined in the distributed system.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static List getAllRegionPaths(InternalCache cache, boolean recursive) {
    ArrayList list = new ArrayList();
    if (cache == null) {
      return list;
    }

    // get a list of all root regions
    Set<Region<?, ?>> regions = cache.rootRegions();

    for (Region rootRegion : regions) {
      String regionPath = rootRegion.getFullPath();

      Region region = cache.getRegion(regionPath);
      list.add(regionPath);
      Set<Region> subregionSet = region.subregions(true);
      if (recursive) {
        for (Region aSubregionSet : subregionSet) {
          list.add(aSubregionSet.getFullPath());
        }
      }
    }
    Collections.sort(list);
    return list;
  }

  static DataCommandResult cachedResult = null;

  public static class SelectDisplayStep extends CLIMultiStepHelper.LocalStep {

    public SelectDisplayStep(Object[] arguments) {
      super(SELECT_STEP_DISPLAY, arguments);
    }

    @Override
    public Result exec() {
      boolean interactive = (Boolean) commandArguments[2];
      GfJsonObject args = CLIMultiStepHelper.getStepArgs();
      int startCount = args.getInt(DataCommandResult.QUERY_PAGE_START);
      int endCount = args.getInt(DataCommandResult.QUERY_PAGE_END);
      int rows = args.getInt(DataCommandResult.NUM_ROWS); // returns Zero if no rows added so it
      // works.
      boolean flag = args.getBoolean(DataCommandResult.RESULT_FLAG);
      CommandResult commandResult = CLIMultiStepHelper.getDisplayResultFromArgs(args);
      Gfsh.println();
      while (commandResult.hasNextLine()) {
        Gfsh.println(commandResult.nextLine());
      }

      if (flag) {
        boolean paginationNeeded = startCount < rows && endCount < rows && interactive;
        if (paginationNeeded) {
          while (true) {
            String message = ("Press n to move to next page, q to quit and p to previous page : ");
            try {
              String step = Gfsh.getCurrentInstance().interact(message);
              if ("n".equals(step)) {
                int nextStart = startCount + getPageSize();
                return CLIMultiStepHelper.createBannerResult(
                    new String[] {DataCommandResult.QUERY_PAGE_START,
                        DataCommandResult.QUERY_PAGE_END,},
                    new Object[] {nextStart, (nextStart + getPageSize())}, SELECT_STEP_MOVE);
              } else if ("p".equals(step)) {
                int nextStart = startCount - getPageSize();
                if (nextStart < 0) {
                  nextStart = 0;
                }
                return CLIMultiStepHelper.createBannerResult(
                    new String[] {DataCommandResult.QUERY_PAGE_START,
                        DataCommandResult.QUERY_PAGE_END},
                    new Object[] {nextStart, (nextStart + getPageSize())}, SELECT_STEP_MOVE);
              } else if ("q".equals(step)) {
                return CLIMultiStepHelper.createBannerResult(new String[] {}, new Object[] {},
                    SELECT_STEP_END);
              } else {
                Gfsh.println("Unknown option ");
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
      return CLIMultiStepHelper.createBannerResult(new String[] {}, new Object[] {},
          SELECT_STEP_END);
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
  }

  public static class SelectExecStep extends CLIMultiStepHelper.RemoteStep {
    private static final Logger logger = LogService.getLogger();

    private static final long serialVersionUID = 1L;

    public SelectExecStep(Object[] arguments) {
      super(DataCommandFunction.SELECT_STEP_EXEC, arguments);
    }

    @Override
    public Result exec() {
      String remainingQuery = (String) commandArguments[0];
      boolean interactive = (Boolean) commandArguments[2];
      DataCommandResult result = _select(remainingQuery);
      int endCount = 0;
      DataCommandFunction.cachedResult = result;
      if (interactive) {
        endCount = DataCommandFunction.getPageSize();
      } else {
        if (result.getSelectResult() != null) {
          endCount = result.getSelectResult().size();
        }
      }
      if (interactive) {
        return result.pageResult(0, endCount, DataCommandFunction.SELECT_STEP_DISPLAY);
      } else {
        return CLIMultiStepHelper.createBannerResult(new String[] {}, new Object[] {},
            DataCommandFunction.SELECT_STEP_END);
      }
    }

    public DataCommandResult _select(String query) {
      InternalCache cache = (InternalCache) CacheFactory.getAnyInstance();
      DataCommandResult dataResult;

      if (StringUtils.isEmpty(query)) {
        dataResult = DataCommandResult.createSelectInfoResult(null, null, -1, null,
            CliStrings.QUERY__MSG__QUERY_EMPTY, false);
        return dataResult;
      }

      Object array[] = DataCommands.replaceGfshEnvVar(query, CommandExecutionContext.getShellEnv());
      query = (String) array[1];
      query = addLimit(query);

      @SuppressWarnings("deprecation")
      QCompiler compiler = new QCompiler();
      Set<String> regionsInQuery;
      try {
        CompiledValue compiledQuery = compiler.compileQuery(query);
        Set<String> regions = new HashSet<>();
        compiledQuery.getRegionsInQuery(regions, null);

        // authorize data read on these regions
        for (String region : regions) {
          cache.getSecurityService().authorizeRegionRead(region);
        }

        regionsInQuery = Collections.unmodifiableSet(regions);
        if (regionsInQuery.size() > 0) {
          Set<DistributedMember> members =
              DataCommands.getQueryRegionsAssociatedMembers(regionsInQuery, cache, false);
          if (members != null && members.size() > 0) {
            DataCommandFunction function = new DataCommandFunction();
            DataCommandRequest request = new DataCommandRequest();
            request.setCommand(CliStrings.QUERY);
            request.setQuery(query);
            Subject subject = cache.getSecurityService().getSubject();
            if (subject != null) {
              request.setPrincipal(subject.getPrincipal());
            }
            dataResult = DataCommands.callFunctionForRegion(request, function, members);
            dataResult.setInputQuery(query);
            return dataResult;
          } else {
            return DataCommandResult.createSelectInfoResult(null, null, -1, null, CliStrings.format(
                CliStrings.QUERY__MSG__REGIONS_NOT_FOUND, regionsInQuery.toString()), false);
          }
        } else {
          return DataCommandResult.createSelectInfoResult(null, null, -1, null,
              CliStrings.format(CliStrings.QUERY__MSG__INVALID_QUERY,
                  "Region mentioned in query probably missing /"),
              false);
        }
      } catch (QueryInvalidException qe) {
        logger.error("{} Failed Error {}", query, qe.getMessage(), qe);
        return DataCommandResult.createSelectInfoResult(null, null, -1, null,
            CliStrings.format(CliStrings.QUERY__MSG__INVALID_QUERY, qe.getMessage()), false);
      }
    }

    private String addLimit(String query) {
      if (StringUtils.containsIgnoreCase(query, " limit")
          || StringUtils.containsIgnoreCase(query, " count(")) {
        return query;
      }
      return query + " limit " + DataCommandFunction.getFetchSize();
    }
  }

  public static class SelectQuitStep extends CLIMultiStepHelper.RemoteStep {

    public SelectQuitStep(Object[] arguments) {
      super(SELECT_STEP_END, arguments);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public Result exec() {
      boolean interactive = (Boolean) commandArguments[2];
      GfJsonObject args = CLIMultiStepHelper.getStepArgs();
      DataCommandResult dataResult = cachedResult;
      cachedResult = null;
      if (interactive) {
        return CLIMultiStepHelper.createEmptyResult("END");
      } else {
        CompositeResultData rd = dataResult.toSelectCommandResult();
        SectionResultData section = rd.addSection(CLIMultiStepHelper.STEP_SECTION);
        section.addData(CLIMultiStepHelper.NEXT_STEP_NAME, "END");
        return ResultBuilder.buildResult(rd);
      }
    }
  }

  public static int getPageSize() {
    int pageSize = -1;
    Map<String, String> session;
    if (CliUtil.isGfshVM()) {
      session = Gfsh.getCurrentInstance().getEnv();
    } else {
      session = CommandExecutionContext.getShellEnv();
    }
    if (session != null) {
      String size = session.get(Gfsh.ENV_APP_COLLECTION_LIMIT);
      if (StringUtils.isEmpty(size)) {
        pageSize = Gfsh.DEFAULT_APP_COLLECTION_LIMIT;
      } else {
        pageSize = Integer.parseInt(size);
      }
    }
    if (pageSize == -1) {
      pageSize = Gfsh.DEFAULT_APP_COLLECTION_LIMIT;
    }
    return pageSize;
  }

  static int getFetchSize() {
    return CommandExecutionContext.getShellFetchSize();
  }

  public static String getLogMessage(QueryObserver observer, long startTime, String query) {
    String usedIndexesString = null;
    float time = 0.0f;

    if (startTime > 0L) {
      time = (NanoTimer.getTime() - startTime) / 1.0e6f;
    }

    if (observer != null && observer instanceof IndexTrackingQueryObserver) {
      IndexTrackingQueryObserver indexObserver = (IndexTrackingQueryObserver) observer;
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
          buf.append(entry.getKey().toString()).append(entry.getValue());
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

    return String.format("Query Executed%s%s", startTime > 0L ? " in " + time + " ms;" : ";",
        usedIndexesString != null ? usedIndexesString : "");
  }

}
