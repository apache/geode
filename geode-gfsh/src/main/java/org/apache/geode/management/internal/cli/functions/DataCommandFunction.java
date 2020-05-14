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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.IndexTrackingQueryObserver;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.domain.DataCommandResult.SelectResultRow;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.JsonUtil;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.util.internal.GeodeJsonMapper;

/**
 * @since GemFire 7.0
 */
public class DataCommandFunction implements InternalFunction<DataCommandRequest> {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;

  private boolean optimizeForWrite = false;

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
  public void execute(FunctionContext<DataCommandRequest> functionContext) {
    try {
      InternalCache cache =
          ((InternalCache) functionContext.getCache()).getCacheForProcessingClientRequests();
      DataCommandRequest request = functionContext.getArguments();
      if (logger.isDebugEnabled()) {
        logger.debug("Executing function : \n{}\n on member {}", request,
            System.getProperty("memberName"));
      }
      DataCommandResult result = null;
      if (request.isGet()) {
        result = get(request, cache);
      } else if (request.isLocateEntry()) {
        result = locateEntry(request, cache);
      } else if (request.isPut()) {
        result = put(request, cache);
      } else if (request.isRemove()) {
        result = remove(request, cache);
      } else if (request.isSelect()) {
        result = select(request, cache);
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

  public DataCommandResult remove(DataCommandRequest request, InternalCache cache) {
    String key = request.getKey();
    String keyClass = request.getKeyClass();
    String regionName = request.getRegionName();
    String removeAllKeys = request.getRemoveAllKeys();
    return remove(key, keyClass, regionName, removeAllKeys, cache);
  }

  public DataCommandResult get(DataCommandRequest request, InternalCache cache) {
    String key = request.getKey();
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    Boolean loadOnCacheMiss = request.isLoadOnCacheMiss();
    return get(request.getPrincipal(), key, keyClass, valueClass, regionName, loadOnCacheMiss,
        cache);
  }

  public DataCommandResult locateEntry(DataCommandRequest request, InternalCache cache) {
    String key = request.getKey();
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    boolean recursive = request.isRecursive();
    return locateEntry(key, keyClass, valueClass, regionName, recursive, cache);
  }

  public DataCommandResult put(DataCommandRequest request, InternalCache cache) {
    String key = request.getKey();
    String value = request.getValue();
    boolean putIfAbsent = request.isPutIfAbsent();
    String keyClass = request.getKeyClass();
    String valueClass = request.getValueClass();
    String regionName = request.getRegionName();
    return put(key, value, putIfAbsent, keyClass, valueClass, regionName, cache);
  }

  public DataCommandResult select(DataCommandRequest request, InternalCache cache) {
    String query = request.getQuery();
    return select(cache, request.getPrincipal(), query);
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
  private DataCommandResult select(InternalCache cache, Object principal, String queryString) {

    if (StringUtils.isEmpty(queryString)) {
      return DataCommandResult.createSelectInfoResult(null, null, -1, null,
          CliStrings.QUERY__MSG__QUERY_EMPTY, false);
    }

    QueryService qs = cache.getQueryService();

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
        select_SelectResults((SelectResults) results, principal, list, cache);
      } else {
        select_NonSelectResults(results, list);
      }
      return DataCommandResult.createSelectResult(queryString, list, queryVerboseMsg, null, null,
          true);

    } catch (FunctionDomainException | QueryInvocationTargetException | NameResolutionException
        | TypeMismatchException e) {
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
    list.add(createSelectResultRow(results));
  }

  private void select_SelectResults(SelectResults selectResults, Object principal,
      List<SelectResultRow> list, InternalCache cache) {
    for (Object object : selectResults) {
      // Post processing
      object = cache.getSecurityService().postProcess(principal, null, null, object, false);

      list.add(createSelectResultRow(object));
    }
  }

  private SelectResultRow createSelectResultRow(Object object) {
    int rowType;
    if (object instanceof Struct) {
      rowType = DataCommandResult.ROW_TYPE_STRUCT_RESULT;
    } else if (JsonUtil.isPrimitiveOrWrapper(object.getClass())) {
      rowType = DataCommandResult.ROW_TYPE_PRIMITIVE;
    } else {
      rowType = DataCommandResult.ROW_TYPE_BEAN;
    }

    return new SelectResultRow(rowType, object);
  }

  @SuppressWarnings({"rawtypes"})
  public DataCommandResult remove(String key, String keyClass, String regionName,
      String removeAllKeys, InternalCache cache) {

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
          Object[] array = getClassAndJson(value);
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
              CliStrings.REMOVE__MSG__CLEARALL_NOT_SUPPORTED_FOR_PARTITIONREGION, false);
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  public DataCommandResult get(Object principal, String key, String keyClass, String valueClass,
      String regionName, Boolean loadOnCacheMiss, InternalCache cache) {

    SecurityService securityService = cache.getSecurityService();

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
        Object[] array = getClassAndJson(value);
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
        return DataCommandResult.createGetInfoResult(key, getClassAndJson(null)[1], null,
            CliStrings.GET__MSG__KEY_NOT_FOUND_REGION, false);
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public DataCommandResult locateEntry(String key, String keyClass, String valueClass,
      String regionPath, boolean recursive, InternalCache cache) {

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
          keyInfo.addLocation(new Object[] {region.getFullPath(), true, getClassAndJson(value)[1],
              isPrimary, "" + bucketId});
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
            keyInfo.addLocation(
                new Object[] {region.getFullPath(), true, getClassAndJson(value)[1], false, null});
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

  public DataCommandResult put(String key, String value, boolean putIfAbsent, String keyClass,
      String valueClass, String regionName, InternalCache cache) {

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

    Region<Object, Object> region = cache.getRegion(regionName);
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
      Object[] array = getClassAndJson(returnValue);
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

    Object resultObject;
    try {
      ObjectMapper mapper = GeodeJsonMapper.getMapper();
      resultObject = mapper.readValue(string, klass);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to convert input key to " + klassString + " Msg : " + e.getMessage());
    }

    return resultObject;
  }

  private Object[] getClassAndJson(Object obj) {
    Object[] array = new Object[2];

    if (obj == null) {
      array[0] = null;
      array[1] = null;
      return array;
    }

    array[0] = obj.getClass().getCanonicalName();

    if (obj instanceof PdxInstance) {
      array[1] = JSONFormatter.toJSON((PdxInstance) obj);
      return array;
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      array[1] = mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      array[1] = e.getMessage();
    }

    return array;
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

  public static String getLogMessage(QueryObserver observer, long startTime, String query) {
    String usedIndexesString = null;
    float time = 0.0f;

    if (startTime > 0L) {
      time = (NanoTimer.getTime() - startTime) / 1.0e6f;
    }

    if (observer instanceof IndexTrackingQueryObserver) {
      IndexTrackingQueryObserver indexObserver = (IndexTrackingQueryObserver) observer;
      @SuppressWarnings("unchecked")
      Map<Object, Object> usedIndexes = indexObserver.getUsedIndexes();
      indexObserver.reset();
      StringBuilder buf = new StringBuilder();
      buf.append(" indexesUsed(");
      buf.append(usedIndexes.size());
      buf.append(")");
      if (usedIndexes.size() > 0) {
        buf.append(":");
        for (Iterator<Map.Entry<Object, Object>> itr = usedIndexes.entrySet().iterator(); itr
            .hasNext();) {
          Map.Entry<Object, Object> entry = itr.next();
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
