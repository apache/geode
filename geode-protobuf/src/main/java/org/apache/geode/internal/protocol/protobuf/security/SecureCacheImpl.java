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
package org.apache.geode.internal.protocol.protobuf.security;

import static org.apache.geode.security.ResourcePermission.ALL;
import static org.apache.geode.security.ResourcePermission.Operation.READ;
import static org.apache.geode.security.ResourcePermission.Operation.WRITE;
import static org.apache.geode.security.ResourcePermission.Resource.DATA;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.ResultsCollectionWrapper;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class SecureCacheImpl implements SecureCache {
  protected final InternalCache cache;
  protected final Security security;
  private final SecureFunctionService functionService;

  public SecureCacheImpl(InternalCache cache, Security security) {
    this.cache = cache;
    this.security = security;
    functionService = new SecureFunctionServiceImpl(cache, security);
  }

  @Override
  public <K, V> void getAll(String regionName, Iterable<K> keys, BiConsumer<K, V> successConsumer,
      BiConsumer<K, Exception> failureConsumer) {
    Region<K, V> region = getRegion(regionName);

    boolean authorized = tryAuthorizeAllKeys(DATA, READ, regionName);

    keys.forEach(key -> {
      try {
        if (!authorized) {
          security.authorize(DATA, READ, regionName, key);
        }
        V value = region.get(key);
        value = postProcess(regionName, key, value);
        successConsumer.accept(key, value);
      } catch (Exception e) {
        failureConsumer.accept(key, e);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private <K, V> V postProcess(String regionName, K key, V value) {
    return (V) security.postProcess(regionName, key, value);
  }

  @Override
  public <K, V> V get(String regionName, K key) {
    security.authorize(DATA, READ, regionName, key);
    Region<K, V> region = getRegion(regionName);
    V value = region.get(key);
    return postProcess(regionName, key, value);
  }

  @Override
  public <K, V> void put(String regionName, K key, V value) {
    security.authorize(DATA, WRITE, regionName, key);
    Region<K, V> region = getRegion(regionName);
    region.put(key, value);
  }

  @Override
  public <K, V> void putAll(String regionName, Map<K, V> entries,
      BiConsumer<K, Exception> failureConsumer) {
    // TODO - this is doing a very inefficient put for each key

    boolean authorized = tryAuthorizeAllKeys(DATA, WRITE, regionName);

    Region<K, V> region = getRegion(regionName);
    entries.forEach((key, value) -> {
      try {
        if (!authorized) {
          security.authorize(DATA, WRITE, regionName, key);
        }
        region.put(key, value);
      } catch (Exception e) {
        failureConsumer.accept(key, e);
      }
    });
  }

  @Override
  public <K, V> V remove(String regionName, K key) {
    security.authorize(DATA, WRITE, regionName, key);
    Region<K, V> region = getRegion(regionName);
    V oldValue = region.remove(key);
    return postProcess(regionName, key, oldValue);
  }

  @Override
  public Collection<String> getRegionNames() {
    security.authorize(DATA, READ, ALL, ALL);
    Set<String> regionNames = new HashSet<>();

    cache.rootRegions().forEach(region -> {
      regionNames.add(region.getFullPath());
      region.subregions(true).stream().map(Region::getFullPath).forEach(regionNames::add);
    });

    return regionNames;
  }

  @Override
  public int getSize(String regionName) {
    security.authorize(DATA, READ, regionName, ALL);
    return getRegion(regionName).size();
  }

  @Override
  public <K> Set<K> keySet(String regionName) {
    security.authorize(DATA, READ, regionName, ALL);
    final Region<K, Object> region = getRegion(regionName);
    return region.keySet();
  }

  @Override
  public SecureFunctionService getFunctionService() {
    return functionService;
  }

  @Override
  public void clear(String regionName) {
    security.authorize(DATA, WRITE, regionName, ALL);
    Region<?, ?> region = getRegion(regionName);
    region.clear();
  }

  @Override
  public <K, V> V putIfAbsent(String regionName, K key, V value) {
    security.authorize(DATA, WRITE, regionName, key);
    Region<K, V> region = getRegion(regionName);
    V oldValue = region.putIfAbsent(key, value);

    return postProcess(regionName, key, oldValue);
  }

  @Override
  public Object query(String queryString, Object[] bindParameters) throws NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {

    InternalQueryService queryService = (InternalQueryService) cache.getQueryService();

    Query query = queryService.newQuery(queryString);

    for (String regionName : ((DefaultQuery) query).getRegionsInQuery(bindParameters)) {
      security.authorize(DATA, READ, regionName, ALL);
    }

    Object result = query.execute(bindParameters);

    if (security.needsPostProcessing()) {
      return postProcessQueryResults(result);
    } else {
      return result;
    }
  }

  private Object postProcessQueryResults(Object value) {
    // The result is a single value
    if (!(value instanceof SelectResults)) {
      // For query results, we don't have the region or the key
      return security.postProcess(null, null, value);
    }

    SelectResults<?> selectResults = (SelectResults<?>) value;

    // The result is a list of objects
    if (!selectResults.getCollectionType().getElementType().isStructType()) {
      List<Object> postProcessed = selectResults.stream()
          .map(element -> security.postProcess(null, null, element)).collect(Collectors.toList());
      return new ResultsCollectionWrapper(selectResults.getCollectionType().getElementType(),
          postProcessed);
    }

    // The result is a list of structs
    @SuppressWarnings("unchecked")
    SelectResults<Struct> structResults = (SelectResults<Struct>) selectResults;

    List<Struct> postProcessed =
        structResults.stream().map(this::postProcessStruct).collect(Collectors.toList());


    return new ResultsCollectionWrapper(selectResults.getCollectionType().getElementType(),
        postProcessed);
  }

  private Struct postProcessStruct(Struct struct) {

    return new StructImpl((StructTypeImpl) struct.getStructType(),
        Arrays.stream(struct.getFieldValues())
            .map(element -> security.postProcess(null, null, element)).toArray());
  }

  private <K, V> Region<K, V> getRegion(String regionName) {
    Region<K, V> region = cache.getRegion(regionName);
    if (region == null) {
      throw new RegionDestroyedException("Region not found " + regionName, regionName);
    }
    return region;
  }

  /**
   * Try to authorize the user for all keys.
   *
   * @return true if the user is authorized for all keys
   */
  private boolean tryAuthorizeAllKeys(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, String regionName) {
    try {
      security.authorize(resource, operation, regionName, ALL);
      return true;
    } catch (NotAuthorizedException e) {
      return false;
    }
  }
}
