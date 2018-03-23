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
package org.apache.geode.internal.protocol.protobuf.v1.authentication;

import static org.apache.geode.security.ResourcePermission.ALL;
import static org.apache.geode.security.ResourcePermission.Operation.READ;
import static org.apache.geode.security.ResourcePermission.Operation.WRITE;
import static org.apache.geode.security.ResourcePermission.Resource.DATA;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class AuthorizingCacheImpl implements AuthorizingCache {
  protected final InternalCache cache;
  protected final Authorizer authorizer;
  private final AuthorizingFunctionService functionService;

  public AuthorizingCacheImpl(InternalCache cache, Authorizer authorizer) {
    this.cache = cache;
    this.authorizer = authorizer;
    this.functionService = new AuthorizingFunctionServiceImpl(cache, authorizer);
  }

  @Override
  public <K, V> void getAll(String regionName, Iterable<K> keys, BiConsumer<K, V> successConsumer,
      BiConsumer<K, Exception> failureConsumer) {
    Region<K, V> region = getRegion(regionName);

    boolean authorized = tryAuthorizeAllKeys(DATA, READ, regionName);

    keys.forEach(key -> {
      try {
        if (!authorized) {
          authorizer.authorize(DATA, READ, regionName, key);
        }
        V value = (V) region.get(key);
        successConsumer.accept(key, value);
      } catch (Exception e) {
        failureConsumer.accept(key, e);
      }
    });
  }

  @Override
  public <K, V> V get(String regionName, K key) {
    authorizer.authorize(DATA, READ, regionName, key);
    Region<K, V> region = getRegion(regionName);
    return region.get(key);
  }

  @Override
  public <K, V> void put(String regionName, K key, V value) {
    authorizer.authorize(DATA, WRITE, regionName, key);
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
          authorizer.authorize(DATA, WRITE, regionName, key);
        }
        region.put(key, value);
      } catch (Exception e) {
        failureConsumer.accept(key, e);
      }
    });
  }

  @Override
  public <K, V> V remove(String regionName, K key) {
    authorizer.authorize(DATA, WRITE, regionName, key);
    Region<K, V> region = getRegion(regionName);
    return region.remove(key);
  }

  @Override
  public Collection<String> getRegionNames() {
    authorizer.authorize(DATA, READ, ALL, ALL);
    Set<String> regionNames = new HashSet<>();

    cache.rootRegions().forEach(region -> {
      regionNames.add(region.getFullPath());
      region.subregions(true).stream().map(Region::getFullPath).forEach(regionNames::add);
    });

    return regionNames;
  }

  @Override
  public int getSize(String regionName) {
    authorizer.authorize(DATA, READ, regionName, ALL);
    return getRegion(regionName).size();
  }

  @Override
  public <K> Set<K> keySet(String regionName) {
    authorizer.authorize(DATA, READ, regionName, ALL);
    return ((Region<K, ?>) getRegion(regionName)).keySet();
  }

  @Override
  public AuthorizingFunctionService getFunctionService() {
    return functionService;
  }

  @Override
  public void clear(String regionName) {
    authorizer.authorize(DATA, WRITE, regionName, ALL);
    Region<?, ?> region = getRegion(regionName);
    region.clear();
  }

  @Override
  public <K, V> V putIfAbsent(String regionName, K key, V value) {
    authorizer.authorize(DATA, WRITE, regionName, key);
    Region<K, V> region = getRegion(regionName);
    return region.putIfAbsent(key, value);
  }

  @Override
  public Object query(String queryString, Object[] bindParameters) throws NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {

    InternalQueryService queryService = cache.getQueryService();

    Query query = queryService.newQuery(queryString);

    for (String regionName : ((DefaultQuery) query).getRegionsInQuery(bindParameters)) {
      authorizer.authorize(DATA, READ, regionName, ALL);
    }

    return query.execute(bindParameters);
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
      authorizer.authorize(resource, operation, regionName, ALL);
      return true;
    } catch (NotAuthorizedException e) {
      return false;
    }
  }
}
