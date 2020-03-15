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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.MapEntry.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.ResultsBag;
import org.apache.geode.cache.query.internal.StructBag;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class SecureCacheImplTest {

  public static final String REGION = "TestRegion";
  private SecureCacheImpl authorizingCache;
  private InternalCache cache;
  private Security security;
  private Region<String, Object> region;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    cache = mock(InternalCacheForClientAccess.class);
    doReturn(cache).when(cache).getCacheForProcessingClientRequests();
    region = mock(Region.class);
    when(cache.<String, Object>getRegion(REGION)).thenReturn(region);
    security = mock(Security.class);
    doThrow(NotAuthorizedException.class).when(security).authorize(any());
    doThrow(NotAuthorizedException.class).when(security).authorize(any(), any(), any(), any());
    when(security.postProcess(any(), any(), any())).then(invocation -> invocation.getArgument(2));
    when(security.needsPostProcessing()).thenReturn(true);
    authorizingCache = new SecureCacheImpl(cache, security);
  }

  @Test
  public void getAllSuccesses() {
    authorize(DATA, READ, REGION, "a");
    authorize(DATA, READ, REGION, "b");
    Map<Object, Object> okValues = new HashMap<>();
    Map<Object, Exception> exceptionValues = new HashMap<>();

    when(region.get("b")).thenReturn("existing value");

    authorizingCache.getAll(REGION, Arrays.asList("a", "b"), okValues::put, exceptionValues::put);

    verify(region).get("a");
    verify(region).get("b");
    assertThat(okValues).containsOnly(entry("a", null), entry("b", "existing value"));
    assertThat(exceptionValues).isEmpty();
  }

  @Test
  public void getAllWithRegionLevelAuthorizationSucceeds() {
    authorize(DATA, READ, REGION, ALL);
    Map<Object, Object> okValues = new HashMap<>();
    Map<Object, Exception> exceptionValues = new HashMap<>();

    when(region.get("b")).thenReturn("existing value");

    authorizingCache.getAll(REGION, Arrays.asList("a", "b"), okValues::put, exceptionValues::put);

    verify(region).get("a");
    verify(region).get("b");
    assertThat(okValues).containsOnly(entry("a", null), entry("b", "existing value"));
    assertThat(exceptionValues).isEmpty();
  }

  @Test
  public void getAllWithFailure() {
    authorize(DATA, READ, REGION, "b");
    Map<Object, Object> okValues = new HashMap<>();
    Map<Object, Exception> exceptionValues = new HashMap<>();

    when(region.get("b")).thenReturn("existing value");

    authorizingCache.getAll(REGION, Arrays.asList("a", "b"), okValues::put, exceptionValues::put);

    verify(region).get("b");
    verifyNoMoreInteractions(region);
    assertThat(okValues).containsOnly(entry("b", "existing value"));
    assertThat(exceptionValues).containsOnlyKeys("a");
    assertThat(exceptionValues.values().iterator().next())
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getAllIsPostProcessed() {
    authorize(DATA, READ, REGION, "a");
    authorize(DATA, READ, REGION, "b");
    when(security.postProcess(any(), any(), any())).thenReturn("spam");
    Map<Object, Object> okValues = new HashMap<>();
    Map<Object, Exception> exceptionValues = new HashMap<>();

    when(region.get("b")).thenReturn("existing value");

    authorizingCache.getAll(REGION, Arrays.asList("a", "b"), okValues::put, exceptionValues::put);

    verify(region).get("a");
    verify(region).get("b");
    assertThat(okValues).containsOnly(entry("a", "spam"), entry("b", "spam"));
    assertThat(exceptionValues).isEmpty();
  }

  private void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, String region, String key) {
    doNothing().when(security).authorize(resource, operation, region, key);
  }

  @Test
  public void get() {
    authorize(DATA, READ, REGION, "a");
    when(region.get("a")).thenReturn("value");
    assertThat(authorizingCache.<String, String>get(REGION, "a")).isEqualTo("value");
  }

  @Test
  public void getWithFailure() {
    assertThatThrownBy(() -> authorizingCache.get(REGION, "a"))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getIsPostProcessed() {
    authorize(DATA, READ, REGION, "a");
    when(security.postProcess(REGION, "a", "value")).thenReturn("spam");
    when(region.get("a")).thenReturn("value");
    assertThat(authorizingCache.<String, String>get(REGION, "a")).isEqualTo("spam");
  }

  @Test
  public void put() {
    authorize(DATA, WRITE, REGION, "a");
    authorizingCache.put(REGION, "a", "value");
    verify(region).put("a", "value");
  }

  @Test
  public void putWithFailure() {
    assertThatThrownBy(() -> authorizingCache.put(REGION, "a", "value"))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void putAll() {
    authorize(DATA, WRITE, REGION, "a");
    authorize(DATA, WRITE, REGION, "c");
    Map<Object, Object> entries = new HashMap<>();
    entries.put("a", "b");
    entries.put("c", "d");

    Map<Object, Exception> exceptionValues = new HashMap<>();

    authorizingCache.putAll(REGION, entries, exceptionValues::put);

    verify(region).put("a", "b");
    verify(region).put("c", "d");
    assertThat(exceptionValues).isEmpty();
  }

  @Test
  public void putAllWithRegionLevelAuthorizationSucceeds() {
    authorize(DATA, WRITE, REGION, ALL);
    Map<Object, Object> entries = new HashMap<>();
    entries.put("a", "b");
    entries.put("c", "d");

    Map<Object, Exception> exceptionValues = new HashMap<>();

    authorizingCache.putAll(REGION, entries, exceptionValues::put);

    verify(region).put("a", "b");
    verify(region).put("c", "d");
    assertThat(exceptionValues).isEmpty();
  }

  @Test
  public void putAllWithFailure() {
    authorize(DATA, WRITE, REGION, "a");
    Map<Object, Object> entries = new HashMap<>();
    entries.put("a", "b");
    entries.put("c", "d");

    Map<Object, Exception> exceptionValues = new HashMap<>();

    authorizingCache.putAll(REGION, entries, exceptionValues::put);

    verify(security).authorize(DATA, WRITE, REGION, "a");
    verify(security).authorize(DATA, WRITE, REGION, "c");
    verify(region).put("a", "b");
    verifyNoMoreInteractions(region);
    assertThat(exceptionValues).containsOnlyKeys("c");
  }

  @Test
  public void remove() {
    authorize(DATA, WRITE, REGION, "a");
    authorizingCache.remove(REGION, "a");
    verify(region).remove("a");
  }

  @Test
  public void removeWithoutAuthorization() {
    assertThatThrownBy(() -> authorizingCache.remove(REGION, "a"))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void removeIsPostProcessed() {
    authorize(DATA, WRITE, REGION, "a");
    when(region.remove("a")).thenReturn("value");
    when(security.postProcess(REGION, "a", "value")).thenReturn("spam");
    Object value = authorizingCache.remove(REGION, "a");
    verify(region).remove("a");
    assertThat(value).isEqualTo("spam");
  }

  @Test
  public void getRegionNames() {
    authorize(DATA, READ, ALL, ALL);
    Set<Region<?, ?>> regions = new HashSet<>();
    regions.add(region);
    when(cache.rootRegions()).thenReturn(regions);

    Set<Region<?, ?>> subregions = new HashSet<>();
    @SuppressWarnings("unchecked")
    Region<Object, Object> region2 = mock(Region.class);
    subregions.add(region2);
    @SuppressWarnings("unchecked")
    Region<Object, Object> region3 = mock(Region.class);
    subregions.add(region3);
    when(region.getFullPath()).thenReturn("region1");
    when(region2.getFullPath()).thenReturn("region2");
    when(region3.getFullPath()).thenReturn("region3");
    when(region.subregions(true)).thenReturn(subregions);
    Collection<String> regionNames = authorizingCache.getRegionNames();
    assertThat(regionNames).containsExactly("region1", "region2", "region3");

    verify(cache).rootRegions();
  }

  @Test
  public void getRegionNamesWithoutAuthorization() {
    assertThatThrownBy(() -> authorizingCache.getRegionNames())
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void getSize() {
    authorize(DATA, READ, REGION, ALL);
    authorizingCache.getSize(REGION);
    verify(region).size();
  }

  @Test
  public void getSizeWithoutAuthorization() {
    assertThatThrownBy(() -> authorizingCache.getSize(REGION))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void keySet() {
    authorize(DATA, READ, REGION, ALL);
    authorizingCache.keySet(REGION);
    verify(region).keySet();
  }

  @Test
  public void keySetWithoutAuthorization() {
    assertThatThrownBy(() -> authorizingCache.keySet(REGION))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void clear() {
    authorize(DATA, WRITE, REGION, ALL);
    authorizingCache.clear(REGION);
    verify(region).clear();
  }

  @Test
  public void clearWithoutAuthorization() {
    assertThatThrownBy(() -> authorizingCache.clear(REGION))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void putIfAbsent() {
    authorize(DATA, WRITE, REGION, "a");
    authorizingCache.putIfAbsent(REGION, "a", "b");
    verify(region).putIfAbsent("a", "b");
  }

  @Test
  public void putIfAbsentIsPostProcessed() {
    authorize(DATA, WRITE, REGION, "a");
    when(region.putIfAbsent(any(), any())).thenReturn("value");
    when(security.postProcess(REGION, "a", "value")).thenReturn("spam");
    String oldValue = authorizingCache.putIfAbsent(REGION, "a", "b");
    verify(region).putIfAbsent("a", "b");
    assertThat(oldValue).isEqualTo("spam");
  }

  @Test
  public void putIfAbsentWithoutAuthorization() {
    assertThatThrownBy(() -> authorizingCache.putIfAbsent(REGION, "a", "b"))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void query() throws Exception {
    authorize(DATA, READ, REGION, ALL);
    mockQuery();
    String queryString = "select * from /region";
    Object[] bindParameters = {"a"};
    authorizingCache.query(queryString, bindParameters);
  }

  @Test
  public void queryIsPostProcessedWithSingleResultValue() throws Exception {
    authorize(DATA, READ, REGION, ALL);
    when(security.postProcess(any(), any(), any())).thenReturn("spam");
    DefaultQuery query = mockQuery();
    String queryString = "select * from /region";
    Object[] bindParameters = {"a"};
    when(query.execute(bindParameters)).thenReturn("value");
    Object result = authorizingCache.query(queryString, bindParameters);
    assertThat(result).isEqualTo("spam");
  }

  @Test
  public void queryIsPostProcessedWithListOfObjectValues() throws Exception {
    authorize(DATA, READ, REGION, ALL);
    when(security.postProcess(any(), any(), any())).thenReturn("spam");
    DefaultQuery query = mockQuery();
    String queryString = "select * from /region";
    Object[] bindParameters = {"a"};

    @SuppressWarnings("unchecked")
    SelectResults<String> results = new ResultsBag();
    results.setElementType(new ObjectTypeImpl(Object.class));
    results.add("value1");
    results.add("value2");
    when(query.execute((Object[]) any())).thenReturn(results);
    @SuppressWarnings("unchecked")
    SelectResults<String> result =
        (SelectResults<String>) authorizingCache.query(queryString, bindParameters);
    assertThat(result.asList()).containsExactly("spam", "spam");
  }

  @Test
  public void queryIsPostProcessedWithListOfStructValues() throws Exception {
    authorize(DATA, READ, REGION, ALL);
    when(security.postProcess(any(), any(), any())).thenReturn("spam");

    SelectResults<Struct> results = buildListOfStructs("value1", "value2");
    DefaultQuery query = mockQuery();
    String queryString = "select * from /region";
    Object[] bindParameters = {"a"};
    when(query.execute((Object[]) any())).thenReturn(results);
    @SuppressWarnings("unchecked")
    SelectResults<Struct> result =
        (SelectResults<Struct>) authorizingCache.query(queryString, bindParameters);
    assertThat(result.asList())
        .containsExactlyInAnyOrderElementsOf(buildListOfStructs("spam", "spam").asList());
  }

  private SelectResults<Struct> buildListOfStructs(String... values) {
    @SuppressWarnings("unchecked")
    SelectResults<Struct> results = new StructBag();
    StructTypeImpl elementType = new StructTypeImpl(new String[] {"field1"});
    results.setElementType(elementType);
    for (String value : values) {
      results.add(new StructImpl(elementType, new Object[] {value}));
    }
    return results;
  }

  private DefaultQuery mockQuery() {
    InternalQueryService queryService = mock(InternalQueryService.class);
    when(cache.getQueryService()).thenReturn(queryService);
    DefaultQuery query = mock(DefaultQuery.class);
    when(queryService.newQuery(any())).thenReturn(query);
    when(query.getRegionsInQuery(any())).thenReturn(Collections.singleton(REGION));
    return query;
  }

}
