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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.extension.ExtensionPoint;
import com.gemstone.gemfire.internal.cache.extension.SimpleExtensionPoint;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link AbstractRegion}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
@Category(UnitTest.class)
public class AbstractRegionJUnitTest {

  /**
   * Test method for {@link AbstractRegion#getExtensionPoint()}.
   * 
   * Assert that method returns a {@link SimpleExtensionPoint} instance and
   * assume that {@link com.gemstone.gemfire.internal.cache.extension.SimpleExtensionPointJUnitTest} has covered the rest.
   * 
   */
  @Test
  public void testGetExtensionPoint() {
    // final Cache cache = new MockCache();
    final AbstractRegion region = new MockRegion(null, 0, false, 0, 0);
    final ExtensionPoint<Region<?, ?>> extensionPoint = region.getExtensionPoint();
    assertNotNull(extensionPoint);
    assertEquals(extensionPoint.getClass(), SimpleExtensionPoint.class);
  }

  @SuppressWarnings("rawtypes")
  private static class MockRegion extends AbstractRegion {

    /**
     * @see AbstractRegion#AbstractRegion(GemFireCacheImpl, int, boolean, long,
     *      long)
     */
    @SuppressWarnings("deprecation")
    private MockRegion(GemFireCacheImpl cache, int serialNumber, boolean isPdxTypeRegion, long lastAccessedTime, long lastModifiedTime) {
      super(cache, serialNumber, isPdxTypeRegion, lastAccessedTime, lastModifiedTime);
    }

    @Override
    public String getName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getFullPath() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Region getParentRegion() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RegionAttributes getAttributes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public AttributesMutator getAttributesMutator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void invalidateRegion(Object aCallbackArgument) throws TimeoutException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void localInvalidateRegion(Object aCallbackArgument) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void destroyRegion(Object aCallbackArgument) throws CacheWriterException, TimeoutException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void localDestroyRegion(Object aCallbackArgument) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void saveSnapshot(OutputStream outputStream) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void loadSnapshot(InputStream inputStream) throws IOException, ClassNotFoundException, CacheWriterException, TimeoutException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Region getSubregion(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Region createSubregion(String subregionName, RegionAttributes aRegionAttributes) throws RegionExistsException, TimeoutException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set subregions(boolean recursive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Entry getEntry(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object put(Object key, Object value, Object aCallbackArgument) throws TimeoutException, CacheWriterException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void create(Object key, Object value, Object aCallbackArgument) throws TimeoutException, EntryExistsException, CacheWriterException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void invalidate(Object key, Object aCallbackArgument) throws TimeoutException, EntryNotFoundException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void localInvalidate(Object key, Object aCallbackArgument) throws EntryNotFoundException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object destroy(Object key, Object aCallbackArgument) throws TimeoutException, EntryNotFoundException, CacheWriterException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void localDestroy(Object key, Object aCallbackArgument) throws EntryNotFoundException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set keys() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set keySet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection values() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set entries(boolean recursive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getUserAttribute() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setUserAttribute(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDestroyed() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValueForKey(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Lock getRegionDistributedLock() throws IllegalStateException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Lock getDistributedLock(Object key) throws IllegalStateException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeToDisk() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectResults query(String queryPredicate) throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void forceRolling() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set entrySet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map map) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map map, Object aCallbackArgument) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Collection keys) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(Collection keys, Object aCallbackArgument) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerInterest(Object key) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterest(Object key, InterestResultPolicy policy) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterestRegex(String regex) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterestRegex(String regex, InterestResultPolicy policy) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void unregisterInterest(Object key) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void unregisterInterestRegex(String regex) {
      throw new UnsupportedOperationException();

    }

    @Override
    public List getInterestList() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerInterest(Object key, boolean isDurable) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterest(Object key, boolean isDurable, boolean receiveValues) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable, boolean receiveValues) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterestRegex(String regex, boolean isDurable) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterestRegex(String regex, boolean isDurable, boolean receiveValues) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable) {
      throw new UnsupportedOperationException();

    }

    @Override
    public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable, boolean receiveValues) {
      throw new UnsupportedOperationException();

    }

    @Override
    public List getInterestListRegex() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set keySetOnServer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKeyOnServer(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object replace(Object key, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getDiskDirSizes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Version[] getSerializationVersions() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CachePerfStats getCachePerfStats() {
      throw new UnsupportedOperationException();
    }

    @Override
    Object get(Object key, Object aCallbackArgument, boolean generateCallbacks, EntryEventImpl clientEvent) throws TimeoutException, CacheLoaderException {
      throw new UnsupportedOperationException();
    }

    @Override
    void basicClear(RegionEventImpl regionEvent) {
      throw new UnsupportedOperationException();
    }

    @Override
    boolean generateEventID() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected DistributedMember getMyId() {
      throw new UnsupportedOperationException();
    }

    @Override
    void basicLocalClear(RegionEventImpl rEvent) {
      throw new UnsupportedOperationException();
    }

    @Override
    Map basicGetAll(Collection keys, Object callback) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RegionEntry basicGetEntry(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isCurrentlyLockGrantor() {
      throw new UnsupportedOperationException();
    }

    @Override
    public File[] getDiskDirs() {
      throw new UnsupportedOperationException();
    }

    @Override
    void checkReadiness() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsConcurrencyChecks() {
      throw new UnsupportedOperationException();
    }
  }
}
