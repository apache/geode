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
package org.apache.geode.cache.query.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;

/**
 * @since GemFire 4.0
 */
public class QRegion implements SelectResults {

  private final Region region;
  /**
   * values is an unmodifiable facade on the region values.
   */
  private final SelectResults values;

  /** Creates a new instance of QRegion */
  public QRegion(Region region, boolean includeKeys) {
    if (region == null) {
      throw new IllegalArgumentException(
          "Region can not be NULL");
    }
    this.region = region;
    Class constraint = this.region.getAttributes().getValueConstraint();
    if (constraint == null) {
      constraint = Object.class;
    }
    ResultsCollectionWrapper res = null;
    if (includeKeys) {
      res =
          new ResultsCollectionWrapper(TypeUtils.getObjectType(constraint), this.region.entrySet());
    } else {
      res = new ResultsCollectionWrapper(TypeUtils.getObjectType(constraint), this.region.values());
    }
    res.setModifiable(false);
    if (!DefaultQueryService.COPY_ON_READ_AT_ENTRY_LEVEL) {
      res.setIgnoreCopyOnReadForQuery(true);
    }
    values = res;
  }

  public QRegion(Region region, boolean includeKeys, ExecutionContext context) {

    if (region == null) {
      throw new IllegalArgumentException(
          "Region can not be NULL");
    }

    Class constraint = region.getAttributes().getValueConstraint();
    if (constraint == null) {
      constraint = Object.class;
    }

    ResultsCollectionWrapper res = null;
    if (context.getBucketList() != null && region instanceof PartitionedRegion) {
      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      LocalDataSet localData =
          new LocalDataSet(partitionedRegion, new HashSet(context.getBucketList()));
      this.region = localData;
      if (includeKeys) {
        res = new ResultsCollectionWrapper(TypeUtils.getObjectType(constraint),
            localData.localEntrySet());
      } else {
        res = new ResultsCollectionWrapper(TypeUtils.getObjectType(constraint),
            localData.localValues());
      }
    } else {
      this.region = region;
      if (includeKeys) {
        res = new ResultsCollectionWrapper(TypeUtils.getObjectType(constraint), region.entrySet());
      } else {
        res = new ResultsCollectionWrapper(TypeUtils.getObjectType(constraint), region.values());
      }
    }
    res.setModifiable(false);
    if (!DefaultQueryService.COPY_ON_READ_AT_ENTRY_LEVEL) {
      res.setIgnoreCopyOnReadForQuery(true);
    }
    values = res;
  }

  public Region getRegion() {
    return region;
  }


  public void setKeepSerialized(boolean keepSerialized) {
    ((ResultsCollectionWrapper) (values)).setKeepSerialized(keepSerialized);
  }

  protected ObjectType getKeyType() {
    Class constraint = region.getAttributes().getKeyConstraint();
    if (constraint == null) {
      constraint = Object.class;
    }
    return TypeUtils.getObjectType(constraint);
  }

  @Override
  public void setElementType(ObjectType elementType) {
    values.setElementType(elementType);
  }

  /**
   * Returns unmodifiable SelectResults for keys. When the "keys" attribute is accessed, this is the
   * preferred method that will be executed.
   */
  public SelectResults getKeys() {
    ResultsCollectionWrapper res;
    if (region instanceof LocalDataSet) {
      LocalDataSet localData = (LocalDataSet) region;
      res = new ResultsCollectionWrapper(getKeyType(), localData.localKeys());
    } else {
      res = new ResultsCollectionWrapper(getKeyType(), region.keySet());
    }
    res.setModifiable(false);
    return res;
  }

  /**
   * Accessing the values is the same as accessing this. Must return Collection to satisfy Region
   * interface
   */
  public Collection values() {
    return this;
  }

  /**
   * getValues is the same as values except returns a SelectResults. This method is invoked when
   * access the "values" attribute in a query.
   */
  public SelectResults getValues() {
    return this;
  }

  /**
   * Returns the entries as an unmodifiable SelectResults. This is the preferred method that is
   * invoked when accessing the attribute "entries".
   */
  public SelectResults getEntries() {
    ResultsCollectionWrapper res;
    if (region instanceof LocalDataSet) {
      LocalDataSet localData = (LocalDataSet) region;
      res = new ResultsCollectionWrapper(TypeUtils.getRegionEntryType(region),
          localData.localEntrySet());
    } else {
      res = new ResultsCollectionWrapper(TypeUtils.getRegionEntryType(region),
          region.entrySet(false));
    }
    res.setModifiable(false);
    return res;
  }

  //////////// SelectResults methods ////////////////////////

  public boolean isOrdered() {
    return false;
  }

  /**
   * Getter for property modifiable. Since a QRegion as a collection represents an unmodifiable
   * facade on the values of this region, return false.
   *
   * @return Value of property modifiable.
   */
  @Override
  public boolean isModifiable() {
    return false;
  }

  @Override
  public int occurrences(Object element) {
    // expensive!!
    int count = 0;
    for (Object v : values) {
      if (element == null ? v == null : element.equals(v)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public List asList() {
    return new ArrayList(values);
  }

  @Override
  public Set asSet() {
    return new HashSet(values);
  }

  @Override
  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(QRegion.class, values.getCollectionType().getElementType());
  }

  //////////// Set methods ///////////////////////////////////
  @Override
  public boolean add(Object obj) {
    throw new UnsupportedOperationException(
        "Region values is not modifiable");
  }

  @Override
  public boolean addAll(Collection collection) {
    throw new UnsupportedOperationException(
        "Region values is not modifiable");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException(
        "Region values is not modifiable");
  }

  // to be sure, we need to iterate to make sure the region isn't full of
  // invalid values
  // @todo in the future calling this.region.values().isEmpty()
  // might be more efficient, but it isn't at the time this was written
  @Override
  public boolean isEmpty() {
    return values.isEmpty();
  }

  @Override
  public boolean contains(Object obj) {
    return values.contains(obj);
  }

  @Override
  public boolean containsAll(Collection collection) {
    return values.containsAll(collection);
  }

  @Override
  public Iterator iterator() {
    return values.iterator();
  }

  public void becomeLockGrantor() {
    // no need to do anything
  }

  @Override
  public boolean remove(Object obj) {
    throw new UnsupportedOperationException(
        "Region values is not modifiable");
  }

  @Override
  public boolean removeAll(Collection collection) {
    throw new UnsupportedOperationException(
        "Region values is not modifiable");
  }

  @Override
  public boolean retainAll(Collection collection) {
    throw new UnsupportedOperationException(
        "Region values is not modifiable");
  }

  @Override
  public int size() {
    // must call getValuesSet() to get the correct size -- expensive.
    // this size must reflect the dropping of null values (invalid entries),
    // (but NOT the dropping of duplicates)
    return values.size();
  }

  @Override
  public Object[] toArray() {
    return values.toArray();
  }

  @Override
  public Object[] toArray(Object[] obj) {
    return values.toArray(obj);
  }

  ///////////// Region methods ////////////////////////////////
  public boolean containsKey(Object key) {
    return region.containsKey(key);
  }

  public boolean containsValueForKey(Object key) {
    return region.containsValueForKey(key);
  }

  public void create(Object key, Object value)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    region.create(key, value);
  }

  public void create(Object key, Object value, Object aCacheWriterParam)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    region.create(key, value, aCacheWriterParam);
  }

  public Region createSubregion(String subregionName, RegionAttributes aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    return region.createSubregion(subregionName, aRegionAttributes);
  }

  public void destroy(Object key)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    region.destroy(key);
  }

  public void destroy(Object key, Object aCacheWriterParam)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    region.destroy(key, aCacheWriterParam);
  }

  public void destroyRegion() throws CacheWriterException, TimeoutException {
    region.destroyRegion();
  }

  public void destroyRegion(Object aCallbackArgument)
      throws CacheWriterException, TimeoutException {
    region.destroyRegion(aCallbackArgument);
  }

  public Set entrySet(boolean recursive) {
    return region.entrySet(recursive);
  }

  public boolean existsValue(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return region.existsValue(queryPredicate);
  }

  public Object get(Object key) throws CacheLoaderException, TimeoutException {
    return region.get(key);
  }

  public Object get(Object key, Object aCallbackArgument)
      throws TimeoutException, CacheLoaderException {
    return region.get(key, aCallbackArgument);
  }

  public RegionAttributes getAttributes() {
    return region.getAttributes();
  }

  public AttributesMutator getAttributesMutator() {
    return region.getAttributesMutator();
  }

  public Cache getCache() {
    return region.getCache();
  }

  public Lock getDistributedLock(Object key) throws IllegalStateException {
    return region.getDistributedLock(key);
  }

  public Region.Entry getEntry(Object key) {
    return region.getEntry(key);
  }

  public String getFullPath() {
    return region.getFullPath();
  }

  public String getName() {
    return region.getName();
  }

  public Region getParentRegion() {
    return region.getParentRegion();
  }

  public Lock getRegionDistributedLock() throws IllegalStateException {
    return region.getRegionDistributedLock();
  }

  public CacheStatistics getStatistics() {
    return region.getStatistics();
  }

  public Region getSubregion(String path) {
    return region.getSubregion(path);
  }

  public Object getUserAttribute() {
    return region.getUserAttribute();
  }

  public void invalidate(Object key) throws TimeoutException, EntryNotFoundException {
    region.invalidate(key);
  }

  public void invalidate(Object key, Object aCallbackArgument)
      throws TimeoutException, EntryNotFoundException {
    region.invalidate(key, aCallbackArgument);
  }

  public void invalidateRegion() throws TimeoutException {
    region.invalidateRegion();
  }

  public void invalidateRegion(Object aCallbackArgument) throws TimeoutException {
    region.invalidateRegion(aCallbackArgument);
  }

  public boolean isDestroyed() {
    return region.isDestroyed();
  }

  public Set keys() {
    return region.keySet();
  }

  public void localDestroy(Object key) throws EntryNotFoundException {
    region.localDestroy(key);
  }

  public void localDestroy(Object key, Object aCallbackArgument) throws EntryNotFoundException {
    region.localDestroy(key, aCallbackArgument);
  }

  public void localDestroyRegion() {
    region.localDestroyRegion();
  }

  public void localDestroyRegion(Object aCallbackArgument) {
    region.localDestroyRegion(aCallbackArgument);
  }

  public void close() {
    region.close();
  }

  public void localInvalidate(Object key) throws EntryNotFoundException {
    region.localInvalidate(key);
  }

  public void localInvalidate(Object key, Object aCallbackArgument) throws EntryNotFoundException {
    region.localInvalidate(key, aCallbackArgument);
  }

  public void localInvalidateRegion() {
    region.localInvalidateRegion();
  }

  public void localInvalidateRegion(Object aCallbackArgument) {
    region.localInvalidateRegion(aCallbackArgument);
  }

  public void put(Object key, Object value) throws TimeoutException, CacheWriterException {
    region.put(key, value);
  }

  public void put(Object key, Object value, Object aCallbackArgument)
      throws TimeoutException, CacheWriterException {
    region.put(key, value, aCallbackArgument);
  }

  public SelectResults query(String predicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return region.query(predicate);
  }

  public Object selectValue(String predicate) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    return region.selectValue(predicate);
  }

  public void setUserAttribute(Object value) {
    region.setUserAttribute(value);
  }

  public Set subregions(boolean recursive) {
    // return this.region.subregions(recursive);
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public void loadSnapshot(InputStream inputStream)
      throws IOException, ClassNotFoundException, CacheWriterException, TimeoutException {
    region.loadSnapshot(inputStream);
  }

  public void saveSnapshot(OutputStream outputStream) throws IOException {
    region.saveSnapshot(outputStream);
  }

  public void registerInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public void registerInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public void registerInterest(Object key, InterestResultPolicy policy)
      throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public void registerInterestRegex(String regex, InterestResultPolicy policy)
      throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public void unregisterInterest(Object key) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public void unregisterInterestRegex(String regex) throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public List getInterestList() throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  public List getInterestListRegex() throws CacheWriterException {
    throw new UnsupportedOperationException(
        "Unsupported at this time");
  }

  // Object methods
  @Override
  public String toString() {
    return region.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SelectResults)) {
      return false;
    }
    return values.equals(obj);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }

  public boolean containsValue(Object arg0) {
    return region.containsValue(arg0);
  }

  public void putAll(Map arg0) {
    region.putAll(arg0);

  }

  public SelectResults entrySet() {
    ResultsCollectionWrapper res = new ResultsCollectionWrapper(new ObjectTypeImpl(Map.Entry.class),
        region.entrySet(false));
    res.setModifiable(false);
    return res;
  }

  public Set keySet() {
    return region.keySet();
  }

}
