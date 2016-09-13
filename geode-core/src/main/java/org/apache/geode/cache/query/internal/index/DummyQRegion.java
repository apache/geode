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
/*
 * DummyQRegion.java
 *
 * Created on March 15, 2005, 6:40 PM
 */

package com.gemstone.gemfire.cache.query.internal.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.QRegion;
import com.gemstone.gemfire.cache.query.internal.ResultsBag;
import com.gemstone.gemfire.cache.query.internal.ResultsSet;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;

/**
 *
 */
public class DummyQRegion extends QRegion {
  
  private RegionEntry entry = null;
  private ObjectType valueType = TypeUtils.OBJECT_TYPE;
  private ObjectType keyType = TypeUtils.OBJECT_TYPE;
  
  private ResultsBag values = null;
  private ResultsSet keys = null;
  private ResultsSet entries = null;
  private List valueInList = null;
  private Object[] valueInArray = null;
  
  public DummyQRegion(Region region) {
    super(region, false);
    Class constraint = region.getAttributes().getValueConstraint();
    if (constraint != null)
      valueType = TypeUtils.getObjectType(constraint);
    
    constraint = region.getAttributes().getKeyConstraint();
    if (constraint != null)
      keyType = TypeUtils.getObjectType(constraint);
    values = new ResultsBag(((GemFireCacheImpl)region.getCache()).getCachePerfStats());
    values.setElementType(valueType);
    keys = new ResultsSet();
    keys.setElementType(keyType);
    entries = new ResultsSet();
    entries.setElementType(TypeUtils.getRegionEntryType(region)); // gets key and value types from region
  }
  
  @Override
  public boolean equals(Object o) { //  for findbugs
    return super.equals(o);
  }
  
  @Override
  public int hashCode() { // for findbugs
    return super.hashCode();
  }
  
  public void setEntry(RegionEntry e){
    this.entry = e;
  }
  
  public RegionEntry getEntry(){
    return this.entry;
  }
  
  @Override
  public SelectResults getKeys() {
    if(keys == null){
      keys = new ResultsSet();
      keys.setElementType(keyType);
    }
    keys.clear();
    keys.add(entry.getKey());
    return keys;
  }
  
  @Override
  public Set keySet() {
    return (ResultsSet)getKeys();
  }
  
  @Override
  public Set keys() {
    return keySet();
  }
  
  @Override
  public Collection values() {
   return getValues();
  }
  
  @Override
  public Set asSet() {
    return getValues().asSet(); 
  }
  
  @Override
  public List asList() {
    if(valueInList == null){
      valueInList = new  ArrayList(1);      
    }
    valueInList.clear();
    Object val = this.entry.getValueOffHeapOrDiskWithoutFaultIn((LocalRegion) getRegion());
    if (val instanceof StoredObject) {
      @Retained @Released StoredObject ohval = (StoredObject) val;
      try {
        val = ohval.getDeserializedValue(getRegion(), this.entry);
      } finally {
        ohval.release();
      }
    } else if (val instanceof CachedDeserializable) {
      val = ((CachedDeserializable)val).getDeserializedValue(getRegion(), this.entry);
    } 
    valueInList.add(val);
    return valueInList;
  }
  
  @Override
  public Object[] toArray() {
    if(valueInArray == null){
      valueInArray = new  Object[1];      
    }   
    Object val = this.entry.getValueOffHeapOrDiskWithoutFaultIn((LocalRegion) getRegion());
    if (val instanceof StoredObject) {      
      @Retained @Released StoredObject ohval = (StoredObject) val;
      try {
        val = ohval.getDeserializedValue(getRegion(), this.entry);
      } finally {
        ohval.release();
      }
    } else if (val instanceof CachedDeserializable) {
      val = ((CachedDeserializable)val).getDeserializedValue(getRegion(), this.entry);
    } 
    valueInArray[0] = val;
    return valueInArray;
  }
  
  @Override
  public SelectResults getValues() {
    if(values == null){
      values = new ResultsBag(((GemFireCacheImpl)getRegion().getCache()).getCachePerfStats());
      values.setElementType(valueType);
    }
    values.clear();
    Object val = this.entry.getValueOffHeapOrDiskWithoutFaultIn((LocalRegion) getRegion());
    if (val instanceof StoredObject) {
      @Retained @Released StoredObject ohval = (StoredObject) val;
      try {
        val = ohval.getDeserializedValue(getRegion(), this.entry);
      } finally {
        ohval.release();
      }
    } else if (val instanceof CachedDeserializable) {
      val = ((CachedDeserializable)val).getDeserializedValue(getRegion(), this.entry);
    } 
    values.add(val);
    return values;
  }
  
  @Override
  public SelectResults getEntries() {
    if(entries == null){
      entries = new ResultsSet();
      entries.setElementType(TypeUtils.getRegionEntryType(getRegion()));
    }
    entries.clear();
    // return collection of Region.Entry, not (dotless) RegionEntry
    Region rgn = getRegion();
    // unwrap until we get the LocalRegion
    while (!(rgn instanceof LocalRegion)) {
      rgn = ((QRegion)TypeUtils.checkCast(rgn, QRegion.class)).getRegion();
    }
    entries.add(((LocalRegion)rgn).new NonTXEntry(entry));
    return entries;
  }
  
  @Override
  public SelectResults entrySet() {
    return getEntries();
  }
  
  @Override
  public Set entries(boolean recursive) {
    return (ResultsSet)getEntries();
  }
  
  @Override
  public Region.Entry getEntry(Object key) {
    LocalRegion.NonTXEntry e =(LocalRegion.NonTXEntry)super.getEntry(key);
    Region.Entry retVal = null;
    if(e != null &&  this.entry == e.getRegionEntry()) {
        retVal = e;
     } 
    return retVal;
  }
  
  @Override
  public Iterator iterator() {
    return values().iterator();
  }
  
  @Override
  public int size() {
    return 1;
  } 
  
  @Override
  public Object[] toArray(Object[] obj) {
    throw new RuntimeException(LocalizedStrings.DummyQRegion_NOT_YET_IMPLEMENTED.toLocalizedString());
  }
  
  @Override
  public String toString(){
    return "DQR "+super.toString();
  }
}
