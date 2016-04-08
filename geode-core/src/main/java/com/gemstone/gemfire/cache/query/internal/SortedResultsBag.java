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
package com.gemstone.gemfire.cache.query.internal;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.gemstone.gemfire.cache.query.internal.types.CollectionTypeImpl;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.cache.CachePerfStats;

/**
 * This results set is used to sort the data allowing duplicates. If the data
 * being added is already sorted, than the underlying Map is a LinkedHashMap ,
 * else a TreeMap is created. For index results expecting duplicates, the
 * constructor used is the one which creates LinkedHashMap
 * This class is used to store ordered data of Structs too, obtained from index lookup
 * 
 *
 * @param <E>
 */
public class SortedResultsBag<E> extends Bag implements Ordered {

  private final Map<E, Integer> sortedMap;
  private final boolean orderedDataAddition;
  private final boolean emitNullAtStart; 

  /**
   * Constructor for unordered input
   * 
   * @param comparator
   * @param nullAtStart Indicates that the first order by coumn is asc , so that tuple with null 
   * order by column value need to be emitted at start, else if desc, then emit at last
   */
  public SortedResultsBag(Comparator<E> comparator, boolean nullAtStart) {
    super();
    this.sortedMap = new TreeMap<E, Integer>(comparator);
    this.emitNullAtStart = nullAtStart;
    this.orderedDataAddition = false;
  }
  
  /**
   * Constructor for unordered input
   * @param comparator
   * @param elementType
   * @param nullAtStart Indicates that the first order by coumn is asc , so that tuple with null 
   * order by column value need to be emitted at start, else if desc, then emit at last
   */
  public SortedResultsBag(Comparator<E> comparator,
      ObjectType elementType, boolean nullAtStart) {
    super();
    this.sortedMap = new TreeMap<E, Integer>(comparator);
    this.setElementType(elementType);
    this.emitNullAtStart = nullAtStart;
    this.orderedDataAddition = false;
  }

  /**
   * Constructor for unordered input
   * 
   * @param comparator
   * @param elementType
   * @param stats
   * @param nullAtStart Indicates that the first order by coumn is asc , so that tuple with null 
   * order by column value need to be emitted at start, else if desc, then emit at last
   */
  public SortedResultsBag(Comparator<E> comparator, ObjectType elementType,
      CachePerfStats stats, boolean nullAtStart) {
    super(elementType, stats);
    this.sortedMap = new TreeMap<E, Integer>(comparator);
    this.emitNullAtStart = nullAtStart;
    this.orderedDataAddition = false;
  }

  /**
   * Constructor for unordered input
   * 
   * @param comparator
   * @param stats
   * @param nullAtStart Indicates that the first order by coumn is asc , so that tuple with null 
   * order by column value need to be emitted at start, else if desc, then emit at last
   */
  public SortedResultsBag(Comparator<E> comparator, CachePerfStats stats, boolean nullAtStart) {
    super(stats);
    this.sortedMap = new TreeMap<E, Integer>(comparator);
    this.emitNullAtStart = nullAtStart;
    this.orderedDataAddition = false;
  }

  /**
   * Constructor for ordered input. Creates underlying Map as LinkedHashMap
   * 
   * @param stats
   * @param nullAtStart Indicates that the first order by coumn is asc , so that tuple with null 
   * order by column value need to be emitted at start, else if desc, then emit at last
   */
  public SortedResultsBag(CachePerfStats stats, boolean nullAtStart) {
    super(stats);
    this.sortedMap = new LinkedHashMap<E, Integer>();
    this.orderedDataAddition = true;
    this.emitNullAtStart = nullAtStart;
  }

  /**
   * Constructor for ordered input. Creates underlying Map as LinkedHashMap
   * @param nullAtStart Indicates that the first order by coumn is asc , so that tuple with null 
   * order by column value need to be emitted at start, else if desc, then emit at last
   */
  public SortedResultsBag(boolean nullAtStart) {
    super();
    this.sortedMap = new LinkedHashMap<E, Integer>();
    this.orderedDataAddition = true;
    this.emitNullAtStart = nullAtStart;
  }

  /**
   * Constructor for ordered input. Creates underlying Map as LinkedHashMap
   * 
   * @param elementType
   * @param nullAtStart Indicates that the first order by coumn is asc , so that tuple with null 
   * order by column value need to be emitted at start, else if desc, then emit at last
   */
  public SortedResultsBag(ObjectType elementType, boolean nullAtStart) {
    super();
    this.sortedMap = new LinkedHashMap<E, Integer>();
    this.orderedDataAddition = true;
    this.setElementType(elementType);
    this.emitNullAtStart = nullAtStart;
  }

  @Override
  public boolean isModifiable() {
    return false;
  }

  @Override
  protected int mapGet(Object element) {
    Integer count = this.sortedMap.get(element);
    if (count == null) {
      return 0;
    } else {
      return count;
    }
  }

  @Override
  protected boolean mapContainsKey(Object element) {
    return this.sortedMap.containsKey(element);
  }

  @Override
  protected void mapPut(Object element, int count) {
    this.sortedMap.put((E) element, count);
  }

  @Override
  protected int mapSize() {
    return this.sortedMap.size();
  }

  @Override
  protected int mapRemove(Object element) {
    Integer count = this.sortedMap.remove(element);
    if (count == null) {
      return 0;
    } else {
      return count;
    }
  }

  @Override
  protected void mapClear() {
    this.sortedMap.clear();
  }

  @Override
  protected Object getMap() {
    return this.sortedMap;
  }

  @Override
  protected int mapHashCode() {
    return this.sortedMap.hashCode();
  }

  @Override
  protected boolean mapEmpty() {
    return this.sortedMap.isEmpty();
  }

  @Override
  protected Iterator mapEntryIterator() {
    return this.sortedMap.entrySet().iterator();
  }

  @Override
  protected Iterator mapKeyIterator() {
    return this.sortedMap.keySet().iterator();
  }

  @Override
  protected Object keyFromEntry(Object entry) {
    Map.Entry<E, Integer> mapEntry = (Map.Entry<E, Integer>) entry;
    return mapEntry.getKey();
  }

  @Override
  protected Integer valueFromEntry(Object entry) {
    Map.Entry<E, Integer> mapEntry = (Map.Entry<E, Integer>) entry;
    return mapEntry.getValue();
  }

  @Override
  public CollectionType getCollectionType() {
    return new CollectionTypeImpl(SortedResultsBag.class, this.elementType);
  }

  @Override
  public Comparator comparator() {
    return this.orderedDataAddition ? null : ((SortedMap) this.sortedMap)
        .comparator();
  }
  
  @Override
  public void setElementType(ObjectType elementType) {   
    this.elementType = elementType;
  }


  @Override
  public boolean dataPreordered() {    
    return this.orderedDataAddition;
  }
  
  @Override
  protected boolean nullOutputAtBegining() {
    return this.emitNullAtStart;
  }

}
