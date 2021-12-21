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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.ObjectIntHashMap.Entry;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

public class ResultsBag extends Bag implements DataSerializableFixedID {

  protected ObjectIntHashMap map;

  public ResultsBag() {
    map = new ObjectIntHashMap();
  }

  /**
   * This constructor should only be used by the DataSerializer. Creates a ResultsBag with no
   * fields.
   */
  public ResultsBag(boolean ignored) {
    super(ignored);
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  public ResultsBag(CachePerfStats stats) {
    super(stats);
    map = new ObjectIntHashMap();
  }

  protected ResultsBag(HashingStrategy strategy, CachePerfStats stats) {
    super(stats);
    map = new ObjectIntHashMap(strategy);
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  ResultsBag(Collection c, CachePerfStats stats) {
    this(stats);
    for (Iterator itr = c.iterator(); itr.hasNext();) {
      add(itr.next());
    }
  }

  protected ResultsBag(Collection c, HashingStrategy strategy, CachePerfStats stats) {
    this(strategy, stats);
    for (Iterator itr = c.iterator(); itr.hasNext();) {
      add(itr.next());
    }
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  ResultsBag(SelectResults sr, CachePerfStats stats) {
    this((Collection) sr, stats);
    // grab type info
    setElementType(sr.getCollectionType().getElementType());
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  ResultsBag(ObjectType elementType, CachePerfStats stats) {
    this(stats);
    setElementType(elementType);
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  ResultsBag(ObjectType elementType, int initialCapacity, CachePerfStats stats) {
    this(initialCapacity, stats);
    setElementType(elementType);
  }

  /**
   * @param stats the CachePerfStats to track hash collisions. Should be null unless this is used as
   *        a query execution-time result set.
   */
  ResultsBag(int initialCapacity, float loadFactor, CachePerfStats stats) {
    map = new ObjectIntHashMap(initialCapacity, loadFactor);
  }

  protected ResultsBag(int initialCapacity, float loadFactor, HashingStrategy strategy,
      CachePerfStats stats) {
    super(stats);
    map = new ObjectIntHashMap(initialCapacity, loadFactor, strategy);
  }

  ResultsBag(int initialCapacity, CachePerfStats stats) {
    super(stats);
    map = new ObjectIntHashMap(initialCapacity);

  }

  protected ResultsBag(int initialCapacity, HashingStrategy strategy, CachePerfStats stats) {
    super(stats);
    map = new ObjectIntHashMap(initialCapacity, strategy);

  }

  protected ObjectIntHashMap createMapForFromData() {
    return new ObjectIntHashMap(size);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    elementType = context.getDeserializer().readObject(in);
    size = in.readInt();
    assert size >= 0 : size;
    map = createMapForFromData();
    readNumNulls(in);
    // Asif: The size will be including null so the Map should at max contain
    // size - number of nulls
    int numLeft = size - numNulls;

    while (numLeft > 0) {
      Object key = context.getDeserializer().readObject(in);
      int occurrence = in.readInt();
      map.put(key, occurrence);
      numLeft -= occurrence;
    }
  }

  @Override
  public int getDSFID() {
    return RESULTS_BAG;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(elementType, out);
    out.writeInt(size());
    writeNumNulls(out);
    // TODO:Asif: Should we actually pass the limit in serialization?
    // For the time being not passing , assuming PR Has parsed
    // it
    // out.writeInt(this.limit);
    int numLeft = size() - numNulls;
    for (Iterator<Entry> itr = map.entrySet().iterator(); itr.hasNext() && numLeft > 0;) {
      Entry entry = itr.next();
      Object key = entry.getKey();
      context.getSerializer().writeObject(key, out);
      int occurrence = entry.getValue();
      if (numLeft < occurrence) {
        occurrence = numLeft;
      }
      out.writeInt(occurrence);
      numLeft -= occurrence;
    }
  }

  void createIntHashMap() {
    map = new ObjectIntHashMap(size - numNulls);
  }

  @Override
  public boolean isModifiable() {
    return true;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  protected int mapGet(Object element) {
    return map.get(element);
  }

  @Override
  protected boolean mapContainsKey(Object element) {
    return map.containsKey(element);
  }

  @Override
  protected void mapPut(Object element, int count) {
    map.put(element, count);

  }

  @Override
  protected int mapSize() {
    return map.size();
  }

  @Override
  protected int mapRemove(Object element) {
    return map.remove(element);
  }

  @Override
  protected void mapClear() {
    map.clear();

  }

  @Override
  protected Object getMap() {
    return map;
  }

  @Override
  protected int mapHashCode() {
    return map.hashCode();
  }

  @Override
  protected boolean mapEmpty() {
    return map.isEmpty();
  }

  @Override
  protected Iterator mapEntryIterator() {
    return map.entrySet().iterator();
  }

  @Override
  protected Iterator mapKeyIterator() {
    return map.keySet().iterator();
  }

  @Override
  protected Object keyFromEntry(Object entry) {
    return ((Entry) entry).getKey();
  }

  @Override
  protected Integer valueFromEntry(Object entry) {

    return ((Entry) entry).getValue();
  }
}
