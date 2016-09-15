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
package org.apache.geode.cache.query.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.ObjectIntHashMap.Entry;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CachePerfStats;

public class ResultsBag extends Bag implements
    DataSerializableFixedID {

  protected ObjectIntHashMap map;

  public ResultsBag() {
    this.map = new ObjectIntHashMap();
  }

  /**
   * This constructor should only be used by the DataSerializer. Creates a
   * ResultsBag with no fields.
   */
  public ResultsBag(boolean ignored) {
    super(ignored);
  }

  /**
   * @param stats
   *          the CachePerfStats to track hash collisions. Should be null unless
   *          this is used as a query execution-time result set.
   */
  public ResultsBag(CachePerfStats stats) {
    super(stats);
    this.map = new ObjectIntHashMap();
  }

  protected ResultsBag(HashingStrategy strategy, CachePerfStats stats) {
    super(stats);
    this.map = new ObjectIntHashMap(strategy);
  }

  /**
   * @param stats
   *          the CachePerfStats to track hash collisions. Should be null unless
   *          this is used as a query execution-time result set.
   */
  ResultsBag(Collection c, CachePerfStats stats) {
    this(stats);
    for (Iterator itr = c.iterator(); itr.hasNext();) {
      this.add(itr.next());
    }
  }

  protected ResultsBag(Collection c, HashingStrategy strategy,
      CachePerfStats stats) {
    this(strategy, stats);
    for (Iterator itr = c.iterator(); itr.hasNext();) {
      this.add(itr.next());
    }
  }

  /**
   * @param stats
   *          the CachePerfStats to track hash collisions. Should be null unless
   *          this is used as a query execution-time result set.
   */
  ResultsBag(SelectResults sr, CachePerfStats stats) {
    this((Collection) sr, stats);
    // grab type info
    setElementType(sr.getCollectionType().getElementType());
  }

  /**
   * @param stats
   *          the CachePerfStats to track hash collisions. Should be null unless
   *          this is used as a query execution-time result set.
   */
  ResultsBag(ObjectType elementType, CachePerfStats stats) {
    this(stats);
    setElementType(elementType);
  }

  /**
   * @param stats
   *          the CachePerfStats to track hash collisions. Should be null unless
   *          this is used as a query execution-time result set.
   */
  ResultsBag(ObjectType elementType, int initialCapacity, CachePerfStats stats) {
    this(initialCapacity, stats);
    setElementType(elementType);
  }

  /**
   * @param stats
   *          the CachePerfStats to track hash collisions. Should be null unless
   *          this is used as a query execution-time result set.
   */
  ResultsBag(int initialCapacity, float loadFactor, CachePerfStats stats) {
    this.map = new ObjectIntHashMap(initialCapacity, loadFactor);
  }

  protected ResultsBag(int initialCapacity, float loadFactor,
      HashingStrategy strategy, CachePerfStats stats) {
    super(stats);
    this.map = new ObjectIntHashMap(initialCapacity, loadFactor, strategy);
  }

  ResultsBag(int initialCapacity, CachePerfStats stats) {
    super(stats);
    this.map = new ObjectIntHashMap(initialCapacity);

  }

  protected ResultsBag(int initialCapacity, HashingStrategy strategy,
      CachePerfStats stats) {
    super(stats);
    this.map = new ObjectIntHashMap(initialCapacity, strategy);

  }

  protected ObjectIntHashMap createMapForFromData() {
    return new ObjectIntHashMap(this.size);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.elementType = (ObjectType) DataSerializer.readObject(in);
    this.size = in.readInt();
    assert this.size >= 0 : this.size;
    this.map = createMapForFromData();
    this.readNumNulls(in);
    // Asif: The size will be including null so the Map should at max contain
    // size - number of nulls
    int numLeft = this.size - this.numNulls;

    while (numLeft > 0) {
      Object key = DataSerializer.readObject(in);
      int occurence = in.readInt();
      this.map.put(key, occurence);
      numLeft -= occurence;
    }
  }

  public int getDSFID() {
    return RESULTS_BAG;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.elementType, out);
    out.writeInt(this.size());
    this.writeNumNulls(out);
    // TODO:Asif: Should we actually pass the limit in serialization?
    // For the time being not passing , assuming PR Has parsed
    // it
    // out.writeInt(this.limit);
    int numLeft = this.size() - this.numNulls;
    for (Iterator<Entry> itr = this.map.entrySet().iterator(); itr.hasNext()
        && numLeft > 0;) {
      Entry entry = itr.next();
      Object key = entry.getKey();
      DataSerializer.writeObject(key, out);
      int occurence = entry.getValue();
      if (numLeft < occurence) {
        occurence = numLeft;
      }
      out.writeInt(occurence);
      numLeft -= occurence;
    }
  }

  /**
   */
  void createIntHashMap() {
    this.map = new ObjectIntHashMap(this.size - this.numNulls);
  }

  @Override
  public boolean isModifiable() {
    return true;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  protected int mapGet(Object element) {
    return this.map.get(element);
  }

  @Override
  protected boolean mapContainsKey(Object element) {
    return this.map.containsKey(element);
  }

  @Override
  protected void mapPut(Object element, int count) {
    this.map.put(element, count);

  }

  @Override
  protected int mapSize() {
    return this.map.size();
  }

  @Override
  protected int mapRemove(Object element) {
    return this.map.remove(element);
  }

  @Override
  protected void mapClear() {
    this.map.clear();

  }

  @Override
  protected Object getMap() {
    return this.map;
  }

  @Override
  protected int mapHashCode() {
    return this.map.hashCode();
  }

  @Override
  protected boolean mapEmpty() {
    return this.map.isEmpty();
  }

  @Override
  protected Iterator mapEntryIterator() {
    return this.map.entrySet().iterator();
  }

  @Override
  protected Iterator mapKeyIterator() {
    return this.map.keySet().iterator();
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
