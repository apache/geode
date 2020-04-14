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

package org.apache.geode.redis.internal.executor.set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;

class DeltaSet implements Set<ByteArrayWrapper>, Delta, DataSerializable {
  private Collection<ByteArrayWrapper> members;
  private boolean hasDelta;
  private Collection<? extends ByteArrayWrapper> elementsAddedDelta;

  public DeltaSet(Collection<ByteArrayWrapper> members) {

    this.members = members;
  }

  public DeltaSet() {
    this.members = new HashSet<>();
  }

  public static Set<ByteArrayWrapper> brandNew(Collection<ByteArrayWrapper> membersToAdd) {
    return new DeltaSet(membersToAdd);
  }

  public static Set<ByteArrayWrapper> fromDeltaSet(Set<ByteArrayWrapper> currentValue) {
    return new DeltaSet(new HashSet<>(currentValue));
  }


  //SET INTERFACE
  @Override
  public int size() {
    return members.size();
  }

  @Override
  public boolean isEmpty() {
    return members.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return members.contains(o);
  }

  @Override
  public Iterator<ByteArrayWrapper> iterator() {
    return members.iterator();
  }

  @Override
  public Object[] toArray() {
    return members.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return members.toArray(a);
  }

  @Override
  public boolean add(ByteArrayWrapper byteArrayWrapper) {
    return members.add(byteArrayWrapper);
  }

  @Override
  public boolean remove(Object o) {
    return members.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return members.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends ByteArrayWrapper> c) {
    return members.addAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return members.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return  members.removeAll(c);
  }

  @Override
  public void clear() {
    members.clear();
  }









  //DELTA
  @Override
  public boolean hasDelta() {
    return hasDelta;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    DataSerializer.writeHashSet((HashSet<?>) elementsAddedDelta, out);
    hasDelta = false;
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    Collection<? extends ByteArrayWrapper>  elementsAdded;
    try {
      elementsAdded = DataSerializer.readHashSet(in);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.members.addAll(elementsAdded);
  }


  //DATA SERIALIZABLE

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashSet((HashSet<?>) members, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.members = DataSerializer.readHashSet(in);
  }

  synchronized public long customAddAll(Collection<ByteArrayWrapper> membersToAdd,
                                        Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region,
                                        ByteArrayWrapper key) {
    
    int oldSize = this.members.size();
    boolean isAddAllSuccessful = this.members.addAll(membersToAdd);
    if(!isAddAllSuccessful) {
      return 0;
    }
    elementsAddedDelta = membersToAdd;
    hasDelta = true;
    int newSize = this.members.size();
    int elementsAdded = newSize - oldSize;
    region.put(key, this);
    return elementsAdded;
  }
}
