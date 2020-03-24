/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF licenses this file to You
 * under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.apache.geode.redis.internal.executor.set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.redis.internal.ByteArrayWrapper;

public class DeltaSet implements Delta, DataSerializable, Serializable, Set<ByteArrayWrapper> {
  transient boolean hasDelta = false;
  transient ByteArrayWrapper latestValue;

  Set<ByteArrayWrapper> internalSet;

  public DeltaSet(Set<ByteArrayWrapper> setToCopy) {
    internalSet = new HashSet<>(setToCopy);
  }

  public DeltaSet() {
    internalSet = new HashSet<>();
  }


  @Override
  public int size() {
    return internalSet.size();
  }

  @Override
  public boolean isEmpty() {
    return internalSet.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return internalSet.contains(o);
  }

  @Override
  public Iterator<ByteArrayWrapper> iterator() {
    return internalSet.iterator();
  }

  @Override
  public Object[] toArray() {
    return internalSet.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return internalSet.toArray(a);
  }

  public synchronized boolean add(ByteArrayWrapper value) {
    latestValue = value;
    hasDelta = internalSet.add(value);
    return hasDelta;
  }

  @Override
  public boolean remove(Object o) {
    return internalSet.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return internalSet.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends ByteArrayWrapper> c) {
    return internalSet.addAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return internalSet.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return internalSet.removeAll(c);
  }

  @Override
  public void clear() {
    internalSet.clear();
  }

  @Override
  public boolean hasDelta() {
    return hasDelta;
  }

  @Override
  public synchronized void toDelta(DataOutput out) throws IOException {
    out.writeUTF(new String(latestValue.toBytes()));
    hasDelta = false;
  }

  @Override
  public synchronized void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    internalSet.add(new ByteArrayWrapper(in.readUTF().getBytes()));
  }

  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashSet((HashSet<ByteArrayWrapper>) this.internalSet, out);
  }

  @Override
  public synchronized void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.internalSet = DataSerializer.readHashSet(in);
  }

}
