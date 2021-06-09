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
package org.apache.geode.redis.internal.collections;

import java.util.Collection;
import java.util.Iterator;

import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.size.ReflectionSingleObjectSizer;
import org.apache.geode.internal.size.Sizeable;

public class SizeableObjectOpenCustomHashSet<K> extends ObjectOpenCustomHashSet<K>
    implements Sizeable {
  private static final long serialVersionUID = 9174920505089089517L;
  public static final int BACKING_ARRAY_OVERHEAD_CONSTANT = 92;
  public static final int BACKING_ARRAY_LENGTH_COEFFICIENT = 4;
  private static final ReflectionSingleObjectSizer elementSizer =
      ReflectionSingleObjectSizer.getInstance();

  private int memberOverhead;

  public SizeableObjectOpenCustomHashSet(int expected, float f, Strategy<? super K> strategy) {
    super(expected, f, strategy);
  }

  public SizeableObjectOpenCustomHashSet(int expected, Strategy<? super K> strategy) {
    super(expected, strategy);
  }

  public SizeableObjectOpenCustomHashSet(Strategy<? super K> strategy) {
    super(strategy);
  }

  public SizeableObjectOpenCustomHashSet(Collection<? extends K> c, float f,
      Strategy<? super K> strategy) {
    super(c, f, strategy);
  }

  public SizeableObjectOpenCustomHashSet(Collection<? extends K> c, Strategy<? super K> strategy) {
    super(c, strategy);
  }

  public SizeableObjectOpenCustomHashSet(ObjectCollection<? extends K> c, float f,
      Strategy<? super K> strategy) {
    super(c, f, strategy);
  }

  public SizeableObjectOpenCustomHashSet(ObjectCollection<? extends K> c,
      Strategy<? super K> strategy) {
    super(c, strategy);
  }

  public SizeableObjectOpenCustomHashSet(Iterator<? extends K> i, float f,
      Strategy<? super K> strategy) {
    super(i, f, strategy);
  }

  public SizeableObjectOpenCustomHashSet(Iterator<? extends K> i, Strategy<? super K> strategy) {
    super(i, strategy);
  }

  public SizeableObjectOpenCustomHashSet(K[] a, int offset, int length, float f,
      Strategy<? super K> strategy) {
    super(a, offset, length, f, strategy);
  }

  public SizeableObjectOpenCustomHashSet(K[] a, int offset, int length,
      Strategy<? super K> strategy) {
    super(a, offset, length, strategy);
  }

  public SizeableObjectOpenCustomHashSet(K[] a, float f, Strategy<? super K> strategy) {
    super(a, f, strategy);
  }

  public SizeableObjectOpenCustomHashSet(K[] a, Strategy<? super K> strategy) {
    super(a, strategy);
  }

  @Override
  public boolean add(K k) {
    boolean added = super.add(k);
    if (added) {
      memberOverhead += (int) elementSizer.sizeof(k);
    }
    return added;
  }

  @Override
  public boolean remove(Object k) {
    boolean removed = super.remove(k);
    if (removed) {
      memberOverhead -= elementSizer.sizeof(k);
    }
    return removed;
  }

  @Override
  public int getSizeInBytes() {
    return memberOverhead + calculateBackingArrayOverhead();
  }

  @VisibleForTesting
  int getMemberOverhead() {
    return memberOverhead;
  }

  @VisibleForTesting
  int getBackingArrayLength() {
    return key.length;
  }

  @VisibleForTesting
  int calculateBackingArrayOverhead() {
    // This formula determined experimentally using tests
    return BACKING_ARRAY_OVERHEAD_CONSTANT + (BACKING_ARRAY_LENGTH_COEFFICIENT * key.length);
  }
}
