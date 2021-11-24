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
package org.apache.geode.redis.internal.data.collections;


import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;

import java.util.Collection;

import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import org.apache.geode.internal.size.Sizeable;

public abstract class SizeableObjectOpenCustomHashSet<K> extends ObjectOpenCustomHashSet<K>
    implements Sizeable {
  private static final long serialVersionUID = 9174920505089089517L;
  private static final int OPEN_HASH_SET_OVERHEAD =
      memoryOverhead(SizeableObjectOpenCustomHashSet.class);

  private int memberOverhead;

  public SizeableObjectOpenCustomHashSet(int expected, Strategy<? super K> strategy) {
    super(expected, strategy);
  }

  public SizeableObjectOpenCustomHashSet(Strategy<? super K> strategy) {
    super(strategy);
  }

  public SizeableObjectOpenCustomHashSet(Collection<? extends K> c, Strategy<? super K> strategy) {
    super(c, strategy);
  }

  @Override
  public boolean add(K k) {
    boolean added = super.add(k);
    if (added) {
      memberOverhead += sizeElement(k);
    }
    return added;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object k) {
    boolean removed = super.remove(k);
    if (removed) {
      memberOverhead -= sizeElement((K) k);
    }
    return removed;
  }

  @Override
  public int getSizeInBytes() {
    // The object referenced by the "strategy" field is not sized
    // since it is usually a singleton instance.
    return OPEN_HASH_SET_OVERHEAD + memoryOverhead(key) + memberOverhead;
  }

  protected abstract int sizeElement(K element);
}
