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
package org.apache.geode.internal.cache.eviction;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.size.Sizeable;

abstract class SizeLRUController extends AbstractEvictionController {

  private int perEntryOverhead;

  private final ObjectSizer sizer;

  SizeLRUController(EvictionCounters evictionCounters, EvictionAction evictionAction,
      ObjectSizer sizer, EvictionAlgorithm algorithm) {
    super(evictionCounters, evictionAction, algorithm);
    this.sizer = sizer;
  }

  public int getPerEntryOverhead() {
    return perEntryOverhead;
  }

  @Override
  public void setPerEntryOverhead(int entryOverhead) {
    perEntryOverhead = entryOverhead;
  }

  /**
   * Return the size of an object as stored in GemFire. Typically this is the serialized size in
   * bytes.
   */
  int sizeof(Object object) {
    final boolean cdChangingForm = object instanceof CachedDeserializableValueWrapper;
    if (cdChangingForm) {
      object = ((CachedDeserializableValueWrapper) object).getValue();
    }
    if (object == null || object == Token.INVALID || object == Token.LOCAL_INVALID
        || object == Token.DESTROYED || object == Token.TOMBSTONE) {
      return 0;
    }

    int size;
    // Shouldn't we defer to the user's object sizer for these things?
    if (object instanceof byte[] || object instanceof String) {
      size = ObjectSizer.DEFAULT.sizeof(object);
    } else if (object instanceof Sizeable) {
      size = ((Sizeable) object).getSizeInBytes();
    } else if (sizer != null) {
      size = sizer.sizeof(object);
    } else {
      size = ObjectSizer.DEFAULT.sizeof(object);
    }
    if (cdChangingForm) {
      size += CachedDeserializableFactory.overhead();
    }
    return size;
  }
}
