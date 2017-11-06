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
package org.apache.geode.cache.lucene.internal.results;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.offheap.StoredObject;

public class PageEntry {

  public Object key;
  public Object value;

  public PageEntry() {}

  public PageEntry(final Object key, final Object value) {
    this.key = key;
    this.value = value;
  }

  public Object getKey() {
    return key;
  }

  public Object getValue() {
    if (value instanceof CachedDeserializable) {
      return ((CachedDeserializable) value).getDeserializedValue(null, null);
    }

    return value;
  }

  public void toData(final DataOutput out) throws IOException {

    DataSerializer.writeObject(key, out);
    if (value instanceof StoredObject) {
      ((StoredObject) value).sendTo(out);
      return;
    }

    if (value instanceof CachedDeserializable) {
      value = ((CachedDeserializable) value).getValue();
      if (value instanceof byte[]) {
        out.write((byte[]) value);
        return;
      }
    }

    DataSerializer.writeObject(value, out);
  }

  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    key = DataSerializer.readObject(in);
    value = DataSerializer.readObject(in);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final PageEntry pageEntry = (PageEntry) o;

    if (!getKey().equals(pageEntry.getKey())) {
      return false;
    }
    return getValue().equals(pageEntry.getValue());

  }

  @Override
  public int hashCode() {
    int result = getKey().hashCode();
    result = 31 * result + getValue().hashCode();
    return result;
  }
}
