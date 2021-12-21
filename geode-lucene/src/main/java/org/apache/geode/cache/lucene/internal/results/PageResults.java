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
import java.util.ArrayList;

import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

public class PageResults extends ArrayList<PageEntry> implements DataSerializableFixedID {

  public PageResults(final int initialCapacity) {
    super(initialCapacity);
  }

  public PageResults() {}

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.LUCENE_PAGE_RESULTS;
  }

  @Override
  public void toData(final DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(size());
    for (PageEntry entry : this) {
      entry.toData(out);
    }
  }

  @Override
  public void fromData(final DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      PageEntry entry = new PageEntry();
      entry.fromData(in);
      add(entry);
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return new KnownVersion[0];
  }
}
