/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Holds a ordered collection of entries matching a search query.
 *
 * @param <K> the type of key
 */
public class TopEntries<K> implements DataSerializableFixedID {
  // ordered collection of entries
  private List<EntryScore<K>> hits = new ArrayList<>();

  // the maximum number of entries stored in this
  private int limit;

  // comparator to order entryScore instances
  final Comparator<EntryScore<K>> comparator = new EntryScoreComparator();

  public TopEntries() {
    this(LuceneQueryFactory.DEFAULT_LIMIT);
  }

  public TopEntries(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException();
    }
    this.limit = limit;
  }

  /**
   * Adds an entry to the collection. The new entry must have a lower score than all previous
   * entries added to the collection. The new entry will be ignored if the limit is already reached.
   *
   */
  public void addHit(EntryScore<K> entry) {
    if (hits.size() > 0) {
      EntryScore lastEntry = hits.get(hits.size() - 1);
      if (comparator.compare(lastEntry, entry) < 0) {
        throw new IllegalArgumentException();
      }
    }

    if (hits.size() >= limit) {
      return;
    }

    hits.add(entry);
  }

  /**
   * @return count of entries in the collection
   */
  public int size() {
    return hits.size();
  }

  /**
   * @return The entries collection managed by this instance
   */
  public List<EntryScore<K>> getHits() {
    return hits;
  }

  /**
   * @return The maximum capacity of this collection
   */
  public int getLimit() {
    return limit;
  }

  /**
   * Compares scores of two entries using natural ordering. I.e. it returns -1 if the first entry's
   * score is less than the second one.
   */
  class EntryScoreComparator implements Comparator<EntryScore<K>> {
    @Override
    public int compare(EntryScore<K> o1, EntryScore<K> o2) {
      return Float.compare(o1.getScore(), o2.getScore());
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getDSFID() {
    return LUCENE_TOP_ENTRIES;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(limit);
    context.getSerializer().writeObject(hits, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    limit = in.readInt();
    hits = context.getDeserializer().readObject(in);
  }
}
