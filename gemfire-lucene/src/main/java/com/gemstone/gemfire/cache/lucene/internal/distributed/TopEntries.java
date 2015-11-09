/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * Holds a ordered collection of entries matching a search query.
 */
public class TopEntries implements DataSerializableFixedID {
  // ordered collection of entries
  private List<EntryScore> hits = new ArrayList<>();

  // the maximum number of entries stored in this
  private int limit;

  // comparator to order entryScore instances
  final Comparator<EntryScore> comparator = new EntryScoreComparator();

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
   * Adds an entry to the collection. The new entry must have a lower score than all previous entries added to the
   * collection. The new entry will be ignored if the limit is already reached.
   * 
   * @param entry
   */
  public void addHit(EntryScore entry) {
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
  public List<EntryScore> getHits() {
    return hits;
  }

  /**
   * @return The maximum capacity of this collection
   */
  public int getLimit() {
    return limit;
  }

  /**
   * Compares scores of two entries using natural ordering. I.e. it returns -1 if the first entry's score is less than
   * the second one.
   */
  class EntryScoreComparator implements Comparator<EntryScore> {
    @Override
    public int compare(EntryScore o1, EntryScore o2) {
      return Float.compare(o1.getScore(), o2.getScore());
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getDSFID() {
    return LUCENE_TOP_ENTRIES;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(limit);
    DataSerializer.writeObject(hits, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    limit = in.readInt();
    hits = DataSerializer.readObject(in);
  };
}
