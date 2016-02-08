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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntries.EntryScoreComparator;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * An implementation of {@link CollectorManager} for managing {@link TopEntriesCollector}. This is used by a member to
 * collect top matching entries from local buckets
 */
public class TopEntriesCollectorManager implements CollectorManager<TopEntriesCollector>, DataSerializableFixedID {
  private static final Logger logger = LogService.getLogger();

  private int limit;
  private String id;

  public TopEntriesCollectorManager() {
    this(null, 0);
  }

  public TopEntriesCollectorManager(String id) {
    this(id, 0);
  }

  public TopEntriesCollectorManager(String id, int resultLimit) {
    this.limit = resultLimit <= 0 ? LuceneQueryFactory.DEFAULT_LIMIT : resultLimit;
    this.id = id == null ? String.valueOf(this.hashCode()) : id;
    logger.debug("Max count of entries to be produced by {} is {}", id, limit);
  }

  @Override
  public TopEntriesCollector newCollector(String name) {
    return new TopEntriesCollector(name, limit);
  }

  @Override
  public TopEntriesCollector reduce(Collection<TopEntriesCollector> collectors) throws IOException {
    TopEntriesCollector mergedResult = new TopEntriesCollector(id, limit);
    if (collectors.isEmpty()) {
      return mergedResult;
    }

    final EntryScoreComparator scoreComparator = new TopEntries().new EntryScoreComparator();

    // orders a entry with higher score above a doc with lower score
    Comparator<ListScanner> entryListComparator = new Comparator<ListScanner>() {
      @Override
      public int compare(ListScanner l1, ListScanner l2) {
        EntryScore o1 = l1.peek();
        EntryScore o2 = l2.peek();
        return scoreComparator.compare(o1, o2);
      }
    };

    // The queue contains iterators for all bucket results. The queue puts the entry with the highest score at the head
    // using score comparator.
    PriorityQueue<ListScanner> entryListsPriorityQueue;
    entryListsPriorityQueue = new PriorityQueue<ListScanner>(collectors.size(),
        Collections.reverseOrder(entryListComparator));

    for (IndexResultCollector collector : collectors) {
      logger.debug("Number of entries found in collector {} is {}", collector.getName(), collector.size());

      if (collector.size() > 0) {
        entryListsPriorityQueue.add(new ListScanner(((TopEntriesCollector) collector).getEntries().getHits()));
      }
    }

    logger.debug("Only {} count of entries will be reduced. Other entries will be ignored", limit);
    while (entryListsPriorityQueue.size() > 0 && limit > mergedResult.size()) {

      ListScanner scanner = entryListsPriorityQueue.remove();
      EntryScore entry = scanner.next();
      mergedResult.collect(entry);

      if (scanner.hasNext()) {
        entryListsPriorityQueue.add(scanner);
      }
    }

    logger.debug("Reduced size of {} is {}", mergedResult.getName(), mergedResult.size());
    return mergedResult;
  }

  /*
   * Utility class to iterate on hits without modifying it
   */
  static class ListScanner {
    private List<EntryScore> hits;
    private int index = 0;

    ListScanner(List<EntryScore> hits) {
      this.hits = hits;
    }

    boolean hasNext() {
      return index < hits.size();
    }

    EntryScore peek() {
      return hits.get(index);
    }

    EntryScore next() {
      return hits.get(index++);
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return LUCENE_TOP_ENTRIES_COLLECTOR_MANAGER;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(id, out);
    out.writeInt(limit);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    id = DataSerializer.readString(in);
    limit = in.readInt();
  }

  /**
   * @return Id of this collector, if any
   */
  public String getId() {
    return id;
  }

  /**
   * @return Result limit enforced by the collectors created by this manager
   */
  public int getLimit() {
    return limit;
  }
}
