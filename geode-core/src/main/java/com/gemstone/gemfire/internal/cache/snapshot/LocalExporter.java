/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.snapshot;

import java.io.IOException;
import java.util.Map.Entry;

import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.snapshot.RegionSnapshotServiceImpl.ExportSink;
import com.gemstone.gemfire.internal.cache.snapshot.RegionSnapshotServiceImpl.Exporter;
import com.gemstone.gemfire.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;

/**
 * Exports snapshot data directly to the supplied {@link ExportSink}.  All data
 * is assumed to be local so snapshot data is obtained directly by iterating
 * over the {@link Region#entrySet()}.
 * 
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LocalExporter<K, V> implements Exporter<K, V> {
  @Override
  public long export(Region<K, V> region, ExportSink sink, SnapshotOptions<K, V> options) throws IOException {
    LocalRegion local = RegionSnapshotServiceImpl.getLocalRegion(region);
    
    long count = 0;
    for (Entry<K, V> entry : region.entrySet()) {
      try {
        if (options.getFilter() == null || options.getFilter().accept(entry)) {
          sink.write(new SnapshotRecord(local, entry));
          count++;
        }
      } catch (EntryDestroyedException e) {
        // continue to next entry
      }
    }
    return count;
  }
}
