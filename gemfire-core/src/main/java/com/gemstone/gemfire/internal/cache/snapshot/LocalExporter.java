/*=========================================================================
 * Copyright (c) 2012 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author bakera
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
