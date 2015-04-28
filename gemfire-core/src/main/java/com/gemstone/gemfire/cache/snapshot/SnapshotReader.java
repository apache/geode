/*=========================================================================
 * Copyright (c) 2012 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.snapshot;

import java.io.File;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.internal.cache.snapshot.GFSnapshot;
import com.gemstone.gemfire.pdx.PdxSerializer;

/**
 * Provides utilities for reading cache data.
 * 
 * @author bakera
 * @since 7.0
 */
public class SnapshotReader {
  private SnapshotReader() {
  }
  
  /**
   * Reads a snapshot file and passes the entries to the application.
   * <p>
   * Prior to invoking <code>read</code> all necessary serializers 
   * (either {@link DataSerializer} or {@link PdxSerializer}) and any
   * {@link Instantiator} should have been registered.
   * 
   * @param <K> the key type
   * @param <V> the value type
   * 
   * @param snapshot the snapshot file
   * @return the snapshot iterator
   * 
   * @throws IOException error reading the snapshot file
   * @throws ClassNotFoundException unable deserialize entry
   */
  public static <K, V> SnapshotIterator<K, V> read(File snapshot) 
      throws IOException, ClassNotFoundException {
    return GFSnapshot.read(snapshot);
  }
}

