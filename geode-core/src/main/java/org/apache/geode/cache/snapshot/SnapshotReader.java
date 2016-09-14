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
package org.apache.geode.cache.snapshot;

import java.io.File;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.internal.cache.snapshot.GFSnapshot;
import org.apache.geode.pdx.PdxSerializer;

/**
 * Provides utilities for reading cache data.
 * 
 * @since GemFire 7.0
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

