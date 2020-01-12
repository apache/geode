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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.examples.snapshot.MyObject;

import org.apache.geode.cache.snapshot.SnapshotIterator;
import org.apache.geode.cache.snapshot.SnapshotReader;

public class SnapshotTestUtil {

  public static <K, V> void checkSnapshotEntries(File dir, Map<K, V> expected, String diskStoreName,
      String regionName) throws Exception {
    final Map<K, V> testData = new HashMap<K, V>(expected);
    String snapshot = "snapshot-" + diskStoreName + "-" + regionName + ".gfd";
    try (SnapshotIterator<Integer, MyObject> iter = SnapshotReader.read(new File(dir, snapshot))) {
      while (iter.hasNext()) {
        Entry<Integer, MyObject> entry = iter.next();
        Object expectedVal = testData.remove(entry.getKey());
        assertEquals(expectedVal, entry.getValue());
      }
      assertTrue(testData.isEmpty());
    }
  }
}
