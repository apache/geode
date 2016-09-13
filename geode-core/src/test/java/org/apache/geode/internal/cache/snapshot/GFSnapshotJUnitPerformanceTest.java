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
package org.apache.geode.internal.cache.snapshot;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.snapshot.SnapshotIterator;
import org.apache.geode.internal.cache.snapshot.GFSnapshot.GFSnapshotExporter;
import org.apache.geode.internal.cache.snapshot.GFSnapshot.GFSnapshotImporter;
import org.apache.geode.internal.cache.snapshot.GFSnapshot.SnapshotWriter;
import org.apache.geode.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;
import org.apache.geode.test.junit.categories.PerformanceTest;

@Category(PerformanceTest.class)
@Ignore("Test has no assertions and will always pass")
public class GFSnapshotJUnitPerformanceTest {

  private static final String val = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

  private File f;

  @Before
  public void setUp() throws Exception {
    f = new File("test.snapshot");
  }

  @After
  public void tearDown() throws Exception {
    if (f.exists()) {
      f.delete();
    }
  }

  @Test
  public void testStreamWritePerformance() throws IOException {
    int i = 0;
    while (i++ < 10) {
      long start = System.currentTimeMillis();
      int count = 100000;
      
      writeFile(count, val);
      
      long elapsed = System.currentTimeMillis() - start;
      double rate = 1000.0 * count / elapsed;
  
      System.out.println(rate + " stream write operations / sec");
    }
  }

  @Test
  public void testWritePerformance() throws IOException {
    int j = 0;
    while (j++ < 10) {
      long start = System.currentTimeMillis();
      int count = 100000;
      
      String s = val;
      SnapshotWriter ss = GFSnapshot.create(f, "test");
      try {
        for (int i = 0; i < count; i++) {
          ss.snapshotEntry(new SnapshotRecord(i, s));
        }
      } finally {
        ss.snapshotComplete();
      }
      
      long elapsed = System.currentTimeMillis() - start;
      double rate = 1000.0 * count / elapsed;
  
      System.out.println(rate + " write operations / sec");
    }
  }
  
  @Test
  public void testStreamReadPerformance() throws IOException,
      ClassNotFoundException {
    writeFile(100000, val);
    
    int i = 0;
    while (i++ < 10) {
      long start = System.currentTimeMillis();
      int count = 0;
  
      GFSnapshotImporter in = new GFSnapshotImporter(f);
      
      SnapshotRecord entry;
      while ((entry = in.readSnapshotRecord()) != null) {
        count++;
      }
      in.close();
      
      long elapsed = System.currentTimeMillis() - start;
      double rate = 1000.0 * count / elapsed;
  
      System.out.println(rate + " stream read operations / sec");
    }
  }
  
  @Test
  public void testCopyPerformance() throws IOException, ClassNotFoundException {
    int count = 100000;
    for (int i = 0; i < 10; i++) {
      writeFile(count, val);

      File tmp = File.createTempFile("snapshot-copy", null);
      tmp.deleteOnExit();
      
      final SnapshotWriter writer = GFSnapshot.create(tmp, "test");
      
      long start = System.currentTimeMillis();
      SnapshotIterator<Integer, String> iter = GFSnapshot.read(f);
      try {
        while (iter.hasNext()) {
          Entry<Integer, String> entry = iter.next();
          writer.snapshotEntry(new SnapshotRecord(null, entry));
        }
        writer.snapshotComplete();
        
        long elapsed = System.currentTimeMillis() - start;
        double rate = 1.0 * count / elapsed;
        
        System.out.println("rate = " + rate + " entries / ms");
      } finally {
        iter.close();
      }
    }
  }

  private void writeFile(int count, String s) throws IOException {
    GFSnapshotExporter out = new GFSnapshotExporter(f, "test");

    try {
      for (int i = 0; i < count; i++) {
        out.writeSnapshotEntry(new SnapshotRecord(i, s));
      }
    } finally {
      out.close();
    }
  }
}
