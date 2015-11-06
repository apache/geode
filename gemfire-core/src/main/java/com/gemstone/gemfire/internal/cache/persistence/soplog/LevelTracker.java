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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Writer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.internal.cache.persistence.soplog.Compactor.CompactionTracker;
import com.gemstone.gemfire.internal.cache.persistence.soplog.Compactor.Fileset;

/**
 * A simple, non-robust file tracker for tracking soplogs by level.
 * 
 * @author bakera
 */
public class LevelTracker implements Fileset<Integer>, CompactionTracker<Integer>, Closeable {
  private final String name;
  private final File manifest;
  
  private final SortedMap<Integer, Set<File>> levels;
  private final AtomicLong file;
  
  public LevelTracker(String name, File manifest) throws IOException {
    this.name = name;
    this.manifest = manifest;
    file = new AtomicLong(0);
    
    levels = new TreeMap<Integer, Set<File>>();
    if (!manifest.exists()) {
      return;
    }
    
    LineNumberReader rdr = new LineNumberReader(new FileReader(manifest));
    try {
      String line;
      while ((line = rdr.readLine()) != null) {
        String[] parts = line.split(",");
        int level = Integer.parseInt(parts[0]);
        File f = new File(parts[1]);
        add(f, level);
      }
    } finally {
      rdr.close();
    }
  }
  
  @Override
  public SortedMap<Integer, ? extends Iterable<File>> recover() {
    return levels;
  }

  @Override
  public File getNextFilename() {
    return new File(manifest.getParentFile(),  name + "-" + System.currentTimeMillis() 
        + "-" + file.getAndIncrement() + ".soplog");
  }

  @Override
  public void fileAdded(File f, Integer attach) {
    add(f, attach);
  }

  @Override
  public void fileRemoved(File f, Integer attach) {
    levels.get(attach).remove(f);
  }

  @Override
  public void fileDeleted(File f) {
  }

  @Override
  public void close() throws IOException {
    Writer wtr = new FileWriter(manifest);
    try {
      for (Map.Entry<Integer, Set<File>> entry : levels.entrySet()) {
        for (File f : entry.getValue()) {
          wtr.write(entry.getKey() + "," + f + "\n");
        }
      }
    } finally {
      wtr.flush();
      wtr.close();
    }
  }
  
  private void add(File f, int level) {
    Set<File> files = levels.get(level);
    if (files == null) {
      files = new HashSet<File>();
      levels.put(level, files);
    }
    files.add(f);
  }
}
