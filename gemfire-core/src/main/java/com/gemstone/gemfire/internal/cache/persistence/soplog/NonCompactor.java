/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogReader;

/**
 * Provides a compactor that does no compaction, primarily for testing purposes.
 *  
 * @author bakera
 */
public class NonCompactor implements Compactor {
  /** the fileset */
  private final Fileset<Integer> fileset;
  
  /** the current readers */
  private final Deque<TrackedReference<SortedOplogReader>> readers;
  
  public static Fileset<Integer> createFileset(final String name, final File dir) {
    return new Fileset<Integer>() {
      private final AtomicLong file = new AtomicLong(0);
      
      @Override
      public SortedMap<Integer, ? extends Iterable<File>> recover() {
        return new TreeMap<Integer, Iterable<File>>();
      }

      @Override
      public File getNextFilename() {
        return new File(dir, name + "-" + System.currentTimeMillis() + "-" 
            + file.getAndIncrement() + ".soplog");
      }
    };
  }
  public NonCompactor(String name, File dir) {
    fileset = createFileset(name, dir);
    readers = new ArrayDeque<TrackedReference<SortedOplogReader>>();
  }
  
  @Override
  public boolean compact() throws IOException {
    // liar!
    return true;
  }

  @Override
  public void compact(boolean force, CompactionHandler cd) {
  }

  @Override
  public synchronized Collection<TrackedReference<SortedOplogReader>> getActiveReaders(
      byte[] start, byte[] end) {
    for (TrackedReference<SortedOplogReader> tr : readers) {
      tr.increment();
    }
    return new ArrayList<TrackedReference<SortedOplogReader>>(readers);
  }

  @Override
  public void add(SortedOplog soplog) throws IOException {
    readers.addFirst(new TrackedReference<SortedOplogReader>(soplog.createReader()));
  }

  @Override
  public synchronized void clear() throws IOException {
    for (TrackedReference<SortedOplogReader> tr : readers) {
      tr.get().close();
      readers.remove(tr);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    clear();
  }

  @Override
  public CompactionTracker<Integer> getTracker() {
    return null;
  }
  
  @Override
  public Fileset<Integer> getFileset() {
    return fileset;
  }
}
