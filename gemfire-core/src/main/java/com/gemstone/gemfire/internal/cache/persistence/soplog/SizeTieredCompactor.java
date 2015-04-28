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
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogReader;

/**
 * Implements a size-tiered compaction scheme in which the soplogs are organized
 * by levels of increasing size.  Each level is limited to a fixed number of 
 * files, <code>M</code>. Given an initial size of <code>N</code> the amount of
 * disk space consumed by a level <code>L</code> is <code>M * N^(L+1)</code>.
 * <p>
 * During compaction, this approach will temporarily double the amount of space
 * consumed by the level.  Compactions are performed on a background thread.
 * <p>
 * Soplogs that have been compacted will be moved to the inactive list where they
 * will be deleted once they are no longer in use.
 * 
 * @author bakera
 */
public class SizeTieredCompactor extends AbstractCompactor<Integer> {
  /** restricts the number of soplogs per level */
  private final int maxFilesPerLevel;
  
  // TODO consider relaxing the upper bound so the levels are created dynamically
  /** restricts the number of levels; files in maxLevel are not compacted */
  private final int maxLevels;
  
  public SizeTieredCompactor(SortedOplogFactory factory, 
      Fileset<Integer> fileset, CompactionTracker<Integer> tracker,
      Executor exec, int maxFilesPerLevel, int maxLevels)
      throws IOException {
    super(factory, fileset, tracker, exec);

    assert maxFilesPerLevel > 0;
    assert maxLevels > 0;
    
    this.maxFilesPerLevel = maxFilesPerLevel;
    this.maxLevels = maxLevels;
    
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}Creating size-tiered compactor", super.logPrefix);
    }

    for (int i = 0; i < maxLevels; i++) {
      levels.add(new OrderedLevel(i));
    }
    
    for (Map.Entry<Integer, ? extends Iterable<File>> entry : fileset.recover().entrySet()) {
      int level = Math.min(maxLevels - 1, entry.getKey());
      for (File f : entry.getValue()) {
        if (isDebugEnabled) {
          logger.debug("{}Adding {} to level {}", super.logPrefix, f, level);
        }
        levels.get(level).add(factory.createSortedOplog(f));
      }
    }
  }

  @Override
  public String toString() {
    return String.format("%s <%d/%d>", factory.getConfiguration().getName(), maxFilesPerLevel, maxLevels);
  }
  
  /**
   * Organizes a set of soplogs for a given level.  All operations on the 
   * soplogs are synchronized via the instance monitor.
   */
  protected class OrderedLevel extends Level {
    /** the ordered set of soplog readers */
    private final Deque<TrackedReference<SortedOplogReader>> soplogs;
    
    /** true if the level is being compacted */
    private final AtomicBoolean isCompacting;
    
    public OrderedLevel(int level) {
      super(level);
      soplogs = new ArrayDeque<TrackedReference<SortedOplogReader>>(maxFilesPerLevel);
      isCompacting = new AtomicBoolean(false);
    }
    
    @Override
    protected synchronized boolean needsCompaction() {
      // TODO this is safe but overly conservative...we need to allow parallel
      // compaction of a level such that we guarantee completion order and handle
      // errors
      return !isCompacting.get() 
          && soplogs.size() >= maxFilesPerLevel 
          && level != maxLevels - 1;
    }
    
    @Override
    protected List<TrackedReference<SortedOplogReader>> getSnapshot(byte[] start, byte[] end) {
      // ignoring range limits since keys are stored in overlapping files
      List<TrackedReference<SortedOplogReader>> snap;
      synchronized (this) {
        snap = new ArrayList<TrackedReference<SortedOplogReader>>(soplogs);
      }

      for (TrackedReference<SortedOplogReader> tr : snap) {
        tr.increment();
      }
      return snap; 
    }

    @Override
    protected synchronized void clear() throws IOException {
      for (TrackedReference<SortedOplogReader> tr : soplogs) {
        tr.get().close();
      }
      markAsInactive(soplogs, level);
      soplogs.clear();
    }
    
    @Override
    protected synchronized void close() throws IOException {
      for (TrackedReference<SortedOplogReader> tr : soplogs) {
        tr.get().close();
        factory.getConfiguration().getStatistics().incActiveFiles(-1);
      }
      soplogs.clear();
    }

    @Override
    protected void add(SortedOplog soplog) throws IOException {
      SortedOplogReader rdr = soplog.createReader();
      synchronized (this) {
        soplogs.addFirst(new TrackedReference<SortedOplogReader>(rdr));
      }
      
      if (logger.isDebugEnabled()) {
        logger.debug("{}Added file {} to level {}", SizeTieredCompactor.super.logPrefix, rdr, level);
      }
      tracker.fileAdded(rdr.getFile(), level);
      factory.getConfiguration().getStatistics().incActiveFiles(1);
    }

    @Override
    protected boolean compact(AtomicBoolean aborted) throws IOException {
      assert level < maxLevels : "Can't compact level: " + level;
      
      if (!isCompacting.compareAndSet(false, true)) {
        // another thread won so gracefully bow out
        return false;
      }
      
      try {
        List<TrackedReference<SortedOplogReader>> snapshot = getSnapshot(null, null);
        try {
          SortedOplog merged = merge(snapshot, level == maxLevels - 1, aborted);
          
          synchronized (this) {
            if (merged != null) {
              levels.get(Math.min(level + 1, maxLevels - 1)).add(merged);
            }
            markAsInactive(snapshot, level);
            soplogs.removeAll(snapshot);
          }
        } catch (InterruptedIOException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}Aborting compaction of level {}", SizeTieredCompactor.super.logPrefix, level);
          }
          return false;
        }
        return true;
      } finally {
        boolean set = isCompacting.compareAndSet(true, false);
        assert set;
      }
    }
  }
}
