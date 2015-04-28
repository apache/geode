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
import java.util.Collection;
import java.util.SortedMap;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogReader;

/**
 * Defines a mechanism to track and compact soplogs.
 * 
 * @author bakera
 */
public interface Compactor {
  /**
   * Compares metadata values.
   */
  public interface MetadataCompactor {
    /**
     * Combines two metadata values into a single value.  Used during compaction
     * to merge metadata between soplog files.
     * 
     * @param metadata1 the first value
     * @param metadata2 the second value
     * @return the combined metadata
     */
    byte[] compact(byte[] metadata1, byte[] metadata2);
  }

  /**
   * Provides notification on the status of a compaction.
   */
  public interface CompactionHandler {
    /**
     * Invoked when a compaction operation has completed successfully.
     * @param compacted true if any files were compacted
     */
    void complete(boolean compacted);
    
    /**
     * Invoked when a compaction operation has failed.
     * @param ex the failure
     */
    void failed(Throwable ex);
  }

  /**
   * Provides external configuration of file operations for recovering and
   * new file creation.
   *
   * @param <T> the compaction info
   */
  public interface Fileset<T extends Comparable<T>> {
    /**
     * Returns the set of active soplogs.
     * @return the active files
     */
    SortedMap<T, ? extends Iterable<File>> recover();
    
    /**
     * Returns the pathname for the next soplog.
     * @return the soplog filename
     */
    File getNextFilename();
  }
  
  /**
   * Provides a mechanism to coordinate file changes to the levels managed
   * by the compactor.
   * 
   * @param T the attachment type
   */
  public interface CompactionTracker<T extends Comparable<T>> {
    /**
     * Invoked when a new file is added.
     * @param f the file
     * @param attach the attachment
     */
    void fileAdded(File f, T attach);
    
    /**
     * Invoked when a file is removed.
     * @param f the file
     * @param attach the attachment
     */
    void fileRemoved(File f, T attach);
    
    /**
     * Invoked when a file is deleted.
     * @param f the attachment
     */
    void fileDeleted(File f);
  }
  
  /**
   * Synchronously invokes the force compaction operation and waits for completion.
   * 
   * @return true if any files were compacted
   * @throws IOException error during compaction
   */
  boolean compact() throws IOException;

  /**
   * Requests a compaction operation be performed on the soplogs.  This invocation
   * may block if there are too many outstanding write requests.
   * 
   * @param force if false, compaction will only be performed if necessary
   * @param ch invoked when the compaction is complete, optionally null
   * @throws IOException error during compaction
   */
  void compact(boolean force, CompactionHandler ch);
  
  /**
   * Returns the active readers for the given key range.  The caller is responsible
   * for decrementing the use count of each reader when finished.
   * 
   * @param start the start key inclusive, or null for beginning
   * @param end the end key inclusive, or null for last
   * @return the readers
   * 
   * @see TrackedReference
   */
  Collection<TrackedReference<SortedOplogReader>> getActiveReaders(
      byte[] start, byte[] end);
  
  /**
   * Adds a new soplog to the active set.
   * @param soplog the soplog
   * @throws IOException unable to add soplog 
   */
  void add(SortedOplog soplog) throws IOException;
  
  /**
   * Returns the compaction tracker for coordinating changes to the file set.
   * @return the tracker
   */
  CompactionTracker<?> getTracker();
  
  /**
   * Returns the file manager for managing the soplog files.
   * @return the fileset
   */
  Fileset<?> getFileset();
  
  /**
   * Clears the active files managed by the compactor.  Files will be marked as
   * inactive and eventually deleted.
   * 
   * @throws IOException unable to clear
   */
  void clear() throws IOException;
  /**
   * Closes the compactor.
   * @throws IOException unable to close
   */
  void close() throws IOException;
}
