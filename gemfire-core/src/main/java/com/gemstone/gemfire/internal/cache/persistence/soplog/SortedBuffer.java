/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Provides an in-memory buffer to temporarily hold key/value pairs until they
 * can be flushed to disk.  Each buffer instance can be optionally associated
 * with a user-specified tag for identification purposes.
 * 
 * @param <T> the tag type
 * @author bakera
 */
public class SortedBuffer<T> extends AbstractSortedReader {
  private static final Logger logger = LogService.getLogger();
  
  /** the tag */
  private final T tag;
  
  /** in-memory sorted key/vaue buffer */
  private final NavigableMap<byte[], byte[]> buffer;

  /** the stats */
  private final BufferStats stats;
  
  /** the metadata, set during flush */
  private final EnumMap<Metadata, byte[]> metadata;
  
  /** the command to run (or defer) when the flush is complete */
  private Runnable flushAction;
  
  private final String logPrefix;
  
  public SortedBuffer(SortedOplogConfiguration config, T tag) {
    assert config != null;
    assert tag != null;
    
    this.tag = tag;
    
    buffer = new ConcurrentSkipListMap<byte[], byte[]>(config.getComparator());
    stats = new BufferStats();
    metadata = new EnumMap<Metadata, byte[]>(Metadata.class);
    
    this.logPrefix = "<" + config.getName() + "#" + tag + "> ";
  }
  
  /**
   * Returns the tag associated with the buffer.
   * @return the tag
   */
  public T getTag() {
    return tag;
  }
  
  @Override
  public String toString() {
    return logger.getName() + this.logPrefix;
  }
  
  /**
   * Adds a new value to the buffer.
   * @param key the key
   * @param value the value
   */
  public void put(byte[] key, byte[] value) {
    if (buffer.put(key, value) == null) {
      // ASSUMPTION: updates don't significantly change the value length
      // this lets us optimize statistics calculations
      stats.add(key.length, value.length);
    }
  }
  
  /**
   * Allows sorted iteration over the buffer contents.
   * @return the buffer entries
   */
  public Iterable<Entry<byte[], byte[]>> entries() {
    return buffer.entrySet();
  }
  
  /**
   * Returns the number of entries in the buffer.
   * @return the count
   */
  public int count() {
    return buffer.size();
  }
  
  /**
   * Returns the size of the data in bytes.
   * @return the data size
   */
  public long dataSize() {
    return stats.totalSize();
  }
  
  /**
   * Clears the buffer of all entries.
   */
  public void clear() {
    if (logger.isDebugEnabled()) {
      logger.debug("{}Clearing buffer", this.logPrefix);
    }
    
    buffer.clear();
    stats.clear();
    metadata.clear();
    
    synchronized (this) {
      flushAction = null;
    }
  }
  
  /**
   * Returns true if the flush completion has been deferred.
   * @return true if deferred
   */
  public synchronized boolean isDeferred() {
    return flushAction != null;
  }
  
  /**
   * Defers the flush completion to a later time.  This is used to ensure correct
   * ordering of soplogs during parallel flushes.
   * 
   * @param action the action to perform when ready
   */
  public synchronized void defer(Runnable action) {
    assert flushAction == null;
    
    if (logger.isDebugEnabled()) {
      logger.debug("{}Deferring flush completion", this.logPrefix);
    }
    flushAction = action;
  }
  
  /**
   * Completes the deferred flush operation.
   */
  public synchronized void complete() {
    assert flushAction != null;

    try {
      if (logger.isDebugEnabled()) {
        logger.debug("{}Completing deferred flush operation", this.logPrefix);
      }
      flushAction.run();
      
    } finally {
      flushAction = null;
    }
  }
  
  /**
   * Returns the buffer metadata.
   * @return the metadata
   */
  public synchronized EnumMap<Metadata, byte[]> getMetadata() {
    return metadata;
  }
  
  /**
   * Returns the metadata value for the given key.
   * 
   * @param name the metadata name
   * @return the requested metadata
   */
  public synchronized byte[] getMetadata(Metadata name) {
    return metadata.get(name);
  }
  
  /**
   * Sets the metadata for the buffer.  This is not available until the buffer
   * is about to be flushed.
   * 
   * @param metadata the metadata
   */
  public synchronized void setMetadata(EnumMap<Metadata, byte[]> metadata) {
    if (metadata != null) {
      this.metadata.putAll(metadata);
    }
  }
  
  @Override
  public boolean mightContain(byte[] key) {
    return true;
  }

  @Override
  public ByteBuffer read(byte[] key) throws IOException {
    byte[] val = buffer.get(key);
    if (val != null) {
      return ByteBuffer.wrap(val);
    }
    return null;
  }

  @Override
  public SortedIterator<ByteBuffer> scan(
      byte[] from, boolean fromInclusive, 
      byte[] to, boolean toInclusive,
      boolean ascending,
      MetadataFilter filter) {

    if (filter == null || filter.accept(metadata.get(filter.getName()))) {
      NavigableMap<byte[],byte[]> subset = ascending ? buffer : buffer.descendingMap();
      if (from == null && to == null) {
        // we're good
      } else if (from == null) {
        subset = subset.headMap(to, toInclusive);
      } else if (to == null) {
        subset = subset.tailMap(from, fromInclusive);
      } else {
        subset = subset.subMap(from, fromInclusive, to, toInclusive);
      }
      return new BufferIterator(subset.entrySet().iterator());
    }
    return new BufferIterator(Collections.<byte[], byte[]>emptyMap().entrySet().iterator());
  }

  @Override
  public SerializedComparator getComparator() {
    return (SerializedComparator) buffer.comparator();
  }

  @Override
  public SortedStatistics getStatistics() {
    return stats;
  }
  
  @Override
  public void close() throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}Closing buffer", this.logPrefix);
    }
    
    synchronized (this) {
      flushAction = null;
    }
  }
  
  /**
   * Allows sorted iteration over the buffer contents.
   */
  public static class BufferIterator 
    extends AbstractKeyValueIterator<ByteBuffer, ByteBuffer>
    implements SortedIterator<ByteBuffer>
  {
    /** the backing iterator */
    private final Iterator<Entry<byte[], byte[]>> entries;
    
    /** the iteration cursor */
    private Entry<byte[], byte[]> current;
    
    public BufferIterator(Iterator<Entry<byte[], byte[]>> iterator) {
      this.entries = iterator;
    }
    
    @Override
    public ByteBuffer key() {
      return ByteBuffer.wrap(current.getKey());
    }

    @Override
    public ByteBuffer value() {
      return ByteBuffer.wrap(current.getValue());
    }

    @Override
    public void close() {
    }

    @Override
    protected boolean step() {
      return (current = entries.hasNext() ? entries.next() : null) != null;
    }
  }
  
  private class BufferStats implements SortedStatistics {
    /** data size */
    private long totalSize;
    
    /** key count */
    private long keys;
    
    /** avg key size */
    private double avgKeySize;
    
    /** avg value size */
    private double avgValueSize;
    
    private synchronized void clear() {
      totalSize = 0;
      keys = 0;
      avgKeySize = 0;
      avgValueSize = 0;
    }
    
    private synchronized void add(int keyLength, int valueLength) {
      totalSize += keyLength + valueLength;
      avgKeySize = (keyLength + keys * avgKeySize) / (keys + 1);
      avgValueSize = (keyLength + keys * avgValueSize) / (keys + 1);
      
      keys++;
    }
    
    @Override
    public synchronized long keyCount() {
      return keys;
    }

    @Override
    public byte[] firstKey() {
      return buffer.firstKey();
    }

    @Override
    public byte[] lastKey() {
      return buffer.lastKey();
    }

    @Override
    public synchronized double avgKeySize() {
      return avgKeySize;
    }
    
    @Override
    public synchronized double avgValueSize() {
      return avgValueSize;
    }
    
    @Override
    public void close() {
    }

    public synchronized long totalSize() {
      return totalSize;
    }
  }
}
