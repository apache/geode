/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog.nofile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

import com.gemstone.gemfire.internal.cache.persistence.soplog.AbstractSortedReader;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedBuffer.BufferIterator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.Metadata;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SortedStatistics;

public class NoFileSortedOplog implements SortedOplog {
  private final SortedOplogConfiguration config;
  
  private final AtomicReference<NavigableMap<byte[], byte[]>> data;
  private final EnumMap<Metadata, byte[]> metadata;
  private final NoFileStatistics stats;
  
  public NoFileSortedOplog(SortedOplogConfiguration config) {
    this.config = config;
    
    data = new AtomicReference<NavigableMap<byte[],byte[]>>();
    metadata = new EnumMap<Metadata, byte[]>(Metadata.class);
    stats = new NoFileStatistics();
  }
  
  @Override
  public SortedOplogReader createReader() throws IOException {
    return new NoFileReader();
  }

  @Override
  public SortedOplogWriter createWriter() throws IOException {
    synchronized (metadata) {
      metadata.clear();
    }
    data.set(new ConcurrentSkipListMap<byte[], byte[]>(config.getComparator()));
    
    return new NoFileWriter();
  }
  
  private class NoFileWriter implements SortedOplogWriter {
    @Override
    public void append(byte[] key, byte[] value) throws IOException {
      if (data.get().put(key, value) == null) {
        stats.add(key.length, value.length);
      }
    }

    @Override
    public void append(ByteBuffer key, ByteBuffer value) throws IOException {
      byte[] k = new byte[key.remaining()];
      byte[] v = new byte[value.remaining()];
      
      key.get(k);
      value.get(v);
      
      append(k, v);
    }

    @Override
    public void close(EnumMap<Metadata, byte[]> meta) throws IOException {
      if (meta != null) {
        synchronized (metadata) {
          metadata.putAll(meta);
        }
      }
    }

    @Override
    public void closeAndDelete() throws IOException {
      data.get().clear();
      data.set(null);
    }
  }
  
  private class NoFileReader extends AbstractSortedReader implements SortedOplogReader {
    private final BloomFilter bloom;
    private volatile boolean closed;
    
    public NoFileReader() {
      closed = false;
      bloom = new BloomFilter() {
        @Override
        public boolean mightContain(byte[] key) {
          return data.get().containsKey(key);
        }
      };
    }
    
    @Override
    public boolean mightContain(byte[] key) throws IOException {
      return bloom.mightContain(key);
    }

    @Override
    public ByteBuffer read(byte[] key) throws IOException {
      return ByteBuffer.wrap(data.get().get(key));
    }

    @Override
    public SortedIterator<ByteBuffer> scan(
        byte[] from, boolean fromInclusive, 
        byte[] to, boolean toInclusive,
        boolean ascending,
        MetadataFilter filter) {

      if (filter == null || filter.accept(metadata.get(filter.getName()))) {
        NavigableMap<byte[],byte[]> subset = ascending ? data.get() : data.get().descendingMap();
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
      return (SerializedComparator) data.get().comparator();
    }

    @Override
    public SortedStatistics getStatistics() {
      return stats;
    }

    @Override
    public boolean isClosed() {
      return closed;
    }
    
    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public BloomFilter getBloomFilter() {
      return bloom;
    }

    @Override
    public byte[] getMetadata(Metadata name) {
      synchronized (metadata) {
        return metadata.get(name);
      }
    }

    @Override
    public File getFile() {
      return new File(".");
    }
    
    @Override
    public String getFileName() {
      return "name";
    }
   
    @Override
    public long getModificationTimeStamp() throws IOException {
      return 0;
    }
    
    @Override
    public void rename(String name) throws IOException {
    }
    
    @Override
    public void delete() throws IOException {
    }
  }
  
  private class NoFileStatistics implements SortedStatistics {
    private long keys;
    private double avgKeySize;
    private double avgValueSize;
    
    private synchronized void add(int keyLength, int valueLength) {
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
      return data.get().firstKey();
    }

    @Override
    public byte[] lastKey() {
      return data.get().lastKey();
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
  }
}
