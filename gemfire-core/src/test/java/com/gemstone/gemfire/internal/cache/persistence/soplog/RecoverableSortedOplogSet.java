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
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.persistence.soplog.AppendLog.AppendLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;

public class RecoverableSortedOplogSet extends AbstractSortedReader implements SortedOplogSet {
  private static final Logger logger = LogService.getLogger();
  
  private final SortedOplogSet sos;
  private final long bufferSize;
  
  private final long maxBufferMemory;
  
  private final Lock rollLock;
  private AtomicReference<AppendLogWriter> writer;
  
  private final String logPrefix;
  
  public RecoverableSortedOplogSet(SortedOplogSet sos, long bufferSize, double memLimit) throws IOException {
    this.sos = sos;
    this.bufferSize = bufferSize;
    
    this.logPrefix = "<" + sos.getFactory().getConfiguration().getName() + "> ";
    
    rollLock = new ReentrantLock();
    writer = new AtomicReference<AppendLogWriter>(AppendLog.create(nextLogFile()));
    
    maxBufferMemory = Math.round(memLimit * ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
  }
  
  @Override
  public boolean mightContain(byte[] key) throws IOException {
    return sos.mightContain(key);
  }

  @Override
  public ByteBuffer read(byte[] key) throws IOException {
    return sos.read(key);
  }

  @Override
  public SortedIterator<ByteBuffer> scan(byte[] from, boolean fromInclusive, byte[] to, boolean toInclusive) throws IOException {
    return sos.scan(from, fromInclusive, to, toInclusive);
  }

  @Override
  public SortedIterator<ByteBuffer> scan(
      byte[] from,
      boolean fromInclusive,
      byte[] to,
      boolean toInclusive,
      boolean ascending,
      MetadataFilter filter) throws IOException {
    return sos.scan(from, fromInclusive, to, toInclusive, ascending, filter);
  }

  @Override
  public SerializedComparator getComparator() {
    return sos.getComparator();
  }

  @Override
  public SortedOplogFactory getFactory() {
    return sos.getFactory();
  }
  
  @Override
  public SortedStatistics getStatistics() throws IOException {
    return sos.getStatistics();
  }

  @Override
  public void close() throws IOException {
    rollLock.lock();
    try {
      writer.get().close();
      writer.set(null);
      sos.close();
    } finally {
      rollLock.unlock();
    }
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    throttle();
    if (sos.bufferSize() > bufferSize) {
      roll(false);
    }

    writer.get().append(key, value);
    sos.put(key, value);
  }

  @Override
  public long bufferSize() {
    return sos.bufferSize();
  }

  @Override
  public long unflushedSize() {
    return sos.unflushedSize();
  }

  @Override
  public void flush(EnumMap<Metadata, byte[]> metadata, FlushHandler handler) throws IOException {
    roll(true);
  }

  @Override
  public void flushAndClose(EnumMap<Metadata, byte[]> metadata) throws IOException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Compactor getCompactor() {
    return sos.getCompactor();
  }

  @Override
  public void clear() throws IOException {
    rollLock.lock();
    try {
      roll(true);
      sos.clear();
    } finally {
      rollLock.unlock();
    }
  }

  @Override
  public void destroy() throws IOException {
    roll(true);
    sos.destroy();
  }

  @Override
  public boolean isClosed() {
    return sos.isClosed();
  }
  
  private void throttle() {
    int n = 0;
    while (sos.bufferSize() + sos.unflushedSize() > maxBufferMemory) {
      try {
        Thread.sleep(1 << n++);
        
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void roll(boolean wait) throws IOException {
    boolean locked = true;
    if (wait) {
      rollLock.lock();
    } else {
      locked = rollLock.tryLock();
    }
    
    if (locked) {
      try {
        AppendLogWriter next = AppendLog.create(nextLogFile());
        final AppendLogWriter old = writer.getAndSet(next);
        old.close();

        if (logger.isDebugEnabled()) {
          logger.debug("{}Rolling from {} to {}", this.logPrefix, old.getFile(), next.getFile());
        }

        sos.flush(null, new FlushHandler() {
          @Override
          public void complete() {
            old.getFile().delete();
          }

          @Override
          public void error(Throwable t) {
          }
        });
      } finally {
        rollLock.unlock();
      }
    }
  }

  private File nextLogFile() {
    return new File(sos.getFactory().getConfiguration().getName() 
        + "-" + UUID.randomUUID().toString() + ".aolog");
  }
}
