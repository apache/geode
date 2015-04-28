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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogReader;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.AbortableTaskService;
import com.gemstone.gemfire.internal.util.AbortableTaskService.AbortableTask;

/**
 * Provides a unifies view across a set of sbuffers and soplogs.  Updates are 
 * made into the current sbuffer.  When requested, the current sbuffer will be
 * flushed and subsequent updates will flow into a new sbuffer.  All flushes are
 * done on a background thread.
 * 
 * @author bakera
 */
public class SortedOplogSetImpl extends AbstractSortedReader implements SortedOplogSet {
  private static final Logger logger = LogService.getLogger();
  
  /** creates new soplogs */
  private final SortedOplogFactory factory;
  
  /** the background flush thread pool */
  private final AbortableTaskService flusher;
  
  /** the compactor */
  private final Compactor compactor;
  
  /** the current sbuffer */
  private final AtomicReference<SortedBuffer<Integer>> current;
  
  /** the buffer count */
  private final AtomicInteger bufferCount;
  
  /** the unflushed sbuffers */
  private final Deque<SortedBuffer<Integer>> unflushed;
  
  /** the lock for access to unflushed and soplogs */
  private final ReadWriteLock rwlock;
  
  /** test hook for clear/close/destroy during flush */
  volatile CountDownLatch testDelayDuringFlush;
  
  /** test hook to cause IOException during flush */
  volatile boolean testErrorDuringFlush;
  
  private final String logPrefix;
  
  public SortedOplogSetImpl(final SortedOplogFactory factory, Executor exec, Compactor ctor) throws IOException {
    this.factory = factory;
    this.flusher = new AbortableTaskService(exec);
    this.compactor = ctor;
    
    rwlock = new ReentrantReadWriteLock();
    bufferCount = new AtomicInteger(0);
    unflushed = new ArrayDeque<SortedBuffer<Integer>>();
    current = new AtomicReference<SortedBuffer<Integer>>(
        new SortedBuffer<Integer>(factory.getConfiguration(), 0));
    
    this.logPrefix = "<" + factory.getConfiguration().getName() + "> ";
    if (logger.isDebugEnabled()) {
      logger.debug("{}Creating soplog set", this.logPrefix);
    }
  }
  
  @Override
  public boolean mightContain(byte[] key) throws IOException {
    // loops through the following readers:
    //   current sbuffer
    //   unflushed sbuffers
    //   soplogs
    //
    // The loop has been unrolled for efficiency.
    //
    if (getCurrent().mightContain(key)) {
      return true;
    }

    // snapshot the sbuffers and soplogs for stable iteration
    List<SortedReader<ByteBuffer>> readers;
    Collection<TrackedReference<SortedOplogReader>> soplogs;
    rwlock.readLock().lock();
    try {
      readers = new ArrayList<SortedReader<ByteBuffer>>(unflushed);
      soplogs = compactor.getActiveReaders(key, key);
      for (TrackedReference<SortedOplogReader> tr : soplogs) {
        readers.add(tr.get());
      }
    } finally {
      rwlock.readLock().unlock();
    }

    try {
      for (SortedReader<ByteBuffer> rdr : readers) {
        if (rdr.mightContain(key)) {
          return true;
        }
      }
      return false;
    } finally {
      TrackedReference.decrementAll(soplogs);
    }
  }

  @Override
  public ByteBuffer read(byte[] key) throws IOException {
    // loops through the following readers:
    //   current sbuffer
    //   unflushed sbuffers
    //   soplogs
    //
    // The loop has been slightly unrolled for efficiency.
    //
    ByteBuffer val = getCurrent().read(key);
    if (val != null) {
      return val;
    }

    // snapshot the sbuffers and soplogs for stable iteration
    List<SortedReader<ByteBuffer>> readers;
    Collection<TrackedReference<SortedOplogReader>> soplogs;
    rwlock.readLock().lock();
    try {
      readers = new ArrayList<SortedReader<ByteBuffer>>(unflushed);
      soplogs = compactor.getActiveReaders(key, key);
      for (TrackedReference<SortedOplogReader> tr : soplogs) {
        readers.add(tr.get());
      }
    } finally {
      rwlock.readLock().unlock();
    }
    
    try {
      for (SortedReader<ByteBuffer> rdr : readers) {
        if (rdr.mightContain(key)) {
          val = rdr.read(key);
          if (val != null) {
            return val;
          }
        }
      }
      return null;
    } finally {
      TrackedReference.decrementAll(soplogs);
    }
  }

  @Override
  public SortedIterator<ByteBuffer> scan(
      byte[] from, boolean fromInclusive, 
      byte[] to, boolean toInclusive,
      boolean ascending,
      MetadataFilter filter) throws IOException {

    SerializedComparator sc = factory.getConfiguration().getComparator();
    sc = ascending ? sc : ReversingSerializedComparator.reverse(sc);

    List<SortedIterator<ByteBuffer>> scans = new ArrayList<SortedIterator<ByteBuffer>>();
    Collection<TrackedReference<SortedOplogReader>> soplogs;
    rwlock.readLock().lock();
    try {
      scans.add(getCurrent().scan(from, fromInclusive, to, toInclusive, ascending, filter));
      for (SortedBuffer<Integer> sb : unflushed) {
        scans.add(sb.scan(from, fromInclusive, to, toInclusive, ascending, filter));
      }
      soplogs = compactor.getActiveReaders(from, to);
    } finally {
      rwlock.readLock().unlock();
    }

    for (TrackedReference<SortedOplogReader> tr : soplogs) {
      scans.add(tr.get().scan(from, fromInclusive, to, toInclusive, ascending, filter));
    }
    return new MergedIterator(sc, soplogs, scans);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    assert key != null;
    assert value != null;
    
    long start = factory.getConfiguration().getStatistics().getPut().begin();
    getCurrent().put(key, value);
    factory.getConfiguration().getStatistics().getPut().end(value.length, start);
  }

  @Override
  public long bufferSize() {
    return getCurrent().dataSize();
  }

  @Override
  public long unflushedSize() {
    long size = 0;
    rwlock.readLock().lock();
    try {
      for (SortedBuffer<Integer> sb : unflushed) {
        size += sb.dataSize();
      }
    } finally {
      rwlock.readLock().unlock();
    }
    return size;
  }
  
  @Override
  public void flushAndClose(EnumMap<Metadata, byte[]> metadata) throws IOException {
    final AtomicReference<Throwable> err = new AtomicReference<Throwable>(null);
    flush(metadata, new FlushHandler() {
      @Override public void complete() { }
      @Override public void error(Throwable t) { err.set(t); }
    });
    
    // waits for flush completion
    close();
    
    Throwable t = err.get();
    if (t != null) {
      throw new IOException(t);
    }
  }
  
  @Override
  public void flush(EnumMap<Metadata, byte[]> metadata, FlushHandler handler) {
    assert handler != null;
    
    long start = factory.getConfiguration().getStatistics().getFlush().begin();
    
    // flip to a new buffer
    final SortedBuffer<Integer> sb;
    rwlock.writeLock().lock();
    try {
      if (isClosed()) {
        handler.complete();
        factory.getConfiguration().getStatistics().getFlush().end(0, start);
        
        return;
      }
      
      sb = flipBuffer();
      if (sb.count() == 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}Skipping flush of empty buffer {}", this.logPrefix, sb);
        }
        handler.complete();
        return;
      }
      
      sb.setMetadata(metadata);
      unflushed.addFirst(sb);
    
      // Note: this is queued while holding the lock to ensure correct ordering
      // on the executor queue.  Don't use a bounded queue here or we will block
      // the flush invoker.
      flusher.execute(new FlushTask(handler, sb, start));
      
    } finally {
      rwlock.writeLock().unlock();
    }
  }

  @Override
  public void clear() throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}Clearing soplog set", this.logPrefix);
    }

    long start = factory.getConfiguration().getStatistics().getClear().begin();

    // acquire lock to ensure consistency with flushes
    rwlock.writeLock().lock();
    try {
      SortedBuffer<Integer> tmp = current.get();
      if (tmp != null) {
        tmp.clear();
      }

      flusher.abortAll();
      for (SortedBuffer<Integer> sb : unflushed) {
        sb.clear();
      }
      
      unflushed.clear();
      compactor.clear();

      releaseTestDelay();
      flusher.waitForCompletion();
      factory.getConfiguration().getStatistics().getClear().end(start);
      
    } catch (IOException e) {
      factory.getConfiguration().getStatistics().getClear().error(start);
      throw (IOException) e.fillInStackTrace();
      
    } finally {
      rwlock.writeLock().unlock();
    }
  }
  
  @Override
  public void destroy() throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}Destroying soplog set", this.logPrefix);
    }

    long start = factory.getConfiguration().getStatistics().getDestroy().begin();
    try {
      unsetCurrent();
      clear();
      close();
      
      factory.getConfiguration().getStatistics().getDestroy().end(start);
      
    } catch (IOException e) {
      factory.getConfiguration().getStatistics().getDestroy().error(start);
      throw (IOException) e.fillInStackTrace();
    }
  }
  
  @Override
  public void close() throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}Closing soplog set", this.logPrefix);
    }

    unsetCurrent();
    releaseTestDelay();

    flusher.waitForCompletion();
    compactor.close();
  }

  @Override
  public SerializedComparator getComparator() {
    return factory.getConfiguration().getComparator();
  }

  @Override
  public SortedStatistics getStatistics() throws IOException {
    List<SortedStatistics> stats = new ArrayList<SortedStatistics>();
    Collection<TrackedReference<SortedOplogReader>> soplogs;
    
    // snapshot, this is expensive
    rwlock.readLock().lock();
    try {
      stats.add(getCurrent().getStatistics());
      for (SortedBuffer<Integer> sb : unflushed) {
        stats.add(sb.getStatistics());
      }
      soplogs = compactor.getActiveReaders(null, null);
    } finally {
      rwlock.readLock().unlock();
    }
    
    for (TrackedReference<SortedOplogReader> tr : soplogs) {
      stats.add(tr.get().getStatistics());
    }
    return new MergedStatistics(stats, soplogs);
  }

  @Override
  public Compactor getCompactor() {
    return compactor;
  }
  
  @Override
  public boolean isClosed() {
    return current.get() == null;
  }
  
  @Override
  public SortedOplogFactory getFactory() {
    return factory;
  }
  
  private SortedBuffer<Integer> flipBuffer() {
    final SortedBuffer<Integer> sb;
    sb = getCurrent();
    SortedBuffer<Integer> next = new SortedBuffer<Integer>(
        factory.getConfiguration(), 
        bufferCount.incrementAndGet());
  
    current.set(next);
    if (logger.isDebugEnabled()) {
      logger.debug("{}Switching from buffer {} to {}", this.logPrefix, sb, next);
    }
    return sb;
  }

  private SortedBuffer<Integer> getCurrent() {
    SortedBuffer<Integer> tmp = current.get();
    if (tmp == null) {
      throw new IllegalStateException("Closed");
    }
    return tmp;
  }
  
  private void unsetCurrent() {
    rwlock.writeLock().lock();
    try {
      SortedBuffer<Integer> tmp = current.getAndSet(null);
      if (tmp != null) {
        tmp.clear();
      }
    } finally {
      rwlock.writeLock().unlock();
    }
  }
  
  private void releaseTestDelay() {
    if (testDelayDuringFlush != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}Releasing testDelayDuringFlush", this.logPrefix);
      }
      testDelayDuringFlush.countDown();
    }
  }

  private class FlushTask implements AbortableTask {
    private final FlushHandler handler;
    private final SortedBuffer<Integer> buffer;
    private final long start;
    
    public FlushTask(FlushHandler handler, SortedBuffer<Integer> buffer, long start) {
      this.handler = handler;
      this.buffer = buffer;
      this.start = start;
    }
    
    @Override
    public void runOrAbort(final AtomicBoolean aborted) {
      try {
        // First transfer the contents of the buffer to a new soplog.
        final SortedOplog soplog = writeBuffer(buffer, aborted);
        
        // If we are aborted, someone else will cleanup the unflushed queue
        if (soplog == null || !lockOrAbort(aborted)) {
          handler.complete();
          return;
        }

        try {
          Runnable action = new Runnable() {
            @Override
            public void run() {
              try {
                compactor.add(soplog);
                compactor.compact(false, null);

                unflushed.removeFirstOccurrence(buffer);
                
                // TODO need to invoke this while NOT holding write lock
                handler.complete();
                factory.getConfiguration().getStatistics().getFlush().end(buffer.dataSize(), start);
                
              } catch (Exception e) {
                handleError(e, aborted);
                return;
              }
            }
          };
          
          // Enforce flush ordering for consistency.  If the previous buffer flush
          // is incomplete, we defer completion and release the thread to avoid
          // deadlocks.
          if (buffer == unflushed.peekLast()) {
            action.run();
            
            SortedBuffer<Integer> tail = unflushed.peekLast();
            while (tail != null && tail.isDeferred() && !aborted.get()) {
              // TODO need to invoke this while NOT holding write lock
              tail.complete();
              tail = unflushed.peekLast();
            }
          } else {
            buffer.defer(action);
          }
        } finally {
          rwlock.writeLock().unlock();
        }
      } catch (Exception e) {
        handleError(e, aborted);
      }
    }
    
    @Override
    public void abortBeforeRun() {
      handler.complete();
      factory.getConfiguration().getStatistics().getFlush().end(start);
    }
    
    private void handleError(Exception e, AtomicBoolean aborted) {
      if (lockOrAbort(aborted)) {
        try {
          unflushed.removeFirstOccurrence(buffer);
        } finally {
          rwlock.writeLock().unlock();
        }
      }  

      handler.error(e);
      factory.getConfiguration().getStatistics().getFlush().error(start);
    }
    
    private SortedOplog writeBuffer(SortedBuffer<Integer> sb, AtomicBoolean aborted) 
        throws IOException {
      File f = compactor.getFileset().getNextFilename();
      if (logger.isDebugEnabled()) {
        logger.debug("{}Flushing buffer {} to {}", SortedOplogSetImpl.this.logPrefix, sb, f);
      }

      SortedOplog so = factory.createSortedOplog(f);
      SortedOplogWriter writer = so.createWriter();
      try {
        if (testErrorDuringFlush) {
          throw new IOException("Flush error due to testErrorDuringFlush=true");
        }
        
        for (Entry<byte[], byte[]> entry : sb.entries()) {
          if (aborted.get()) {
            writer.closeAndDelete();
            return null;
          }
          writer.append(entry.getKey(), entry.getValue());
        }
   
        checkTestDelay();
        
        writer.close(buffer.getMetadata());
        return so;
   
      } catch (IOException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}Encountered error while flushing buffer {}", SortedOplogSetImpl.this.logPrefix, sb, e);
        }
        
        writer.closeAndDelete();
        throw e;
      }
    }

    private void checkTestDelay() {
      if (testDelayDuringFlush != null) {
        try {
          if (logger.isDebugEnabled()) {
            logger.debug("{}Waiting for testDelayDuringFlush", SortedOplogSetImpl.this.logPrefix);
          }
          testDelayDuringFlush.await();
          
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private boolean lockOrAbort(AtomicBoolean abort) {
      try {
        while (!abort.get()) {
          if (rwlock.writeLock().tryLock(10, TimeUnit.MILLISECONDS)) {
            return true;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return false;
    }
  }
  
  private class MergedStatistics implements SortedStatistics {
    private final List<SortedStatistics> stats;
    private final Collection<TrackedReference<SortedOplogReader>> soplogs;

    public MergedStatistics(List<SortedStatistics> stats, Collection<TrackedReference<SortedOplogReader>> soplogs) {
      this.stats = stats;
      this.soplogs = soplogs;
    }
    
    @Override
    public long keyCount() {
      // TODO we have no way of determining the overall key population
      // just assume no overlap for now
      long keys = 0;
      for (SortedStatistics ss : stats) {
        keys += ss.keyCount();
      }
      return keys;
    }

    @Override
    public byte[] firstKey() {
      byte[] first = stats.get(0).firstKey();
      for (int i = 1; i < stats.size(); i++) {
        byte[] tmp = stats.get(i).firstKey();
        if (getComparator().compare(first, tmp) > 0) {
          first = tmp;
        }
      }
      return first;
    }

    @Override
    public byte[] lastKey() {
      byte[] last = stats.get(0).lastKey();
      for (int i = 1; i < stats.size(); i++) {
        byte[] tmp = stats.get(i).lastKey();
        if (getComparator().compare(last, tmp) < 0) {
          last = tmp;
        }
      }
      return last;
    }

    @Override
    public double avgKeySize() {
      double avg = 0;
      for (SortedStatistics ss : stats) {
        avg += ss.avgKeySize();
      }
      return avg / stats.size();
    }

    @Override
    public double avgValueSize() {
      double avg = 0;
      for (SortedStatistics ss : stats) {
        avg += ss.avgValueSize();
      }
      return avg / stats.size();
    }

    @Override
    public void close() {
      TrackedReference.decrementAll(soplogs);
    }
  }
  
  /**
   * Provides ordered iteration across a set of sorted data sets. 
   */
  public static class MergedIterator 
    extends AbstractKeyValueIterator<ByteBuffer, ByteBuffer> 
    implements SortedIterator<ByteBuffer>
  {
    /** the comparison operator */
    private final SerializedComparator comparator;
    
    /** the reference counted soplogs */
    private final Collection<TrackedReference<SortedOplogReader>> soplogs;

    /** the backing iterators */
    private final List<SortedIterator<ByteBuffer>> iters;
    
    /** the current key */
    private ByteBuffer key;
    
    /** the current value */
    private ByteBuffer value;
    
    public MergedIterator(SerializedComparator comparator, 
        Collection<TrackedReference<SortedOplogReader>> soplogs, 
        List<SortedIterator<ByteBuffer>> iters) {
      this.comparator = comparator;
      this.soplogs = soplogs;
      this.iters = iters;
      
      // initialize iteration positions
      int i = 0;
      while (i < iters.size()) {
        i = advance(i);
      }
    }
    
    @Override
    public ByteBuffer key() {
      return key;
    }

    @Override
    public ByteBuffer value() {
      return value;
    }

    @Override
    protected boolean step() {
      if (iters.isEmpty() || readerIsClosed()) {
        return false;
      }
      
      int cursor = 0;
      key = iters.get(cursor).key();
      
      int i = 1;
      while (i < iters.size()) {
        ByteBuffer tmp = iters.get(i).key();
        
        int diff = comparator.compare(tmp.array(), tmp.arrayOffset(), tmp.remaining(), 
            key.array(), key.arrayOffset(), key.remaining());
        if (diff < 0) {
          cursor = i++;
          key = tmp;
          
        } else if (diff == 0) {
          i = advance(i);
          
        } else {
          i++;
        }
      }
      
      value = iters.get(cursor).value();
      advance(cursor);
      
      return true;
    }
    
    @Override
    public void close() {
      for (SortedIterator<ByteBuffer> iter : iters) {
        iter.close();
      }
      TrackedReference.decrementAll(soplogs);
    }

    private int advance(int idx) {
      // either advance the cursor or remove the iterator
      if (!iters.get(idx).hasNext()) {
        iters.remove(idx).close();
        return idx;
      }
      iters.get(idx).next();
      return idx + 1;
    }
    
    private boolean readerIsClosed() {
      for (TrackedReference<SortedOplogReader> tr : soplogs) {
        if (tr.get().isClosed()) {
          return true;
        }
      }
      return false;
    }
  }
}
