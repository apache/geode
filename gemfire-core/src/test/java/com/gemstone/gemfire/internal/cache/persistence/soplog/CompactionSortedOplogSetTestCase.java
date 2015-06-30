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
import java.util.concurrent.Executors;

import com.gemstone.gemfire.internal.cache.persistence.soplog.CompactionTestCase.FileTracker;
import com.gemstone.gemfire.internal.cache.persistence.soplog.CompactionTestCase.WaitingHandler;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SortedIterator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.nofile.NoFileSortedOplogFactory;

public abstract class CompactionSortedOplogSetTestCase extends SortedOplogSetJUnitTest {
  public void testWithCompaction() throws IOException, InterruptedException {
    FlushCounter handler = new FlushCounter();
    SortedOplogSet sos = createSoplogSet("compact");
    
    for (int i = 0; i < 1000; i++) {
      sos.put(wrapInt(i), wrapInt(i));
      if (i % 100 == 0) {
        sos.flush(null, handler);
      }
    }
    
    flushAndWait(handler, sos);
    
    compactAndWait(sos, false);
    validate(sos, 1000);
    sos.close();
  }
  
  public void testTombstone() throws Exception {
    FlushCounter handler = new FlushCounter();
    SortedOplogFactory factory = new NoFileSortedOplogFactory("tombstone");
    Compactor compactor = new SizeTieredCompactor(factory, 
        NonCompactor.createFileset("tombstone", new File(".")), 
        new FileTracker(), 
        Executors.newSingleThreadExecutor(), 
        2, 2);
    
    SortedOplogSet sos = new SortedOplogSetImpl(factory, Executors.newSingleThreadExecutor(), compactor);
    
    for (int i = 0; i < 1000; i++) {
      sos.put(wrapInt(i), wrapInt(i));
    }
    sos.flush(null, handler);
    
    for (int i = 900; i < 1000; i++) {
      sos.put(wrapInt(i), new byte[] {SoplogToken.TOMBSTONE.toByte()});
    }
    flushAndWait(handler, sos);
    compactAndWait(sos, true);

    validate(sos, 900);
    sos.close();
    
  }
  
  public void testInUse() throws Exception {
    FlushCounter handler = new FlushCounter();
    SortedOplogSet sos = createSoplogSet("inuse");
    
    for (int i = 0; i < 1000; i++) {
      sos.put(wrapInt(i), wrapInt(i));
    }
    
    flushAndWait(handler, sos);
    
    // start iterating over soplog
    SortedIterator<ByteBuffer> range = sos.scan();
    assertEquals(0, ((SizeTieredCompactor) sos.getCompactor()).countInactiveReaders());

    for (int i = 1000; i < 5000; i++) {
      sos.put(wrapInt(i), wrapInt(i));
      if (i % 100 == 0) {
        sos.flush(null, handler);
      }
    }

    flushAndWait(handler, sos);
    compactAndWait(sos, false);
    assertEquals(1, ((SizeTieredCompactor) sos.getCompactor()).countInactiveReaders());

    range.close();
    compactAndWait(sos, false);
    assertEquals(0, ((SizeTieredCompactor) sos.getCompactor()).countInactiveReaders());

    validate(sos, 5000);
    sos.close();
  }

  @Override
  protected SortedOplogSetImpl createSoplogSet(String name) throws IOException {
    SortedOplogFactory factory = new NoFileSortedOplogFactory(name);
    Compactor compactor = createCompactor(factory);
    
    return new SortedOplogSetImpl(factory,  Executors.newSingleThreadExecutor(), compactor);
  }
  
  protected abstract AbstractCompactor<?> createCompactor(SortedOplogFactory factory) throws IOException;
  
  private void validate(SortedReader<ByteBuffer> range, int count) throws IOException {
    int i = 0;
    for (SortedIterator<ByteBuffer> iter = range.scan(); iter.hasNext(); i++) {
      iter.next();
      assertEquals(i, iter.key().getInt());
    }
    assertEquals(count, i);
    range.close();
  }

  private void compactAndWait(SortedOplogSet sos, boolean force) throws InterruptedException {
    WaitingHandler wh = new WaitingHandler();
    sos.getCompactor().compact(force, wh);
    wh.waitForCompletion();
    assertNull(wh.getError());
  }
}
