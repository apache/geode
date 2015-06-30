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

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogReader;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SortedIterator;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SizeTieredCompactorJUnitTest extends CompactionTestCase<Integer> {
  public void testBasic() throws Exception {
    compactor.add(createSoplog(0, 100, 0));
    
    assertEquals(1, compactor.getActiveReaders(null, null).size());
    assertEquals(1, compactor.getLevel(0).getSnapshot().size());
    assertFalse(compactor.getLevel(0).needsCompaction());
    
    WaitingHandler wh = new WaitingHandler();
    compactor.compact(false, wh);
    wh.waitForCompletion();

    assertEquals(1, compactor.getActiveReaders(null, null).size());
    assertEquals(1, compactor.getLevel(0).getSnapshot().size());
    assertFalse(compactor.getLevel(0).needsCompaction());
  }

  public void testCompactionLevel0() throws Exception {
    compactor.add(createSoplog(0, 100, 0));
    compactor.add(createSoplog(100, 100, 1));
    
    assertEquals(2, compactor.getActiveReaders(null, null).size());
    assertEquals(2, compactor.getLevel(0).getSnapshot().size());
    assertTrue(compactor.getLevel(0).needsCompaction());

    WaitingHandler wh = new WaitingHandler();
    compactor.compact(false, wh);
    wh.waitForCompletion();

    assertEquals(1, compactor.getActiveReaders(null, null).size());
    assertEquals(0, compactor.getLevel(0).getSnapshot().size());
    assertEquals(1, compactor.getLevel(1).getSnapshot().size());
    assertFalse(compactor.getLevel(0).needsCompaction());
    assertFalse(compactor.getLevel(1).needsCompaction());
    
    validate(compactor.getActiveReaders(null, null).iterator().next().get(), 0, 200);
  }
  
  public void testMultilevelCompaction() throws Exception {
    for (int i = 0; i < 8; i += 2) {
      compactor.add(createSoplog(0, 100, i));
      compactor.add(createSoplog(100, 100, i+1));

      WaitingHandler wh = new WaitingHandler();
      compactor.compact(false, wh);
      wh.waitForCompletion();
    }
    
    assertEquals(1, compactor.getActiveReaders(null, null).size());
    validate(compactor.getActiveReaders(null, null).iterator().next().get(), 0, 200);
  }
  
  public void testForceCompaction() throws Exception {
    compactor.add(createSoplog(0, 100, 0));
    compactor.add(createSoplog(100, 100, 1));
    boolean compacted = compactor.compact();
    
    assertTrue(compacted);
    validate(compactor.getActiveReaders(null, null).iterator().next().get(), 0, 200);
  }

  @Override
  protected AbstractCompactor<Integer> createCompactor(SortedOplogFactory factory) throws IOException {
    return new SizeTieredCompactor(factory, 
        NonCompactor.createFileset("test", new File(".")), 
        new FileTracker(), 
        Executors.newSingleThreadExecutor(),
        2, 4);
  }

  private void validate(SortedOplogReader soplog, int start, int count) throws IOException {
    int i = 0;
    for (SortedIterator<ByteBuffer> iter = soplog.scan(); iter.hasNext(); i++) {
      iter.next();
      assertEquals(i, iter.key().getInt());
      assertEquals(i, iter.value().getInt());
    }
    assertEquals(count, i);
  }
}
