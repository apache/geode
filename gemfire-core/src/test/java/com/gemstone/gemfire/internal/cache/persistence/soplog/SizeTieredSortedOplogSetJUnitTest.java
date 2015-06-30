package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.persistence.soplog.CompactionTestCase.FileTracker;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SizeTieredSortedOplogSetJUnitTest extends CompactionSortedOplogSetTestCase {
  @Override
  protected AbstractCompactor<?> createCompactor(SortedOplogFactory factory) throws IOException {
    return new SizeTieredCompactor(factory, 
        NonCompactor.createFileset("test", new File(".")), 
        new FileTracker(), 
        Executors.newSingleThreadExecutor(),
        2, 4);
  }
  @Override
  public void testStatistics() throws IOException {
    // remove this noop override when bug 52249 is fixed
  }

}
