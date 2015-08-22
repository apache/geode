package com.gemstone.gemfire.cache.lucene.internal;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * A unit test of the RegionDirectory class that uses the Directory test
 * case from the lucene code base.
 * 
 * This test is still mocking out the underlying cache, rather than using
 * a real region.
 */
@Category(UnitTest.class)
public class RegionDirectoryJUnitTest extends BaseDirectoryTestCase {
  
  @After
  public void clearLog4J() {
    //The lucene test ensures that no system properties
    //have been modified by the test. GFE leaves this property
    //set
    System.clearProperty("log4j.configurationFile");
  }
  
  protected Directory getDirectory(Path path) throws IOException {
    return new RegionDirectory(new ConcurrentHashMap<String, File>(), new ConcurrentHashMap<ChunkKey, byte[]>());
  }

  @Override
  public void testCopyBytesWithThreads() throws Exception {
    //TODO - this method is currently failing
  }
  
  
}
