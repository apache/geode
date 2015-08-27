package com.gemstone.gemfire.cache.lucene.internal.directory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
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
  @Rule
  public SystemPropertiesRestoreRule restoreProps = new SystemPropertiesRestoreRule();
  
//  @After
//  public void clearLog4J() {
//    //The lucene test ensures that no system properties
//    //have been modified by the test. GFE leaves this property
//    //set
//    System.clearProperty("log4j.configurationFile");
//  }
  
  protected Directory getDirectory(Path path) throws IOException {
    
    //This is super lame, but log4j automatically sets the system property, and the lucene
    //test asserts that no system properties have changed. Unfortunately, there is no
    //way to control the order of rules, so we can't clear this property with a rule
    //or @After method. Instead, do it in the close method of the directory.
    return new RegionDirectory(new ConcurrentHashMap<String, File>(), new ConcurrentHashMap<ChunkKey, byte[]>());
  }
}
