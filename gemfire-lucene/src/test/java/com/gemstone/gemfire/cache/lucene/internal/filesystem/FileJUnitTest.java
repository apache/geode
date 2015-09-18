package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FileJUnitTest {

  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    File file = new File(null, "fileName");
    file.modified = -10;
    file.length = 5;
    file.chunks = 7;
    File copy = CopyHelper.deepCopy(file);
    
    assertEquals(file.chunks, copy.chunks);
    assertEquals(file.created, copy.created);
    assertEquals(file.modified, copy.modified);
    assertEquals(file.getName(), copy.getName());
    assertEquals(file.length, copy.length);
    assertEquals(file.id, copy.id);
  }

}
