package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneResultStructImpJUnitTest {

  @Test
  public void hashCodeAndEquals() {
    
    //Create 2 equal structs
    LuceneResultStructImpl<String, String> result1 = new LuceneResultStructImpl<String, String>("key1", "value1", 5);
    LuceneResultStructImpl<String, String> result2 = new LuceneResultStructImpl<String, String>("key1", "value1", 5);
    assertEquals(result1, result1);
    assertEquals(result1, result2);
    assertEquals(result1.hashCode(), result2.hashCode());
    
    //And some unequal ones
    LuceneResultStructImpl<String, String> result3 = new LuceneResultStructImpl<String, String>("key2", "value1", 5);
    LuceneResultStructImpl<String, String> result4 = new LuceneResultStructImpl<String, String>("key1", "value2", 5);
    LuceneResultStructImpl<String, String> result5 = new LuceneResultStructImpl<String, String>("key1", "value1", 6);
    assertNotEquals(result1, result3);
    assertNotEquals(result1, result4);
    assertNotEquals(result1, result5);
  }

}
