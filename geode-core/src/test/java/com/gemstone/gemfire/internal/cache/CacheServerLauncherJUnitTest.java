package com.gemstone.gemfire.internal.cache; 

import static com.gemstone.gemfire.internal.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/** 
* CacheServerLauncher Tester. 
*/
@Category(UnitTest.class)
public class CacheServerLauncherJUnitTest {

  @Test
  public void testSafeEquals(){
    String string1 = "string1";

    String string2 = string1;

    @SuppressWarnings("RedundantStringConstructorCall")
    String string3 = new String(string1);

    assertTrue(CacheServerLauncher.safeEquals(string1, string2));
    assertTrue(CacheServerLauncher.safeEquals(string1, string3));
    assertTrue(CacheServerLauncher.safeEquals(null, null));
    assertFalse(CacheServerLauncher.safeEquals(null, string3));
    assertFalse(CacheServerLauncher.safeEquals(string1, null));

    
  }


} 
