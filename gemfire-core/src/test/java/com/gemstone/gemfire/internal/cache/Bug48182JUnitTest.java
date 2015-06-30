package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * TestCase that emulates the conditions that produce defect 48182 and ensures that the fix works under those conditions.
 * 48182: Unexpected EntryNotFoundException while shutting down members with off-heap
 * https://svn.gemstone.com/trac/gemfire/ticket/48182 
 * @author rholmes
 */
@Category(IntegrationTest.class)
public class Bug48182JUnitTest {
  /**
   * A region entry key.
   */
  private static final String KEY = "KEY";
  
  /**
   * A region entry value.
   */
  private static final String VALUE = " Vestibulum quis lobortis risus. Cras cursus eget dolor in facilisis. Curabitur purus arcu, dignissim ac lorem non, venenatis condimentum tellus. Praesent at erat dapibus, bibendum nunc sed, congue nulla";
  
  /**
   * A cache.
   */
  private GemFireCacheImpl cache = null;


  @Before
  public void setUp() throws Exception {
    // Create our cache
    this.cache = createCache();
  }

  @After
  public void tearDown() throws Exception {
    // Cleanup our cache
    closeCache(this.cache);
  }

  /**
   * @return the test's cache.
   */
  protected GemFireCacheImpl getCache() {
    return this.cache;
  }
  
  /**
   * Close a cache.
   * @param gfc the cache to close.
   */
  protected void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
  }
  
  /**
   * @return the test's off heap memory size.
   */
  protected String getOffHeapMemorySize() {
    return "2m";
  }
  
  /**
   * @return the type of region for the test.
   */
  protected RegionShortcut getRegionShortcut() {
    return RegionShortcut.REPLICATE;
  }
  
  /**
   * @return the region containing our test data.
   */
  protected String getRegionName() {
    return "region1";
  }
  
  /**
   * Creates and returns the test region with concurrency checks enabled.
   */
  protected Region<Object,Object> createRegion() {
    return createRegion(true);
  }
  
  /**
   * Creates and returns the test region.
   * @param concurrencyChecksEnabled concurrency checks will be enabled if true.
   */
  protected Region<Object,Object> createRegion(boolean concurrencyChecksEnabled) {
    return getCache().createRegionFactory(getRegionShortcut()).setOffHeap(true).setConcurrencyChecksEnabled(concurrencyChecksEnabled).create(getRegionName());    
  }

  /**
   * Creates and returns the test cache.
   */
  protected GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty("locators", "");
    props.setProperty("mcast-port", "0");
    props.setProperty("off-heap-memory-size", getOffHeapMemorySize());
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    return result;
  }

  /**
   * Simulates the conditions for 48182 by setting a test hook boolean in {@link AbstractRegionMap}.  This test 
   * hook forces a cache close during a destroy in an off-heap region.  This test asserts that a CacheClosedException
   * is thrown rather than an EntryNotFoundException (or any other exception type for that matter).
   */
  @Test
  public void test48182WithCacheClose() throws Exception {
    AbstractRegionMap.testHookRunnableFor48182 = new Runnable() {
      @Override
      public void run() {
        getCache().close();
      }      
    };
    
    // True when the correct exception has been triggered.
    boolean correctException = false;
    
    Region<Object,Object> region = createRegion();
    region.put(KEY, VALUE);
    
    try {
      region.destroy(KEY);
    } catch(CacheClosedException e) {
      correctException = true;
//      e.printStackTrace();
    } catch(Exception e) {
//      e.printStackTrace();
      fail("Did not receive a CacheClosedException.  Received a " + e.getClass().getName() + " instead.");
    }
    
    assertTrue("A CacheClosedException was not triggered",correctException);
  }  

  /**
   * Simulates the conditions similar to 48182 by setting a test hook boolean in {@link AbstractRegionMap}.  This test 
   * hook forces a region destroy during a destroy operation in an off-heap region.  This test asserts that a RegionDestroyedException
   * is thrown rather than an EntryNotFoundException (or any other exception type for that matter).
   */
  @Test
  public void test48182WithRegionDestroy() throws Exception {
    AbstractRegionMap.testHookRunnableFor48182 = new Runnable() {
      @Override
      public void run() {
        getCache().getRegion(getRegionName()).destroyRegion();
      }      
    };

    // True when the correct exception has been triggered.
    boolean correctException = false;
    
    Region<Object,Object> region = createRegion();
    region.put(KEY, VALUE);
    
    try {
      region.destroy(KEY);
    } catch(RegionDestroyedException e) {
      correctException = true;
//      e.printStackTrace();
    } catch(Exception e) {
//      e.printStackTrace();
      fail("Did not receive a RegionDestroyedException.  Received a " + e.getClass().getName() + " instead.");
    }

    assertTrue("A RegionDestroyedException was not triggered",correctException);    
  }
}
