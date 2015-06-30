package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test off-heap regions with indexes.
 * 
 * @author darrel
 *
 */
@Category(IntegrationTest.class)
public class OffHeapIndexJUnitTest {
  private GemFireCacheImpl gfc;
  
  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty("locators", "");
    props.setProperty("mcast-port", "0");
    props.setProperty("off-heap-memory-size", "100m");
    this.gfc = (GemFireCacheImpl) new CacheFactory(props).create();
  }
  @After
  public void tearDown() {
    this.gfc.close();
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    // TODO cleanup default disk store files
  }
  
  @Test
  public void testUnsupportedAsyncIndexes() throws RegionNotFoundException, IndexInvalidException, IndexNameConflictException, IndexExistsException {
    RegionFactory<Object, Object> rf = this.gfc.createRegionFactory();
    rf.setOffHeap(true);
    rf.setIndexMaintenanceSynchronous(false);
    rf.create("r");
    QueryService qs = this.gfc.getQueryService();
    try {
      qs.createIndex("idx", "age", "/r");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertEquals("Asynchronous index maintenance is currently not supported for off-heap regions. The off-heap region is /r", expected.getMessage());
    }
  }
  @Test
  public void testUnsupportedMultiIteratorIndexes() throws RegionNotFoundException, IndexInvalidException, IndexNameConflictException, IndexExistsException {
    RegionFactory<Object, Object> rf = this.gfc.createRegionFactory();
    rf.setOffHeap(true);
    rf.setIndexMaintenanceSynchronous(true);
    rf.create("r");
    QueryService qs = this.gfc.getQueryService();
    try {
      qs.createIndex("idx", "addr", "/r r, r.addresses addr");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertEquals("From clauses having multiple iterators(collections) are not supported for off-heap regions. The off-heap region is /r", expected.getMessage());
    }
  }
}
