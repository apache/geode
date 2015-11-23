package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CacheServiceJUnitTest {
  
  private GemFireCacheImpl cache;

  @Before
  public void setUp() {
    cache = (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").create();
  }
  
  @After
  public void tearDown() {
    if(cache != null) {
      cache.close();
    }
  }

  @Test
  public void test() {
    MockCacheService service = cache.getService(MockCacheService.class);
    assertEquals(cache, service.getCache());
  }

}
