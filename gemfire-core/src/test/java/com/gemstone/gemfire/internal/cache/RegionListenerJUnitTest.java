package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;

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
public class RegionListenerJUnitTest {

  @Test
  public void test() {
    final AtomicBoolean afterCreateInvoked = new AtomicBoolean(); 
    RegionListener listener = new RegionListener() {
      
      @Override
      public RegionAttributes beforeCreate(Region parent, String regionName,
          RegionAttributes attrs, InternalRegionArguments internalRegionArgs) {
        AttributesFactory newAttrsFactory = new AttributesFactory(attrs);
        newAttrsFactory.setDataPolicy(DataPolicy.EMPTY);
        return newAttrsFactory.create();
      }

      @Override
      public void afterCreate(Region region) {
        afterCreateInvoked.set(true);
      }
    };
    
    GemFireCacheImpl cache = (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").create();
    cache.addRegionListener(listener);
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
    assertEquals(DataPolicy.EMPTY, region.getAttributes().getDataPolicy());
    assertTrue(afterCreateInvoked.get());
  }

}
