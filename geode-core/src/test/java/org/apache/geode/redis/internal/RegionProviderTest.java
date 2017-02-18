package org.apache.geode.redis.internal;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.hll.HyperLogLogPlus;
import org.apache.geode.redis.internal.executor.hash.HashInterpreter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.codearte.catchexception.shade.mockito.Mockito;

public class RegionProviderTest {

	private String NEW_REGION_NM = "NEW_REGION";
	
  private RegionProvider regionProvider;

  private ExecutionHandlerContext context;
  
  private Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> hashRegion;
  private Region<ByteArrayWrapper, Set<ByteArrayWrapper>> setRegion;
  
  /**
   * Setup data, objects mocks for the test case 
   */
  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
	  
	  Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion = Mockito.mock(Region.class);
	  
      Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion = Mockito.mock(Region.class);
      Region<String, RedisDataType> redisMetaRegion = Mockito.mock(Region.class);
      
      ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap = Mockito.mock(ConcurrentMap.class);
      ScheduledExecutorService expirationExecutor = Mockito.mock(ScheduledExecutorService.class);
      Cache cache = Mockito.mock(Cache.class);
      Region<Object,Object> newRegion = org.mockito.Mockito.mock(Region.class);
      
      setRegion = Mockito.mock(Region.class);
      
      Mockito.when(cache.getRegion(NEW_REGION_NM)).thenReturn(newRegion);
      
      RegionShortcut defaultShortcut = RegionShortcut.PARTITION;
    		  
      hashRegion = Mockito.mock(Region.class);
      
      regionProvider = new RegionProvider(stringsRegion, hLLRegion, redisMetaRegion, expirationsMap, expirationExecutor, defaultShortcut, hashRegion,setRegion,cache);
      context = Mockito.mock(ExecutionHandlerContext.class);
	  
  }
  
  /**
   * Test the RegionProvide.getRegion
   */
  @Test
  public void testGetRegion() {
    
	  Assert.assertNull(regionProvider.getRegion(null));
	  
	  Assert.assertNull(regionProvider.getRegion(Coder.stringToByteArrayWrapper("invalid")));
  }

  /**
   * Test RegionProvider get or create region method
   */
  @Test
  public void testGetOrCreateRegion() {
	  
	  Region<?,?> region = regionProvider.getOrCreateRegion(Coder.stringToByteArrayWrapper(NEW_REGION_NM), RedisDataType.REDIS_HASH, context);
	  assertNotNull(region);
	  
	  Region<?,?> sameregion = regionProvider.getRegion(Coder.stringToByteArrayWrapper(NEW_REGION_NM));
	  assertNotNull(region);
	
	  assertTrue(sameregion == region);
	  
	  assertTrue(hashRegion == regionProvider.getOrCreateRegion(HashInterpreter.REGION_HASH_REGION, RedisDataType.REDIS_HASH, context));
	  
   }

}
