package org.apache.geode.redis.internal.executor.set;

import static org.junit.Assert.*;

import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RegionProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test case for the Set interpreter test
 * @author Gregory Green
 *
 */
public class SetInterpreterTest
{	private ExecutionHandlerContext context;
	private RegionProvider regionProvider;
	
	@SuppressWarnings("rawtypes")
	private Region setRegion;
	
	
	/**
	 * Setup the mock and test data
	 */
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		
		context = Mockito.mock(ExecutionHandlerContext.class);
		regionProvider = Mockito.mock(RegionProvider.class);
		setRegion = Mockito.mock(Region.class);
		
		Mockito.when(context.getRegionProvider()).thenReturn(regionProvider);

		Mockito.when(regionProvider.getSetRegion()).thenReturn(setRegion);

	}
	/**
	 * Test the get region method
	 */
	@Test
	public void testGetRegion() {
		Region<ByteArrayWrapper,Set<ByteArrayWrapper>> region = SetInterpreter.getRegion(context);
		assertNotNull(region);
		
		assertEquals(setRegion, region);
		
	}

}
