package org.apache.geode.redis.internal.executor.hash;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RegionProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.buffer.UnpooledByteBufAllocator;

/**
 * Test case for HashInterpreter
 * @author Gregory Green
 *
 */
public class HashInterpreterTest
{
	private Command command;
	private ExecutionHandlerContext context;
	private UnpooledByteBufAllocator byteBuf;
	private RegionProvider regionProvider;
	private ByteArrayWrapper NON_NAMED_HASH_KEY = Coder.stringToByteArrayWrapper("junit");
	private ByteArrayWrapper NAMED_HASH_KEY = Coder.stringToByteArrayWrapper("myObject:junit");
	
	@SuppressWarnings("rawtypes")
	private Region redisRegion;
	
	@SuppressWarnings("rawtypes")
	private Region namedRegion;
	
	
	/**
	 * Setup the mock and test data
	 */
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		command = Mockito.mock(Command.class);
		context = Mockito.mock(ExecutionHandlerContext.class);
		regionProvider = Mockito.mock(RegionProvider.class);
		redisRegion = Mockito.mock(Region.class);
		namedRegion = Mockito.mock(Region.class);
		
		Mockito.when(context.getRegionProvider()).thenReturn(regionProvider);

		Mockito.when(regionProvider.getHashRegion()).thenReturn(redisRegion);
		byteBuf = new UnpooledByteBufAllocator(false);
		Mockito.when(context.getByteBufAllocator()).thenReturn(byteBuf);
		
		Mockito.when(regionProvider.getOrCreateRegion( Mockito.any(), Mockito.any(),  Mockito.any())).thenReturn(namedRegion);
		System.out.println("command"+command);
	}
	
	/**
	 * Test the get region method
	 */
	@Test
	public void testGetRegion() {
		
		Region<ByteArrayWrapper, Map<ByteArrayWrapper,ByteArrayWrapper>> region = HashInterpreter.getRegion(NON_NAMED_HASH_KEY, context);
		
		assertNotNull(region);
		
		assertTrue(redisRegion == region);
		
		 region = HashInterpreter.getRegion(null, context);
		 assertNull(region);
		 
		 region = HashInterpreter.getRegion(NAMED_HASH_KEY, context);
		 assertNotNull(region);
		 
	}

	@Test
	public void testToRegionNameByteArray() {
		
		assertEquals(HashInterpreter.REGION_HASH_REGION,
				HashInterpreter.toRegionNameByteArray(
						new ByteArrayWrapper("unnamed".getBytes(StandardCharsets.UTF_8))));
		
		
		assertEquals(Coder.stringToByteArrayWrapper("companies"),
				HashInterpreter.toRegionNameByteArray(
						new ByteArrayWrapper("companies:unnamed".getBytes(StandardCharsets.UTF_8))));
		
		
		assertEquals(Coder.stringToByteArrayWrapper("persons"),
				HashInterpreter.toRegionNameByteArray(
						new ByteArrayWrapper(" persons:unnamed".getBytes(StandardCharsets.UTF_8))));
		
		assertEquals(Coder.stringToByteArrayWrapper("users"),
				HashInterpreter.toRegionNameByteArray(
						new ByteArrayWrapper(" users :unnamed".getBytes(StandardCharsets.UTF_8))));
		
		assertEquals(HashInterpreter.REGION_HASH_REGION,
				HashInterpreter.toRegionNameByteArray(
						new ByteArrayWrapper("  :unnamed".getBytes(StandardCharsets.UTF_8))));


		
		assertEquals(HashInterpreter.REGION_HASH_REGION,
				HashInterpreter.toRegionNameByteArray(
						new ByteArrayWrapper(":keys".getBytes(StandardCharsets.UTF_8))));
		

		assertEquals(HashInterpreter.REGION_HASH_REGION,
				HashInterpreter.toRegionNameByteArray(
						new ByteArrayWrapper(": keys".getBytes(StandardCharsets.UTF_8))));
	}

	@Test
	public void testToEntryKey() {
		assertEquals(Coder.stringToByteArrayWrapper("keys"),
				HashInterpreter.toEntryKey(
						new ByteArrayWrapper("companieses:keys".getBytes(StandardCharsets.UTF_8))));

		assertEquals(Coder.stringToByteArrayWrapper("keys"),
				HashInterpreter.toEntryKey(
						new ByteArrayWrapper(":keys".getBytes(StandardCharsets.UTF_8))));


		assertEquals(Coder.stringToByteArrayWrapper(" keys"),
				HashInterpreter.toEntryKey(
						new ByteArrayWrapper(": keys".getBytes(StandardCharsets.UTF_8))));
		
		assertEquals(Coder.stringToByteArrayWrapper(" keys "),
				HashInterpreter.toEntryKey(
						new ByteArrayWrapper(": keys ".getBytes(StandardCharsets.UTF_8))));

		assertEquals(Coder.stringToByteArrayWrapper(" keys "),
				HashInterpreter.toEntryKey(
						new ByteArrayWrapper(" keys ".getBytes(StandardCharsets.UTF_8))));
		
		assertEquals(Coder.stringToByteArrayWrapper("green"),
				HashInterpreter.toEntryKey(
						new ByteArrayWrapper("green".getBytes(StandardCharsets.UTF_8))));
	}
}
