package org.apache.geode.redis.internal.executor;

import static org.junit.Assert.*;

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.string.SetExecutor;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for AbstractExecutor
 * @author Gregory Green
 *
 */
public class AbstractExecutorTest
{

	/**
	 * Test the remove entry mehtod
	 */
	@Test
	public void testRemoveEntry()
	{
		//Create any instance of the AbstractExecutor
		AbstractExecutor abstractExecutor = new SetExecutor();
		
		//setup mocks
		ExecutionHandlerContext context = Mockito.mock(ExecutionHandlerContext.class);
		RegionProvider rp = Mockito.mock(RegionProvider.class);
		Mockito.when(context.getRegionProvider()).thenReturn(rp);
		Mockito.when(rp.removeKey(Mockito.any())).thenReturn(true);
		
		//Assert false to protected or null types
		assertFalse(abstractExecutor.removeEntry(
				Coder.stringToByteArrayWrapper("junit"), RedisDataType.REDIS_PROTECTED, context));
		
		assertFalse(abstractExecutor.removeEntry(
				Coder.stringToByteArrayWrapper("junit"), null, context));
	
		
		
		
	}

}
