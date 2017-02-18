package org.apache.geode.redis.internal.executor.hash;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.buffer.UnpooledByteBufAllocator;

/**
 * Test for the HDelExecutor class
 * 
 * @author Gregory Green
 *
 */
public class HDelExecutorTest
{
	/**
	 * Test the execute command method
	 */
	@Test
	public void testExecuteCommand() {
		Command command = Mockito.mock(Command.class);
		ExecutionHandlerContext context = Mockito.mock(ExecutionHandlerContext.class);
		

		UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
		Mockito.when(context.getByteBufAllocator()).thenReturn(byteBuf);
		
		HDelExecutor exec = new HDelExecutor();
		
		exec.executeCommand(command, context);
		
		//verify the response was set
		Mockito.verify(command).setResponse(Mockito.any());
	}

}
