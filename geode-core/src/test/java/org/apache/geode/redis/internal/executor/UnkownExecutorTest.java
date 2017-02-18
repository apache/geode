package org.apache.geode.redis.internal.executor;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.junit.Test;
import org.mockito.Mockito;
import io.netty.buffer.UnpooledByteBufAllocator;

/**
 * Test for the UnkownExecutor
 * @author Gregory Green
 *
 */
public class UnkownExecutorTest
{
	/**
	 * Test the execution method
	 */
	@Test
	public void testExecuteCommand() {
		UnkownExecutor exe = new UnkownExecutor();
		
		Command command = Mockito.mock(Command.class);
		ExecutionHandlerContext context = Mockito.mock(ExecutionHandlerContext.class);
		
		
		UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
		Mockito.when(context.getByteBufAllocator()).thenReturn(byteBuf);
		
		exe.executeCommand(command, context);
		
		//verify the response was set
		Mockito.verify(command).setResponse(Mockito.any());
		
	}

}
