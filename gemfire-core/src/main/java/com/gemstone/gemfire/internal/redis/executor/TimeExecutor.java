package com.gemstone.gemfire.internal.redis.executor;

import io.netty.buffer.ByteBuf;

import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;

public class TimeExecutor extends AbstractExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    long timeStamp = System.currentTimeMillis();
    long seconds = timeStamp / 1000;
    long microSeconds = (timeStamp - (seconds * 1000)) * 1000;
    byte[] secAr = Coder.longToBytes(seconds);
    byte[] micAr = Coder.longToBytes(microSeconds);

    ByteBuf response = context.getByteBufAllocator().buffer(50);
    response.writeByte(Coder.ARRAY_ID);
    response.writeByte(50); // #2
    response.writeBytes(Coder.CRLFar);
    response.writeByte(Coder.BULK_STRING_ID);
    response.writeBytes(Coder.intToBytes(secAr.length));
    response.writeBytes(Coder.CRLFar);
    response.writeBytes(secAr);
    response.writeBytes(Coder.CRLFar);
    response.writeByte(Coder.BULK_STRING_ID);
    response.writeBytes(Coder.intToBytes(micAr.length));
    response.writeBytes(Coder.CRLFar);
    response.writeBytes(micAr);
    response.writeBytes(Coder.CRLFar);
    command.setResponse(response);    
  }
}
