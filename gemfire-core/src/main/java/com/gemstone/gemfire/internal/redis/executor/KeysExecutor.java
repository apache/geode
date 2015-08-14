package com.gemstone.gemfire.internal.redis.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.org.apache.hadoop.fs.GlobPattern;
import com.gemstone.gemfire.redis.GemFireRedisServer;

public class KeysExecutor extends AbstractExecutor {
  
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.KEYS));
      return;
    }

    String glob = Coder.bytesToString(commandElems.get(1));
    Set<String> allKeys = context.getRegionProvider().metaKeySet();
    List<String> matchingKeys = new ArrayList<String>();

    Pattern pattern;
    try {
      pattern = GlobPattern.compile(glob);
    } catch (PatternSyntaxException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_ILLEGAL_GLOB));
      return;
    }

    for (String key: allKeys) {
      if (!(key.equals(GemFireRedisServer.REDIS_META_DATA_REGION) ||
              key.equals(GemFireRedisServer.STRING_REGION) ||
              key.equals(GemFireRedisServer.HLL_REGION))
              && pattern.matcher(key).matches())
        matchingKeys.add(key);
    }

    if (matchingKeys.isEmpty()) 
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
    else
      command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), matchingKeys));


  }
}
