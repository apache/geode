package com.gemstone.gemfire.internal.redis.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.redis.GemFireRedisServer;

public class ScanExecutor extends AbstractScanExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SCAN));
      return;
    }

    String cursorString = command.getStringKey();
    int cursor = 0;
    Pattern matchPattern = null;
    String globMatchString = null;
    int count = DEFUALT_COUNT;
    try {
      cursor = Integer.parseInt(cursorString);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_CURSOR));
      return;
    }
    if (cursor < 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_CURSOR));
      return;
    }

    if (commandElems.size() > 3) {
      try {
        byte[] bytes = commandElems.get(2);
        String tmp = Coder.bytesToString(bytes);
        if (tmp.equalsIgnoreCase("MATCH")) {
          bytes = commandElems.get(3);
          globMatchString = Coder.bytesToString(bytes);
        } else if (tmp.equalsIgnoreCase("COUNT")) {
          bytes = commandElems.get(3);
          count = Coder.bytesToInt(bytes);
        }
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_COUNT));
        return;
      }
    }

    if (commandElems.size() > 5) {
      try {
        byte[] bytes = commandElems.get(4);
        String tmp = Coder.bytesToString(bytes);
        if (tmp.equalsIgnoreCase("COUNT")) {
          bytes = commandElems.get(5);
          count = Coder.bytesToInt(bytes);
        }
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_COUNT));
        return;
      }
    }

    if (count < 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_COUNT));
      return;
    }

    try {
      matchPattern = convertGlobToRegex(globMatchString);
    } catch (PatternSyntaxException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_ILLEGAL_GLOB));
      return;
    }

    @SuppressWarnings("unchecked")
    List<String> returnList = (List<String>) getIteration(context.getRegionProvider().metaKeySet(), matchPattern, count, cursor);

    command.setResponse(Coder.getScanResponse(context.getByteBufAllocator(), returnList));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<?> getIteration(Collection<?> list, Pattern matchPattern, int count, int cursor) {
    List<String> returnList = new ArrayList<String>();
    int size = list.size();
    int beforeCursor = 0;
    int numElements = 0;
    int i = -1;
    for (String key: (Collection<String>) list) {
      if (key.equals(GemFireRedisServer.REDIS_META_DATA_REGION) || key.equals(GemFireRedisServer.STRING_REGION) || key.equals(GemFireRedisServer.HLL_REGION))
        continue;
      i++;
      if (beforeCursor < cursor) {
        beforeCursor++;
        continue;
      } else if (numElements < count) {
        if (matchPattern != null) {
          if (matchPattern.matcher(key).matches()) {
            returnList.add(key);
            numElements++;
          }
        } else {
          returnList.add(key);
          numElements++;
        }
      } else
        break;
    }

    if (i == size - (NUM_DEFAULT_REGIONS + 1))
      returnList.add(0, String.valueOf(0));
    else
      returnList.add(0, String.valueOf(i));
    return returnList;
  }

}
