package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.executor.AbstractScanExecutor;

public class SScanExecutor extends AbstractScanExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SSCAN));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_SET, context);
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, Boolean> keyRegion = (Region<ByteArrayWrapper, Boolean>) context.getRegionProvider().getRegion(key);
    if (keyRegion == null) {
      command.setResponse(Coder.getScanResponse(context.getByteBufAllocator(), new ArrayList<String>()));
      return;
    }
    byte[] cAr = commandElems.get(2);
    String cursorString = Coder.bytesToString(cAr);
    int cursor = 0;
    Pattern matchPattern = null;
    String globMatchPattern = null;
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

    if (commandElems.size() > 4) {
      try {
        byte[] bytes = commandElems.get(3);
        String tmp = Coder.bytesToString(bytes);
        if (tmp.equalsIgnoreCase("MATCH")) {
          bytes = commandElems.get(4);
          globMatchPattern = Coder.bytesToString(bytes);
        } else if (tmp.equalsIgnoreCase("COUNT")) {
          bytes = commandElems.get(4);
          count = Coder.bytesToInt(bytes);
        }
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_COUNT));
        return;
      }
    }

    if (commandElems.size() > 6) {
      try {
        byte[] bytes = commandElems.get(5);
        String tmp = Coder.bytesToString(bytes);
        if (tmp.equalsIgnoreCase("COUNT")) {
          bytes = commandElems.get(6);
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
      matchPattern = convertGlobToRegex(globMatchPattern);
    } catch (PatternSyntaxException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ERROR_ILLEGAL_GLOB));
      return;
    }

    @SuppressWarnings("unchecked")
    List<ByteArrayWrapper> returnList = (List<ByteArrayWrapper>) getIteration(new ArrayList(keyRegion.keySet()), matchPattern, count, cursor);

    command.setResponse(Coder.getScanResponse(context.getByteBufAllocator(), returnList));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<?> getIteration(Collection<?> list, Pattern matchPattern, int count, int cursor) {
    List<Object> returnList = new ArrayList<Object>();
    int size = list.size();
    int beforeCursor = 0;
    int numElements = 0;
    int i = -1;
    for (ByteArrayWrapper value: (Collection<ByteArrayWrapper>) list) {
      String key = Coder.bytesToString(value.toBytes());
      i++;
      if (beforeCursor < cursor) {
        beforeCursor++;
        continue;
      } else if (numElements < count) {
        if (matchPattern != null) {
          if (matchPattern.matcher(key).matches()) {
            returnList.add(value);
            numElements++;
          }
        } else {
          returnList.add(value);
          numElements++;
        }
      } else
        break;
    }

    if (i == size - 1)
      returnList.add(0, String.valueOf(0));
    else
      returnList.add(0, String.valueOf(i));
    return returnList;
  }
}
