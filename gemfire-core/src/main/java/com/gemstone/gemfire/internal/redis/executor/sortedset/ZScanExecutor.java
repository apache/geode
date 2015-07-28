package com.gemstone.gemfire.internal.redis.executor.sortedset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.DoubleWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.executor.AbstractScanExecutor;

public class ZScanExecutor extends AbstractScanExecutor {

  @SuppressWarnings("unchecked")
  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZSCAN));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = (Region<ByteArrayWrapper, DoubleWrapper>) context.getRegionProvider().getRegion(key);
    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    if (keyRegion == null) {
      command.setResponse(Coder.getScanResponse(context.getByteBufAllocator(), new ArrayList<String>()));
      return;
    }
    byte[] cAr = commandElems.get(2);
    //String cursorString = ResponseToByteEncoder.bytesToString(cAr);

    int cursor = 0;
    Pattern matchPattern = null;
    String globMatchPattern = null;
    int count = DEFUALT_COUNT;
    try {
      cursor = Coder.bytesToInt(cAr);
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
        if (Coder.bytesToString(bytes).equalsIgnoreCase("MATCH")) {
          bytes = commandElems.get(4);
          globMatchPattern = Coder.bytesToString(bytes);
        } else if (Coder.bytesToString(bytes).equalsIgnoreCase("COUNT")) {
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
        if (Coder.bytesToString(bytes).equalsIgnoreCase("COUNT")) {
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

    List<ByteArrayWrapper> returnList = (List<ByteArrayWrapper>) getIteration(new HashSet(keyRegion.entrySet()), matchPattern, count, cursor);

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
    for (Entry<ByteArrayWrapper, DoubleWrapper> entry: (Collection<Entry<ByteArrayWrapper, DoubleWrapper>>) list) {
      ByteArrayWrapper keyWrapper = entry.getKey();
      String key = keyWrapper.toString();

      DoubleWrapper value = entry.getValue();
      i++;
      if (beforeCursor < cursor) {
        beforeCursor++;
        continue;
      } else if (numElements < count) {
        if (matchPattern != null) {
          if (matchPattern.matcher(key).matches()) {
            returnList.add(keyWrapper);
            returnList.add(value.toString());
            numElements++;
          }
        } else {
          returnList.add(keyWrapper);
          returnList.add(value.toString());
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
