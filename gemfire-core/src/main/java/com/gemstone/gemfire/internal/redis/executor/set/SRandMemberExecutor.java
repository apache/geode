package com.gemstone.gemfire.internal.redis.executor.set;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.Coder;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.RedisConstants.ArityDef;

public class SRandMemberExecutor extends SetExecutor {

  private final static String ERROR_NOT_NUMERIC = "The count provided must be numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SRANDMEMBER));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, Boolean> keyRegion = (Region<ByteArrayWrapper, Boolean>) context.getRegionProvider().getRegion(key);

    int count = 1;

    if (commandElems.size() > 2) {
      try {
        count = Coder.bytesToInt(commandElems.get(2));
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
        return;
      }
    }

    if (keyRegion == null || count == 0) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    int members = keyRegion.size();

    if (members <= count && count != 1) {
      command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), new HashSet<ByteArrayWrapper>(keyRegion.keySet())));
      return;
    }

    Random rand = new Random();

    ByteArrayWrapper[] entries = keyRegion.keySet().toArray(new ByteArrayWrapper[members]);

    if (count == 1) {
      ByteArrayWrapper randEntry = entries[rand.nextInt(entries.length)];
      command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), randEntry.toBytes()));
    } else if (count > 0) {
      Set<ByteArrayWrapper> randEntries = new HashSet<ByteArrayWrapper>();
      do {
        ByteArrayWrapper s = entries[rand.nextInt(entries.length)];
        randEntries.add(s);
      } while(randEntries.size() < count);
      command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), randEntries));
    } else {
      count = -count;
      List<ByteArrayWrapper> randEntries = new ArrayList<ByteArrayWrapper>();
      for (int i = 0; i < count; i++) {
        ByteArrayWrapper s = entries[rand.nextInt(entries.length)];
        randEntries.add(s);
      }
      command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), randEntries));
    }
  }
}
