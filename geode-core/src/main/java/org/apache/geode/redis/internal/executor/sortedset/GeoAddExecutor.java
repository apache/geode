package org.apache.geode.redis.internal.executor.sortedset;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.GeoCoder;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.StringWrapper;

public class GeoAddExecutor extends GeoSortedSetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    int numberOfAdds = 0;
    List<byte[]> commandElems = command.getProcessedCommand();
    ByteArrayWrapper key = command.getKey();

    if (commandElems.size() < 5 || ((commandElems.size() - 2) % 3) != 0) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.GEOADD));
      return;
    }

    Region<ByteArrayWrapper, StringWrapper> keyRegion =
        getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);

    for (int i = 2; i < commandElems.size(); i += 3) {
      byte[] longitude = commandElems.get(i);
      byte[] latitude = commandElems.get(i + 1);
      byte[] member = commandElems.get(i + 2);

      String score;
      score = GeoCoder.geoHash(longitude, latitude);
      Object oldVal = keyRegion.put(new ByteArrayWrapper(member), new StringWrapper(score));

      if (oldVal == null)
        numberOfAdds++;
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numberOfAdds));
  }
}
