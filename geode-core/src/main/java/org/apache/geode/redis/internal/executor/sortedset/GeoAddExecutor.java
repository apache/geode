package org.apache.geode.redis.internal.executor.sortedset;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.*;

import java.util.List;

public class GeoAddExecutor extends GeoSortedSetExecutor {

    @Override
    public void executeCommand(Command command, ExecutionHandlerContext context) {
        int numberOfAdds = 0;
        List<byte[]> commandElems = command.getProcessedCommand();
        ByteArrayWrapper key = command.getKey();

        byte[] longitude = commandElems.get(2);
        byte[] latitude = commandElems.get(3);
        byte[] member = commandElems.get(4);

        String score;
        score = Coder.geoHash(longitude, latitude);

        Region<ByteArrayWrapper, StringWrapper> keyRegion =
                getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);
        Object oldVal = keyRegion.put(new ByteArrayWrapper(member), new StringWrapper(score));

        if (oldVal == null)
            numberOfAdds = 1;

        command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numberOfAdds));
    }
}
