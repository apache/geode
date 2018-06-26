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

        for (int i = 2; i < commandElems.size(); i+=3) {
            byte[] longitude = commandElems.get(i);
            byte[] latitude = commandElems.get(i+1);
            byte[] member = commandElems.get(i+2);

            String score;
            score = Coder.geoHash(longitude, latitude);

            Region<ByteArrayWrapper, StringWrapper> keyRegion =
                    getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);
            Object oldVal = keyRegion.put(new ByteArrayWrapper(member), new StringWrapper(score));

            if (oldVal == null)
                numberOfAdds++;
        }

        command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numberOfAdds));
    }
}
