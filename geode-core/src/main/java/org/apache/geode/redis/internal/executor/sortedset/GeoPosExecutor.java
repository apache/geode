package org.apache.geode.redis.internal.executor.sortedset;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.StringWrapper;
import org.apache.geode.redis.internal.org.apache.hadoop.fs.GeoCoord;

import java.util.ArrayList;
import java.util.List;

public class GeoPosExecutor extends GeoSortedSetExecutor {

    @Override
    public void executeCommand(Command command, ExecutionHandlerContext context) {
        List<byte[]> commandElems = command.getProcessedCommand();
        ByteArrayWrapper key = command.getKey();

        if (commandElems.size() < 3) {
            command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.GEOHASH));
            return;
        }

        List<GeoCoord> positions = new ArrayList<>();
        Region<ByteArrayWrapper, StringWrapper> keyRegion = getRegion(context, key);

        for (int i = 2; i < commandElems.size(); i++) {
            byte[] member = commandElems.get(i);

            StringWrapper hashWrapper = keyRegion.get(new ByteArrayWrapper(member));
            if (hashWrapper != null) {
                positions.add(Coder.geoPos(hashWrapper.toString()));
            } else {
                positions.add(null);
            }
        }

        command.setResponse(Coder.getBulkStringGeoCoordinateArrayResponse(context.getByteBufAllocator(), positions));
    }
}
