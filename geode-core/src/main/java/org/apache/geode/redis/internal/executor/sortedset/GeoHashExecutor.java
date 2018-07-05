package org.apache.geode.redis.internal.executor.sortedset;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.StringWrapper;

import java.util.ArrayList;
import java.util.List;

public class GeoHashExecutor extends GeoSortedSetExecutor {

    @Override
    public void executeCommand(Command command, ExecutionHandlerContext context) {
        List<byte[]> commandElems = command.getProcessedCommand();
        ByteArrayWrapper key = command.getKey();

        if (commandElems.size() < 3) {
            command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.GEOHASH));
            return;
        }

        List<String> hashes = new ArrayList<>();
        for (int i = 2; i < commandElems.size(); i++) {
            byte[] member = commandElems.get(i);

            Region<ByteArrayWrapper, StringWrapper> keyRegion =
                    getRegion(context, key);
            StringWrapper hashWrapper = keyRegion.get(new ByteArrayWrapper(member));
            if (hashWrapper != null) {
                hashes.add(hashWrapper.toString());
            } else {
                hashes.add(null);
            }
        }

        command.setResponse(Coder.getBulkStringArrayResponse(context.getByteBufAllocator(), hashes));
    }
}
