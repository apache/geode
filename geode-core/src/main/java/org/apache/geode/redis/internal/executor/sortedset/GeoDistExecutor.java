package org.apache.geode.redis.internal.executor.sortedset;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.StringWrapper;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.List;

public class GeoDistExecutor extends GeoSortedSetExecutor {

    @Override
    public void executeCommand(Command command, ExecutionHandlerContext context) {
        List<byte[]> commandElems = command.getProcessedCommand();
        ByteArrayWrapper key = command.getKey();

        if (commandElems.size() < 4 || commandElems.size() > 5) {
            command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.GEODIST));
            return;
        }

        Region<ByteArrayWrapper, StringWrapper> keyRegion = getRegion(context, key);
        StringWrapper hw1 = keyRegion.get(new ByteArrayWrapper(commandElems.get(2)));
        StringWrapper hw2 = keyRegion.get(new ByteArrayWrapper(commandElems.get(3)));
        if (hw1 == null || hw2 == null) {
            command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
            return;
        }

        Double dist = Coder.geoDist(hw1.toString(), hw2.toString());

        if (commandElems.size() == 5) {
            String unit = new String(commandElems.get(4));
            switch (unit) {
                case "km":
                    dist = dist * 0.001;
                    break;
                case "ft":
                    dist = dist * 3.28084;
                    break;
                case "mi":
                    dist = dist * 0.000621371;
                    break;
                default:
                    break;
            }
        }

        DecimalFormat df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);
        command.setResponse(Coder.getBulkStringResponse(context.getByteBufAllocator(), df.format(dist)));
    }
}
