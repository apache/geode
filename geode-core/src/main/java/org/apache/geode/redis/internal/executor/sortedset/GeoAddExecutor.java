/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
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
