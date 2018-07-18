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
import org.apache.geode.redis.internal.StringWrapper;

public class GeoDistExecutor extends GeoSortedSetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    ByteArrayWrapper key = command.getKey();

    if (commandElems.size() < 4 || commandElems.size() > 5) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.GEODIST));
      return;
    }

    Region<ByteArrayWrapper, StringWrapper> keyRegion = getRegion(context, key);
    StringWrapper hw1 = keyRegion.get(new ByteArrayWrapper(commandElems.get(2)));
    StringWrapper hw2 = keyRegion.get(new ByteArrayWrapper(commandElems.get(3)));
    if (hw1 == null || hw2 == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    Double dist = GeoCoder.geoDist(hw1.toString(), hw2.toString());

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

    command.setResponse(
        Coder.getBulkStringResponse(context.getByteBufAllocator(), Double.toString(dist)));
  }
}
