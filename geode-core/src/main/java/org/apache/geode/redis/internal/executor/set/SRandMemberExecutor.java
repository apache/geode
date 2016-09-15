/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.redis.internal.executor.set;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

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
