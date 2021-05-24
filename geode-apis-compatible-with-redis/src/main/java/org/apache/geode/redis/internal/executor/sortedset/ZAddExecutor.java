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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddExecutor extends SortedSetExecutor {
  private boolean nxFound, xxFound;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    nxFound = false;
    xxFound = false;
    RedisSortedSetCommands redisSortedSetCommands = context.getRedisSortedSetCommands();
    List<byte[]> commandElements = command.getProcessedCommand();
    Iterator<byte[]> commandIterator = commandElements.iterator();

    SkipCommandAndKey(commandIterator);

    byte[] firstScore = FindZAddOptions(commandIterator);

    List<byte[]> scoresAndMembersToAdd = GetScoresAndMembers(firstScore, commandIterator);

    return RedisResponse
        .integer(redisSortedSetCommands.zadd(command.getKey(), scoresAndMembersToAdd,
            makeOptions(nxFound, xxFound)));
  }

  private void SkipCommandAndKey(Iterator<byte[]> commandIterator) {
    commandIterator.next();
    commandIterator.next();
  }

  private byte[] FindZAddOptions(Iterator<byte[]> commandIterator) {
    boolean scoreFound = false;
    byte[] next = "".getBytes();

    while (commandIterator.hasNext() && !scoreFound) {
      next = commandIterator.next();
      String subCommandString = Coder.bytesToString(next).toLowerCase();
      switch (subCommandString) {
        case "ch":
          break;
        case "incr":
          break;
        case "nx":
          nxFound = true;
          break;
        case "xx":
          xxFound = true;
          break;
        default:
          scoreFound = true;
      }
    }
    return next;
  }

  private List<byte[]> GetScoresAndMembers(byte[] firstScore, Iterator<byte[]> commandIterator) {
    List<byte[]> scoresAndMembersToAdd = new ArrayList<>();
    scoresAndMembersToAdd.add(firstScore);
    while (commandIterator.hasNext()) {
      scoresAndMembersToAdd.add(commandIterator.next());
    }
    return scoresAndMembersToAdd;
  }

  private ZAddOptions makeOptions(boolean nxFound, boolean xxFound) {
    ZAddOptions.Exists existsOption = ZAddOptions.Exists.NONE;

    if (nxFound) {
      existsOption = ZAddOptions.Exists.NX;
    }
    if (xxFound) {
      existsOption = ZAddOptions.Exists.XX;
    }
    return new ZAddOptions(existsOption);
  }
}
