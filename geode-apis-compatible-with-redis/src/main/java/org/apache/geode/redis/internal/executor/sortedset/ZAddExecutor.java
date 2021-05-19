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
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    RedisSortedSetCommands redisSortedSetCommands = createRedisSortedSetCommands(context);

    List<byte[]> commandElements = command.getProcessedCommand();

    List<byte[]> scoresAndMembersToAdd = new ArrayList<>();
    Iterator<byte[]> commandIterator = commandElements.iterator();
    boolean adding = false;
    boolean nxFound = false, xxFound = false, gtFound = false, ltFound = false;
    int count = 0;

    while (commandIterator.hasNext()) {
      byte[] next = commandIterator.next();
      if (count < 2) { // Skip past command, key
        count++;
        continue;
      } else {
        String subCommandString = Coder.bytesToString(next).toLowerCase();
        try {
          Double.valueOf(subCommandString);
          adding = true;
        } catch (NumberFormatException nfe) {
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
            case "gt":
              gtFound = true;
              break;
            case "lt":
              ltFound = true;
              break;
            default:
              // TODO: Should never happen?
          }
        }
      }
      if (adding) {
        byte[] score = next;
        scoresAndMembersToAdd.add(score);
        if (commandIterator.hasNext()) {
          byte[] member = commandIterator.next();
          scoresAndMembersToAdd.add(member);
        } else {
          // TODO: throw exception - should never happen
        }
      }
    }
    long entriesAdded = 0;
    entriesAdded = redisSortedSetCommands.zadd(command.getKey(), scoresAndMembersToAdd,
        makeOptions(nxFound, xxFound, gtFound, ltFound));

    return RedisResponse.integer(entriesAdded);
  }

  private SortedSetOptions makeOptions(boolean nxFound, boolean xxFound, boolean gtFound,
      boolean ltFound) {
    SortedSetOptions.Exists existsOption = SortedSetOptions.Exists.NONE;
    SortedSetOptions.Update updateOption = SortedSetOptions.Update.NONE;

    if (nxFound) {
      existsOption = SortedSetOptions.Exists.NX;
    }
    if (xxFound) {
      existsOption = SortedSetOptions.Exists.XX;
    }
    if (gtFound) {
      updateOption = SortedSetOptions.Update.GT;
    }
    if (ltFound) {
      updateOption = SortedSetOptions.Update.LT;
    }
    return new SortedSetOptions(existsOption, updateOption);
  }
}
