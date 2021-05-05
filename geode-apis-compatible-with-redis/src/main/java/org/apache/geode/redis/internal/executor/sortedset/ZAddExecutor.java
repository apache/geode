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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddExecutor extends SortedSetExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    RedisSortedSetCommands redisSortedSetCommands = createRedisSortedSetCommands(context);

    List<ByteArrayWrapper> commandElements = command.getProcessedCommandWrappers();

    // zadd key score1 elem1 score2 elem2 ....
    // loop through elements, starting at 2 (or whatever)
    // confirm scores are numbers
    // add pairs to scoresAndMembersToAdd (turn into byte[])
    List<byte[]> scoresAndMembersToAdd = new ArrayList<>();
    Iterator<ByteArrayWrapper> commandIterator = commandElements.iterator();
    boolean adding = false;
    int count = 0;
    while (commandIterator.hasNext()) {
      ByteArrayWrapper next = commandIterator.next();
      if (count < 2) {
        count++;
        continue;
      } else {
        adding = true;
      }
      if (adding) {
        byte[] score = next.toBytes();
        scoresAndMembersToAdd.add(score);
//        if (StringUtils.isNumeric(score.toString())) {
//          scoresAndMembersToAdd.add(score);
//        } else {
//          return RedisResponse.error(ERROR_NOT_A_VALID_FLOAT);
//        }
        // member can be any old thing
        if (commandIterator.hasNext()) {
          ByteArrayWrapper member = commandIterator.next();
          scoresAndMembersToAdd.add(member.toBytes());
        } else {
          // TODO: throw exception
        }
      }
    }
    System.out.println("Executor about to call rssc.zadd...");
    long entriesAdded = 0;
    try {
      entriesAdded = redisSortedSetCommands.zadd(command.getKey(), scoresAndMembersToAdd);
    } catch (Exception e) {
      System.out.println(e.getCause() + " : " + e.getMessage());
    }
    return RedisResponse.integer(entriesAdded);
  }
}
