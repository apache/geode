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


import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.redis.internal.ParameterRequirements.RedisParametersMismatchException;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddExecutor extends SortedSetExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    RedisSortedSetCommands redisSortedSetCommands = createRedisSortedSetCommands(context);

    List<ByteArrayWrapper> commandElements = command.getProcessedCommandWrappers();

    List<byte[]> scoresAndMembersToAdd = new ArrayList<>();
    Iterator<ByteArrayWrapper> commandIterator = commandElements.iterator();
    boolean adding = false;
    boolean nxFound = false, xxFound = false, gtFound = false, ltFound = false;
    int count = 0;

    while (commandIterator.hasNext()) {
      ByteArrayWrapper next = commandIterator.next();
      if (count < 2) { // Skip past command, key
        count++;
        System.out.println("****************zaddexec counting:" + count);
        continue;
      } else {
        String subCommandString = next.toString().toLowerCase();
        try {
          Double.valueOf(subCommandString);
          System.out.println("******************zaddexec got a double");
          adding = true;
        } catch (NumberFormatException nfe) {
          switch (subCommandString) {
            case "ch":
              break;
            case "incr":
              break;
            case "nx":
              System.out.println("YEAH WE GOT AN NX!!!!!");
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
        System.out.println("*******************zaddexec adding");
        byte[] score = next.toBytes();
        scoresAndMembersToAdd.add(score);
        if (commandIterator.hasNext()) {
          ByteArrayWrapper member = commandIterator.next();
          scoresAndMembersToAdd.add(member.toBytes());
        } else {
          // TODO: throw exception - should never happen
        }
      }
    }
    long entriesAdded = 0;
    try {
      System.out.println("****************zaddexec calling zadd");
      entriesAdded = redisSortedSetCommands.zadd(command.getKey(), scoresAndMembersToAdd,
          makeOptions(nxFound, xxFound, gtFound, ltFound));
    } catch (Exception e) {
      System.out.println(e.getCause() + " : " + e.getMessage());
    }
    return RedisResponse.integer(entriesAdded);
  }

  private ZSetOptions makeOptions(boolean nxFound, boolean xxFound, boolean gtFound, boolean ltFound) {
    ZSetOptions.Exists existsOption = ZSetOptions.Exists.NONE;
    ZSetOptions.Update updateOption = ZSetOptions.Update.NONE;

    if (nxFound) {
      existsOption = ZSetOptions.Exists.NX;
    }
    if (xxFound) {
      existsOption = ZSetOptions.Exists.XX;
    }
    if (gtFound) {
      updateOption = ZSetOptions.Update.GT;
    }
    if (ltFound) {
      updateOption = ZSetOptions.Update.LT;
    }
    return new ZSetOptions(existsOption, updateOption);
  }
}
