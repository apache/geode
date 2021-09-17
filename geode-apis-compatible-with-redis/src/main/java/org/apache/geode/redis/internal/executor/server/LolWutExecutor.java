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
 *
 */
package org.apache.geode.redis.internal.executor.server;


import java.util.List;
import java.util.Random;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class LolWutExecutor extends AbstractExecutor {
  @Override
  public RedisResponse executeCommand(Command command,
                                      ExecutionHandlerContext context) {
    StringBuilder mazeString = new StringBuilder();
    final int width = 40;
    int height = 10;
    List<byte[]> commands = command.getProcessedCommand();
    if (commands.size() > 1) {
      // get and validate height argument
    }

    int[] left = new int[width];
    left[0] = 1;
    int[] right = new int[width];

    // Top of maze
    for (int i = width; --i > 0; left[i] = right[i] = i) {
      mazeString.append("._");
    }

    // Entrance
    mazeString.append("\n ");

    // Do a row of the maze
    int temp;
    Random rand = new Random();
    while (--height > 0) {
      String first;
      String second;
      for (int i = width; --i > 0; /* */) {
        if ((i != (temp = left[i - 1])) && rand.nextBoolean()) {
          // connect to cell to right
          right[temp] = right[i];
          left[right[i]] = temp;
          right[i] = i - 1;
          left[i - 1] = i;
          second = ".";
        } else {
          // block cell to right
          second = "|";
        }
        if ((i != (temp = left[i])) && rand.nextBoolean()) {
          // block off cell below
          right[temp] = right[i];
          left[right[i]] = temp;
          left[i] = i;
          right[i] = i;
          first = "_";
        } else {
          // leave open lower wall
          first = " ";
        }
        mazeString.append(first);
        mazeString.append(second);
      }
      mazeString.append("\n|");
    }

    // Handle last row
    for (int i = width; --i > 0; ) {
      if ((i != (temp = left[i - 1])) && (i == right[i] || rand.nextBoolean())) {
        left[right[temp]] = right[i] = temp;
        left[(right[i] = i - 1)] = i;
        mazeString.append("_.");
      } else {
        mazeString.append('_');
        if (i==1) {
          mazeString.append(" "); // Exit
        } else {
          mazeString.append('|');
        }
      }

      temp = left[i];
      right[temp] = right[i];
      left[right[i]] = temp;
      left[i] = i;
      right[i] = i;
    }
    mazeString.append("\n " + KnownVersion.getCurrentVersion().toString() + "\n");

    return RedisResponse.bulkString(mazeString.toString());
  }
}
