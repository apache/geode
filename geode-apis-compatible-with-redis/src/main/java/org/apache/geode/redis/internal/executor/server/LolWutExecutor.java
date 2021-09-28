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


import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;

import java.util.List;
import java.util.Random;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class LolWutExecutor extends AbstractExecutor {

  public static final int DEFAULT_WIDTH = 40;
  public static final int DEFAULT_HEIGHT = 10;
  private static int width = DEFAULT_WIDTH;
  private static int height = DEFAULT_HEIGHT;

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {

    long inputWidth = -1;
    long inputHeight = -1;

    List<byte[]> commands = command.getProcessedCommand();
    if (commands.size() > 1) {
      for (int i = 1; i < commands.size(); i++) {
        if (Coder.bytesToString(commands.get(i)).equalsIgnoreCase("version")) {
          i += 1; // skip next arg, we only have one version for now
        } else {
          try {
            if (inputWidth < 0) {
              inputWidth = Coder.bytesToLong(commands.get(i));
            } else if (inputHeight < 0) {
              inputHeight = Coder.bytesToLong(commands.get(i));
            } else {
              break; // all required args filled
            }
          } catch (NumberFormatException ignored) {
            return RedisResponse.error(ERROR_NOT_INTEGER);
          }
        }
      }
    }
    if (inputHeight >= 0) {
      height = (int) inputHeight;
    }
    if (inputWidth >= 0) {
      width = (int) inputWidth;
    }

    return RedisResponse.bulkString(makeArbitraryHeightMaze());
  }

  // Adapted from code here: https://tromp.github.io/maze.html
  public static String makeArbitraryHeightMaze() {
    StringBuilder mazeString = new StringBuilder();
    int[] leftLinks = new int[width];
    int[] rightLinks = new int[width];

    Random rand = new Random();
    leftLinks[0] = 1;

    mazeTopAndEntrance(mazeString, leftLinks, rightLinks);

    mazeRows(mazeString, leftLinks, rightLinks, rand);

    mazeBottomRow(mazeString, leftLinks, rightLinks, rand);

    mazeString.append("\n " + KnownVersion.getCurrentVersion().toString() + "\n");

    return mazeString.toString();
  }

  private static void mazeTopAndEntrance(StringBuilder mazeString, int[] leftLinks,
      int[] rightLinks) {
    int tempIndex;
    for (tempIndex = width; --tempIndex > 0; leftLinks[tempIndex] =
        rightLinks[tempIndex] = tempIndex) {
      mazeString.append("._"); // top walls
    }
    mazeString.append("\n "); // Open wall for entrance at top left
  }

  private static void mazeRows(StringBuilder mazeString,
      int[] leftLinks,
      int[] rightLinks, Random rand) {
    int currentCell;
    int tempIndex;
    String first;
    String second;

    while (--height > 0) {
      for (currentCell = width; --currentCell > 0;) {
        if (currentCell != (tempIndex = leftLinks[currentCell - 1])
            && rand.nextBoolean()) { // connect cell to cell on right?
          rightLinks[tempIndex] = rightLinks[currentCell];
          leftLinks[rightLinks[currentCell]] = tempIndex;
          rightLinks[currentCell] = currentCell - 1;
          leftLinks[currentCell - 1] = currentCell;
          second = "."; // no wall to right
        } else {
          second = "|"; // wall to the right
        }
        if (currentCell != (tempIndex = leftLinks[currentCell])
            && rand.nextBoolean()) { // omit down-connection?
          rightLinks[tempIndex] = rightLinks[currentCell];
          leftLinks[rightLinks[currentCell]] = tempIndex;
          leftLinks[currentCell] = currentCell;
          rightLinks[currentCell] = currentCell;
          first = "_"; // wall downward
        } else {
          first = " "; // no wall downward
        }
        mazeString.append(first);
        mazeString.append(second);
      }
      mazeString.append("\n|");
    }
  }

  private static void mazeBottomRow(StringBuilder mazeString, int[] leftLinks,
      int[] rightLinks, Random rand) {
    int currentCell;
    int tempIndex;

    for (currentCell = width; --currentCell > 0;) {
      if (currentCell != (tempIndex = leftLinks[currentCell - 1])
          && (currentCell == rightLinks[currentCell] || rand.nextBoolean())) {
        leftLinks[rightLinks[tempIndex] = rightLinks[currentCell]] = tempIndex;
        leftLinks[rightLinks[currentCell] = currentCell - 1] = currentCell;
        mazeString.append("_.");
      } else {
        if (currentCell == 1) {
          mazeString.append("_."); // maze exit
        } else {
          mazeString.append("_|"); // regular wall
        }
      }
      tempIndex = leftLinks[currentCell];
      rightLinks[tempIndex] = rightLinks[currentCell];
      leftLinks[rightLinks[currentCell]] = tempIndex;
      leftLinks[currentCell] = currentCell;
      rightLinks[currentCell] = currentCell;
    }
  }
}
