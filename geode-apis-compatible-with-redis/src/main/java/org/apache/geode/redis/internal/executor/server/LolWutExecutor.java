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
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bVERSION;

import java.util.List;
import java.util.Random;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class LolWutExecutor extends AbstractExecutor {

  public static final int MAX_MAZE_WIDTH = 1024; // limit width to keep memory usage low
  public static final int MAX_MAZE_HEIGHT = 1024 * 1024; // if user wants a bigger maze they can
                                                         // recompile
  public static final int DEFAULT_WIDTH = 40;
  public static final int DEFAULT_HEIGHT = 10;
  private static int width;
  private static int height;

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {

    width = DEFAULT_WIDTH;
    height = DEFAULT_HEIGHT;
    int inputWidth = -1;
    int inputHeight = -1;

    List<byte[]> commands = command.getProcessedCommand();
    if (commands.size() > 1) {
      for (int i = 1; i < commands.size(); i++) {
        if (equalsIgnoreCaseBytes(commands.get(i), bVERSION)) {
          i += 1; // skip next arg, we only have one version for now
        } else {
          try {
            if (inputWidth < 0) {
              inputWidth = Coder.narrowLongToInt(Coder.bytesToLong(commands.get(i)));
              if (inputWidth > MAX_MAZE_WIDTH) {
                inputWidth = MAX_MAZE_WIDTH;
              }
            } else if (inputHeight < 0) {
              inputHeight = Coder.narrowLongToInt(Coder.bytesToLong(commands.get(i)));
              if (inputHeight > MAX_MAZE_HEIGHT) {
                inputHeight = MAX_MAZE_HEIGHT;
              }
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
      height = inputHeight;
    }
    if (inputWidth >= 0) {
      width = inputWidth;
    }

    return RedisResponse.bulkString(makeArbitrarySizeMaze());
  }

  // Adapted from code here: https://tromp.github.io/maze.html
  private static String makeArbitrarySizeMaze() {
    StringBuilder mazeString = new StringBuilder();
    int[] leftLinks = new int[width];
    int[] rightLinks = new int[width];

    Random rand = new Random();
    leftLinks[0] = 1;

    mazeTopAndEntrance(mazeString, leftLinks, rightLinks);

    mazeRows(mazeString, leftLinks, rightLinks, rand);

    mazeBottomRowAndExit(mazeString, leftLinks, rightLinks, rand);

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

  private static void mazeBottomRowAndExit(StringBuilder mazeString, int[] leftLinks,
      int[] rightLinks, Random rand) {
    int currentCell;
    int tempIndex;

    for (currentCell = width; --currentCell > 0;) {
      if (currentCell != (tempIndex = leftLinks[currentCell - 1])
          && (currentCell == rightLinks[currentCell] || rand.nextBoolean())) {
        leftLinks[rightLinks[tempIndex] = rightLinks[currentCell]] = tempIndex;
        leftLinks[rightLinks[currentCell] = currentCell - 1] = currentCell;
        mazeString.append("_."); // no wall to right
      } else {
        if (currentCell == 1) {
          mazeString.append("_."); // maze exit
        } else {
          mazeString.append("_|"); // regular wall to right
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
