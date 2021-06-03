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
package org.apache.geode.management.internal.cli.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 * Helps table builder for adjusting result width according to screen width
 *
 */

public class TableBuilderHelper {

  private static final int SCREEN_WIDTH_MARGIN_BUFFER = 5;

  public static class Column implements Comparable<Column> {
    int length;
    int originalIndex;
    boolean markForTrim = false;
    int trimmedLength = 0;

    @Override
    public int compareTo(Column o) {
      return length - o.length;
    }

    public String toString() {
      return ("OI:" + originalIndex + "<" + length + ">\n");
    }

  }

  public static class TooManyColumnsException extends RuntimeException {
    public TooManyColumnsException(String str) {
      super(str);
    }
  }

  public static int[] recalculateColSizesForScreen(int screenWidth, int[] colSizes,
      String colSeparators) {

    if (shouldTrimColumns()) {
      int totalLength = 0;
      // change the screen width to account for separator chars
      screenWidth -= (colSizes.length - 1) * colSeparators.length();

      // build sorted list and find total width
      List<Column> stringList = new ArrayList<Column>();
      int index = 0;
      for (int k : colSizes) {
        Column cs = new Column();
        cs.originalIndex = index++;
        cs.length = k;
        stringList.add(cs);
        totalLength += k;
      }

      // No need to reduce the column width return orig array
      if (totalLength <= screenWidth) {
        return colSizes;
      }

      Collections.sort(stringList);

      // find out columns which need trimming
      totalLength = 0;
      int spaceLeft = 0;
      int totalExtra = 0;
      for (Column s : stringList) {
        int newLength = totalLength + s.length;
        // Ensure that the spaceLeft is never < 2 which would prevent displaying a trimmed value
        // even when there is space available on the screen.
        if (newLength + SCREEN_WIDTH_MARGIN_BUFFER > screenWidth) {
          s.markForTrim = true;
          totalExtra += s.length;
          if (spaceLeft == 0) {
            spaceLeft = screenWidth - totalLength;
          }
        }
        totalLength = newLength;
      }

      Collections.sort(stringList, (o1, o2) -> o1.originalIndex - o2.originalIndex);

      // calculate trimmed width for columns marked for
      // distribute the trimming as per percentage
      int finalColSizes[] = new int[colSizes.length];
      int i = 0;
      for (Column s : stringList) {
        if (totalLength > screenWidth) {
          if (s.markForTrim) {
            s.trimmedLength = (int) Math.floor((spaceLeft * ((double) s.length / totalExtra)));
            finalColSizes[i] = s.trimmedLength;
          } else {
            s.trimmedLength = s.length;
            finalColSizes[i] = s.trimmedLength;
          }
        } else {
          s.trimmedLength = s.length;
          finalColSizes[i] = s.trimmedLength;
        }
        i++;
      }

      totalLength = 0;
      index = 0;
      for (int colSize : finalColSizes) {
        if (colSize != colSizes[index] && colSize < 2) {
          throw new TooManyColumnsException("Computed ColSize=" + colSize
              + " Set RESULT_VIEWER to external. This uses the 'less' command (with horizontal scrolling) to see wider results");
        }
        totalLength += colSize;
        index++;
      }

      return finalColSizes;
    } else {
      // Returning original colSizes since reader is set to external
      return colSizes;
    }
  }

  public static int trimWidthForScreen(int maxColLength) {
    if (shouldTrimColumns()) {
      int screenWidth = getScreenWidth();
      if (maxColLength > screenWidth) {
        return screenWidth;
      } else {
        return maxColLength;
      }
    } else {
      return maxColLength;
    }

  }

  public static int getScreenWidth() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh == null) {
      return Gfsh.DEFAULT_WIDTH;
    } else {
      return gfsh.getTerminalWidth();
    }
  }

  public static boolean shouldTrimColumns() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh == null) {
      return Boolean.getBoolean("GFSH.TRIMSCRWIDTH");
    } else {
      return Gfsh.DEFAULT_APP_RESULT_VIEWER.equals(gfsh.getEnvProperty(Gfsh.ENV_APP_RESULT_VIEWER))
          && !Gfsh.isInfoResult();
    }

  }


}
