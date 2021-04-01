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

import static org.apache.commons.lang3.StringUtils.repeat;

class Column {

  private final Align align;
  private final String stringValue;

  Column(Object value, Align align) {
    stringValue = value == null ? "" : String.valueOf(value);
    this.align = align;
  }

  @Override
  public String toString() {
    return "Column{align=" + align + ", stringValue='" + stringValue + "'}";
  }

  int getLength() {
    return stringValue.length();
  }

  String buildColumn(int colWidth, boolean trimIt) {
    // If string value is greater than colWidth
    // This can happen because colSizes are re-computed
    // to fit the screen width
    if (stringValue.length() > colWidth) {
      int endIndex = colWidth - 2;
      if (endIndex < 0) {
        return "";
      }
      return stringValue.substring(0, endIndex) + "..";
    }

    int numSpaces = colWidth - stringValue.length();
    if (trimIt) {
      numSpaces = 0;
    }

    StringBuilder stringBuilder = new StringBuilder();

    switch (align) {
      case LEFT:
        stringBuilder
            .append(stringValue)
            .append(spaces(numSpaces));
        break;
      case RIGHT:
        stringBuilder
            .append(spaces(numSpaces))
            .append(stringValue);
        break;
      case CENTER:
        stringBuilder
            .append(spaces(numSpaces / 2))
            .append(stringValue)
            .append(spaces(numSpaces - numSpaces / 2));
        break;
    }

    return stringBuilder.toString();
  }

  private static String spaces(int count) {
    return repeat(' ', count);
  }
}
