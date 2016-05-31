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
package com.gemstone.gemfire.management.internal.cli.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;

/**
 * 
 * Utility class for parsing and pre-processing
 * 
 * The methods herein always ensure that the syntax is proper before performing
 * the desired operation
 * 
 * @since GemFire 7.0
 */
public class ParserUtils {
  public static String[] split(String input, String splitAround) {
    if (input != null && splitAround != null) {
      List<String> parts = new ArrayList<String>();
      StringBuffer part = new StringBuffer();
      outer: for (int i = 0; i < input.length(); i++) {
        char ch = input.charAt(i);
        if (splitAround.startsWith("" + ch)) {
          // First check whether syntax is valid
          if (PreprocessorUtils.isSyntaxValid(part.toString())) {
            // This means that we need to check further whether
            // the splitAround is present
            StringBuffer temp = new StringBuffer("");
            for (int j = 0; j < splitAround.length()
                && (i + j) < input.length(); j++) {
              temp.append(input.charAt((i + j)));
              if (temp.toString().equals(splitAround)) {
                parts.add(part.toString().trim());
                part.delete(0, part.length());
                i = i + j + 1;
                if (i < input.length()) {
                  ch = input.charAt(i);
                } else {
                  break outer;
                }
                break;
              }
            }
          }
        }
        part.append(ch);
      }
      // Need to copy the last part in the parts list
      if (part.length() > 0) {
        if (!PreprocessorUtils.containsOnlyWhiteSpaces(part.toString())) {
          if (!part.toString().equals(splitAround))
            parts.add(part.toString().trim());
        }
      }
      // Convert the list into an array
      String[] split = new String[parts.size()];
      for (int i = 0; i < split.length; i++) {
        split[i] = parts.get(i);
      }
      return split;
    } else {
      return null;
    }
  }

  public static String[] splitValues(String value, String valueSeparator) {
    if (value != null && valueSeparator != null) {
      String[] split = split(value, valueSeparator);
      if (value.endsWith(valueSeparator)
          && PreprocessorUtils.isSyntaxValid(split[split.length - 1])) {
        String[] extendedSplit = new String[split.length + 1];
        for (int i = 0; i < split.length; i++) {
          extendedSplit[i] = split[i];
        }
        extendedSplit[split.length] = "";
        return extendedSplit;
      }

      // Remove quotes from the beginning and end of split strings
      for (int i = 0; i < split.length; i++) {
        if ((split[i].endsWith("\"") && split[i].endsWith("\""))
            || (split[i].startsWith("\'") && split[i].endsWith("\'"))) {
          split[i] = split[i].substring(1, split[i].length() - 1);
        }
      }
      
      return split;
    } else {
      return null;
    }
  }

  public static boolean contains(String value, String subString) {
    if (value != null && subString != null) {
      // Here we need to keep in mind that once we get the substring, we
      // should check whether the syntax remains valid
      StringBuffer part = new StringBuffer();
      for (int i = 0; i < value.length(); i++) {
        char ch = value.charAt(i);
        if (subString.startsWith("" + ch)) {
          StringBuffer subPart = new StringBuffer(ch);
          if (PreprocessorUtils.isSyntaxValid(part.toString())) {
            for (int j = 0; j < subString.length() && (i + j) < value.length(); j++) {
              subPart.append("" + value.charAt(i + j));
              if (subPart.toString().equals(subString)) {
                // The subString is present
                // We can return from here
                return true;
              }
            }
          }
        }
        part.append(ch);
      }
    }
    return false;
  }

  public static int lastIndexOf(String value, String subString) {
    int index = -1;
    if (value != null && subString != null) {
      StringBuffer part = new StringBuffer();
      outer: for (int i = 0; i < value.length(); i++) {
        char ch = value.charAt(i);
        if (subString.startsWith("" + ch)) {
          StringBuffer subPart = new StringBuffer(ch);
          if (PreprocessorUtils.isSyntaxValid(part.toString())) {
            for (int j = 0; j < subString.length() && (i + j) < value.length(); j++) {
              subPart.append(value.charAt(i + j));
              if (subPart.toString().equals(subString)) {
                // The subString is present
                // We can return from here
                index = i;
                part.delete(0, part.length());
                i += j + 1;
                if (i < value.length()) {
                  ch = value.charAt(i);
                } else {
                  break outer;
                }
              }
            }
          }
        }
        part.append(ch);
      }
    }
    return index;
  }

  public static String getPadding(int numOfSpaces) {
    char[] arr = new char[numOfSpaces];
    Arrays.fill(arr, ' ');
    return new String(arr);
  }

  public static String trimBeginning(String stringToTrim) {
    if (stringToTrim.startsWith(" ")) {
      int i = 0;
      for (; i < stringToTrim.length(); i++) {
        if (stringToTrim.charAt(i) != ' ') {
          break;
        }
      }
      stringToTrim = stringToTrim.substring(i);
    }

    return stringToTrim;
  }

}
