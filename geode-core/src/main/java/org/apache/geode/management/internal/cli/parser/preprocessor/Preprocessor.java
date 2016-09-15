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
package org.apache.geode.management.internal.cli.parser.preprocessor;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.management.internal.cli.parser.SyntaxConstants;

/**
 * 
 * <code><br></code> This Class will serve the same purpose as pre-processors do during compilation of a program.
 * 
 * It will act as pre-processor for jopt.
 * 
 * It will split the user input into an array of strings as per the specifications of the command for e.g; Different
 * command might require different value separators, different value specifiers and many more customizations
 * 
 * @since GemFire 7.0
 * 
 */
public class Preprocessor {
  private static final String VALUE_SPECIFIER = SyntaxConstants.OPTION_VALUE_SPECIFIER;
  private static final String ARGUMENT_SEPARATOR = SyntaxConstants.ARGUMENT_SEPARATOR;
  private static final String OPTION_SEPARATOR = SyntaxConstants.OPTION_SEPARATOR;
  private static final String LONG_OPTION_SPECIFIER = SyntaxConstants.LONG_OPTION_SPECIFIER;
  private static final String OPTION_DELIMITER = OPTION_SEPARATOR + LONG_OPTION_SPECIFIER;

  public static String[] split(final String input) {
    if (input == null) {
      return null;
    }

    final String trimInput = PreprocessorUtils.trim(input).getString();
    final int length = trimInput.length();
    final List<String> returnStrings = new ArrayList<String>();

    int index = 0; // Current location of the character(s) in the string being examined
    int startOfString = 0; // Starting index of the string we're currently parsing and preparing to save

    // If this first string doesn't start with the long option specifier, then there are arguments.
    // Process the arguments first.
    if (!trimInput.regionMatches(index, LONG_OPTION_SPECIFIER, 0, LONG_OPTION_SPECIFIER.length())) {
      // Until we find the first occurrence of Option Delimiter (" --")
      while (index < length && !trimInput.regionMatches(index, OPTION_DELIMITER, 0, OPTION_DELIMITER.length())) {
        // Anything inside single or double quotes gets ignored
        if (trimInput.charAt(index) == '\'' || trimInput.charAt(index) == '\"') {
          char charToLookFor = trimInput.charAt(index++);
          // Look for the next single or double quote. Those preceded by a '\' character are ignored.
          while (index < length && (trimInput.charAt(index) != charToLookFor || trimInput.charAt(index - 1) == '\\')) {
            index++;
          }
        }

        index++;
        // 1. There are only arguments & we've reached the end OR
        // 2. We are at index where option (" --") has started OR
        // 3. One argument has finished & we are now at the next argument - check for Argument Separator (" ") 
        if (index >= length || trimInput.regionMatches(index, OPTION_DELIMITER, 0, OPTION_DELIMITER.length())
            || trimInput.regionMatches(index, ARGUMENT_SEPARATOR, 0, ARGUMENT_SEPARATOR.length())) {
          String stringToAdd = trimInput.substring(startOfString, (index > length ? length : index)).trim();
          returnStrings.add(stringToAdd);

          if (trimInput.regionMatches(index, ARGUMENT_SEPARATOR, 0, ARGUMENT_SEPARATOR.length())) {
            index += ARGUMENT_SEPARATOR.length();
          }
          startOfString = index;
        }
      }
      index += OPTION_SEPARATOR.length();
    }

    // Process the options
    startOfString = index;
    while (index < length) {
      // Until we find the first occurrence of Option Separator (" ") or Value Specifier ("=")
      while (index < length && !trimInput.regionMatches(index, OPTION_SEPARATOR, 0, OPTION_SEPARATOR.length())
          && !trimInput.regionMatches(index, VALUE_SPECIFIER, 0, VALUE_SPECIFIER.length())) {
        index++;
      }

      if (startOfString != index) {
        returnStrings.add(trimInput.substring(startOfString, index));
        startOfString = index + 1;
      }

      // If value part (starting with "=") has started
      if (trimInput.regionMatches(index++, VALUE_SPECIFIER, 0, VALUE_SPECIFIER.length())) {
        startOfString = index;

        // Keep going over chars until we find the option separator ("--")
        while (index < length && !trimInput.regionMatches(index, OPTION_SEPARATOR, 0, OPTION_SEPARATOR.length())) {
          // Anything inside single or double quotes gets ignored
          if (index < length && (trimInput.charAt(index) == '\'' || trimInput.charAt(index) == '\"')) {
            char charToLookFor = trimInput.charAt(index++);
            // Look for the next single or double quote. Those preceded by a '\' character are ignored.
            while (index < length && (trimInput.charAt(index) != charToLookFor || trimInput.charAt(index - 1) == '\\')) {
              index++;
            }
          }
          index++;
        }

        // 1. We are done & at the end OR
        // 2. There is another word which matches ("--")
        if (index >= length || trimInput.regionMatches(index, OPTION_SEPARATOR, 0, OPTION_SEPARATOR.length())) {
          if (startOfString == index) {
            // This place-holder value indicates to OptionParser that an option
            // was specified without a value.
            returnStrings.add("__NULL__");
          } else {
            String stringToAdd = trimInput.substring(startOfString, (index > length ? length : index));
            returnStrings.add(stringToAdd);
          }
          startOfString = index + 1;
        }
        index++;
      }
    }

    return returnStrings.toArray(new String[0]);
  }
}
