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

package org.apache.geode.redis.internal.executor;

import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.regex.Pattern;

/**
 * A class for POSIX glob pattern with brace expansions.
 * This class is a factory for Pattern instances using
 * the static factory method {@link #createPattern(byte[])}.
 * To see if a string matches a pattern
 * use {@link #matches(Pattern, CharSequence)}.
 */
public class GlobPattern {
  private static final char BACKSLASH = '\\';

  private GlobPattern() {
    // no instances allowed
  }

  /**
   * Match input against the given glob pattern
   *
   * @param pattern the pattern obtained from {@link #createPattern(byte[])}
   * @param input the chars to match against pattern
   * @return true for successful matches
   */
  public static boolean matches(Pattern pattern, CharSequence input) {
    return pattern.matcher(input).matches();
  }

  /**
   * Match input against the given glob pattern
   *
   * @param pattern the pattern obtained from {@link #createPattern(byte[])}
   * @param inputBytes the bytes to match against pattern
   * @return true for successful matches
   */
  public static boolean matches(Pattern pattern, byte[] inputBytes) {
    return matches(pattern, bytesToString(inputBytes));
  }

  /**
   * Compile the given glob style pattern bytes into
   * an instance of Pattern.
   *
   * @param globBytes the glob pattern as a byte array
   * @return A regex pattern to recognize the given glob pattern.
   */
  public static Pattern createPattern(byte[] globBytes) {
    String glob = bytesToString(globBytes);
    StringBuilder regex = new StringBuilder();
    int setOpen = 0;
    int len = glob.length();

    for (int i = 0; i < len; i++) {
      char c = glob.charAt(i);

      switch (c) {
        case BACKSLASH:
          if (++i < len) {
            regex.append(c).append(glob.charAt(i));
            continue;
          }
          break;
        case '.':
        case '$':
        case '(':
        case ')':
        case '|':
        case '+':
        case '{':
        case '!':
          // escape regex special chars that are not glob special chars
          regex.append(BACKSLASH);
          break;
        case '*':
          regex.append('.');
          break;
        case '?':
          regex.append('.');
          continue;
        case '[':
          if (setOpen > 0) {
            regex.append(BACKSLASH);
          }
          setOpen++;
          break;
        case '^': // ^ inside [...] can be unescaped
          if (setOpen == 0) {
            regex.append(BACKSLASH);
          }
          break;
        case ']':
          setOpen = 0;
          break;
        default:
      }
      regex.append(c);
    }

    if (setOpen > 0) {
      regex.append(']');
    }

    return Pattern.compile(regex.toString(), Pattern.DOTALL | Pattern.MULTILINE);
  }

}
