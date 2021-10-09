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



/**
 * A class for POSIX glob pattern with brace expansions.
 * This code is derived from native redis utl.c stringmatchlen.
 */
public class GlobPattern {
  private final byte[] pattern;

  public GlobPattern(byte[] patternBytes) {
    pattern = patternBytes;
  }

  public boolean matches(byte[] bytes) {
    return matches(pattern, 0, bytes, 0);
  }

  public static boolean matches(byte[] pattern, byte[] bytes) {
    return matches(pattern, 0, bytes, 0);
  }

  private static boolean matches(byte[] pattern, int patternIdx, byte[] string, int stringIdx) {
    while (patternIdx < pattern.length && stringIdx < string.length) {
      switch (pattern[patternIdx]) {
        case '*': {
          while (patternIdx + 1 < pattern.length && pattern[patternIdx + 1] == '*') {
            patternIdx++;
          }
          if ((pattern.length - patternIdx) == 1) {
            return true; /* match */
          }
          while (stringIdx < string.length) {
            if (matches(pattern, patternIdx + 1, string, stringIdx)) {
              return true; /* match */
            }
            stringIdx++;
          }
          return false; /* no match */
        }
        case '?': {
          stringIdx++;
          break;
        }
        case '[': {
          boolean not = false;
          boolean match = false;

          patternIdx++;
          if (patternIdx < pattern.length) {
            not = pattern[patternIdx] == '^';
            if (not) {
              patternIdx++;
            }
          }
          while (patternIdx < pattern.length && pattern[patternIdx] != ']') {
            if (pattern[patternIdx] == '\\' && (pattern.length - patternIdx) >= 2) {
              patternIdx++;
              if (pattern[patternIdx] == string[stringIdx]) {
                match = true;
              }
            } else if ((pattern.length - patternIdx) >= 3 && pattern[patternIdx + 1] == '-') {
              int start = pattern[patternIdx];
              int end = pattern[patternIdx + 2];
              int c = string[stringIdx];
              if (start > end) {
                int t = start;
                start = end;
                end = t;
              }
              patternIdx += 2;
              if (c >= start && c <= end) {
                match = true;
              }
            } else {
              if (pattern[patternIdx] == string[stringIdx]) {
                match = true;
              }
            }
            patternIdx++;
          }
          if (patternIdx == pattern.length) {
            patternIdx--;
          }
          if (not) {
            match = !match;
          }
          if (!match) {
            return false; /* no match */
          }
          stringIdx++;
          break;
        }
        case '\\': {
          if ((pattern.length - patternIdx) >= 2) {
            patternIdx++;
          }
          /* fall through */
        }
        default:
          if (pattern[patternIdx] != string[stringIdx]) {
            return false; /* no match */
          }
          stringIdx++;
          break;
      }
      patternIdx++;
      if (stringIdx == string.length) {
        while (patternIdx < pattern.length && pattern[patternIdx] == '*') {
          patternIdx++;
        }
        break;
      }
    }
    return patternIdx == pattern.length && stringIdx == string.length;
  }
}
