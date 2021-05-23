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

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A class for POSIX glob pattern with brace expansions.
 */
public class GlobPattern {
  private static final char BACKSLASH = '\\';
  private final Pattern compiled;
  private final String globPattern;
  private boolean hasWildcard = false;

  /**
   * Construct the glob pattern object with a glob pattern string
   *
   * @param globPattern the glob pattern string
   */
  public GlobPattern(String globPattern) {
    this.compiled = createPattern(globPattern);
    this.globPattern = globPattern;
  }

  /**
   * @return the compiled pattern
   */
  public Pattern compiled() {
    return compiled;
  }

  /**
   * @return the original glob pattern
   */
  public String globPattern() {
    return globPattern;
  }

  /**
   * Compile glob pattern string
   *
   * @param globPattern the glob pattern
   * @return the pattern object
   */
  public static Pattern compile(String globPattern) {
    return new GlobPattern(globPattern).compiled();
  }

  /**
   * Match input against the compiled glob pattern
   *
   * @param s input chars
   * @return true for successful matches
   */
  public boolean matches(CharSequence s) {
    return compiled.matcher(s).matches();
  }

  /**
   * Set and compile a glob pattern
   *
   * @param glob the glob pattern string
   */
  private Pattern createPattern(String glob) {
    StringBuilder regex = new StringBuilder();
    int setOpen = 0;
    int len = glob.length();
    hasWildcard = false;

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
          hasWildcard = true;
          break;
        case '?':
          regex.append('.');
          hasWildcard = true;
          continue;
        case '[':
          if (setOpen > 0) {
            regex.append(BACKSLASH);
          }
          setOpen++;
          hasWildcard = true;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GlobPattern)) {
      return false;
    }
    GlobPattern that = (GlobPattern) o;
    return this.compiled.pattern().equals(that.compiled.pattern());
  }

  @Override
  public int hashCode() {
    return Objects.hash(compiled, hasWildcard);
  }
}
