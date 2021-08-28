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

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A class for POSIX glob pattern with brace expansions.
 */
public class GlobPattern {
  private static final char BACKSLASH = '\\';
  private final Pattern compiled;
  private boolean hasWildcard = false;

  public GlobPattern(byte[] globPattern) {
    this.compiled = createPattern(globPattern);
  }

  /**
   * @return the compiled pattern
   */
  public Pattern compiled() {
    return compiled;
  }

  /**
   * Compile glob pattern string
   *
   * @param globPattern the glob pattern
   * @return the pattern object
   */
  public static Pattern compile(byte[] globPattern) {
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

  public boolean matches(byte[] bytes) {
    return matches(bytesToString(bytes));
  }

  /**
   * Set and compile a glob pattern
   *
   * @param globBytes the glob pattern bytes
   */
  private Pattern createPattern(byte[] globBytes) {
    String glob = bytesToString(globBytes);
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
