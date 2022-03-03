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
package org.apache.geode.management.internal.cli.shell.jline;



/**
 * Overrides jline.History to add History without newline characters.
 *
 * @since GemFire 7.0
 */
public class ANSIHandler {

  private final boolean isAnsiEnabled;

  public ANSIHandler(boolean isAnsiEnabled) {
    this.isAnsiEnabled = isAnsiEnabled;
  }

  public static ANSIHandler getInstance(boolean isAnsiSupported) {
    return new ANSIHandler(isAnsiSupported);
  }

  public boolean isAnsiEnabled() {
    return isAnsiEnabled;
  }

  public String decorateString(String input, ANSIStyle... styles) {
    String decoratedInput = input;

    if (isAnsiEnabled()) {
      ANSIBuffer ansiBuffer = ANSIBuffer.getANSIBuffer();


      for (ANSIStyle ansiStyle : styles) {
        switch (ansiStyle) {
          case RED:
            ansiBuffer.red(input);
            break;
          case BLUE:
            ansiBuffer.blue(input);
            break;
          case GREEN:
            ansiBuffer.green(input);
            break;
          case BLACK:
            ansiBuffer.black(input);
            break;
          case YELLOW:
            ansiBuffer.yellow(input);
            break;
          case MAGENTA:
            ansiBuffer.magenta(input);
            break;
          case CYAN:
            ansiBuffer.cyan(input);
            break;
          case BOLD:
            ansiBuffer.bold(input);
            break;
          case UNDERSCORE:
            ansiBuffer.underscore(input);
            break;
          case BLINK:
            ansiBuffer.blink(input);
            break;
          case REVERSE:
            ansiBuffer.reverse(input);
            break;
          default:
            break;
        }
      }

      decoratedInput = ansiBuffer.toString();
    }

    return decoratedInput;
  }

  public enum ANSIStyle {
    RED, BLUE, GREEN, BLACK, YELLOW, MAGENTA, CYAN, BOLD, UNDERSCORE, BLINK, REVERSE
  }
}
