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
package com.gemstone.gemfire.management.internal.cli.util;

public class OptionJFormatter {

  private static final String J_OPTION = "--J=";
  private static final char QUOTE = '\"';
  private static final char SPACE = ' ';

  private boolean quotesOpened;
  private int previousSpace;
  private String command;
  private StringBuilder formatted;

  public String formatCommand(String command){
    if (!containsJopt(command)) {
      return command;
    }
    this.command = command;
    this.formatted = new StringBuilder();
    quotesOpened = false;

    int nextJ = this.command.indexOf(J_OPTION);

    while (nextJ > -1) {
      String stringBeforeJ = this.command.substring(0, nextJ+4);
      if (quotesOpened && stringBeforeJ.contains("--")){
        previousSpace = stringBeforeJ.indexOf("--") - 1;
        while (stringBeforeJ.charAt(previousSpace) == SPACE){
          previousSpace--;
        }
        stringBeforeJ = stringBeforeJ.substring(0,previousSpace + 1) + QUOTE + stringBeforeJ.substring(previousSpace + 1);
        quotesOpened = false;
      }

      this.command = this.command.substring(nextJ+4);

      this.formatted.append(stringBeforeJ);
      if (!this.command.startsWith(""+QUOTE)){
        this.formatted.append(QUOTE);
        quotesOpened = true;
      }
      quotesOpened = true;

      int nextSpace = this.command.indexOf(SPACE);
      String stringAfterJ = null;
      if (nextSpace > -1) {
        stringAfterJ = this.command.substring(0, nextSpace);
        this.command = this.command.substring(nextSpace);
      } else {
        stringAfterJ = this.command.substring(0);
        this.command = "";
      }

      this.formatted.append(stringAfterJ);
      if (stringAfterJ.endsWith("\"")){
        quotesOpened = false;
      }

      nextSpace = this.command.indexOf(SPACE);

      if (nextSpace == -1) {
        if (!stringAfterJ.endsWith("" + QUOTE)) {
          this.formatted.append(QUOTE);
          quotesOpened = false;
        }
      } else if (!this.formatted.toString().endsWith(""+QUOTE)) {
        if(this.command.startsWith(" --")){
          this.formatted.append(QUOTE);
          quotesOpened = false;
        }
      }

      if (!containsJopt(this.command)){
        this.formatted.append(this.command);
      }

      nextJ = this.command.indexOf(J_OPTION);
    }

    return formatted.toString();
  }

  public boolean containsJopt(String cmd){
    if (cmd.contains("--J")){
      return true;
    }
    return false;
  }

}
