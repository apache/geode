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
package org.apache.geode.management.internal.cli;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 * Semi-complete implementation of {@link CliAroundInterceptor} for convenience for implementors.
 *
 *
 * @since GemFire 7.0
 */
public abstract class AbstractCliAroundInterceptor implements CliAroundInterceptor {
  protected enum Response {
    YES("yes"), NO("no");

    private final String text;

    Response(String string) {
      text = string;
    }

    public String toString() {
      return text;
    }

    public String toUpperPrompt() {
      return text.substring(0, 1).toUpperCase();
    }

    public String toLowerPrompt() {
      return text.substring(0, 1).toLowerCase();
    }

    public static Response fromString(final String text) {
      if (text != null) {
        for (Response response : Response.values()) {
          // If the whole string matches, or the input is only 1 character and it matches the
          // first character of the response text, then they are considered equal.
          if (text.equalsIgnoreCase(response.text)
              || ((text.length() == 1 && text.equalsIgnoreCase(response.text.substring(0, 1))))) {
            return response;
          }
        }
      }
      return null;
    }
  }

  public boolean interactionSupported() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    return gfsh != null && !gfsh.isQuietMode() && !gfsh.isHeadlessMode();
  }

  protected String interact(String message) {
    return Gfsh.getCurrentInstance().interact(message);
  }

  protected Response readYesNo(String message, Response defaultResponse) {
    if (defaultResponse == Response.YES) {
      message += " (" + Response.YES.toUpperPrompt() + "/" + Response.NO.toLowerPrompt() + "): ";
    } else {
      message += " (" + Response.YES.toLowerPrompt() + "/" + Response.NO.toUpperPrompt() + "): ";
    }

    if (!interactionSupported()) {
      return defaultResponse;
    }

    Response response = null;
    do {
      String userInput = interact(message);

      if (StringUtils.isEmpty(userInput)) {
        return defaultResponse;
      }
      response = Response.fromString(userInput);

    } while (response == null);

    return response;
  }
}
