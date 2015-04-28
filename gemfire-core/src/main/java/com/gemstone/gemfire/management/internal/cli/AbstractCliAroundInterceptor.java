/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import java.io.IOException;

import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 * Semi-complete implementation of {@link CliAroundInterceptor} for convenience for implementors.
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public abstract class AbstractCliAroundInterceptor implements CliAroundInterceptor {
  protected enum Response {
    YES("yes"), NO("no");

    private String text;

    private Response(String string) {
      this.text = string;
    }

    public String toString() {
      return this.text;
    }

    public String toUpperPrompt() {
      return this.text.substring(0,1).toUpperCase();
    }

    public String toLowerPrompt() {
      return this.text.substring(0,1).toLowerCase();
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

  protected String interact(String message) throws IOException {
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
      try {
        String userInput = interact(message);

        if (userInput == null || userInput == "") {
          return defaultResponse;
        }
        response = Response.fromString(userInput);

      } catch (IOException ioex) {
        severe("Could not read user response", ioex);
        // What can you do except try again???
      }

    } while (response == null);

    return response;
  }

  protected static void info(String msg, Throwable th) {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null) {
      gfsh.logInfo(msg, th);
    } else {
      LogWrapper.getInstance().info(msg, th);
    }
  }

  protected static void warning(String msg, Throwable th) {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null) {
      gfsh.logWarning(msg, th);
    } else {
      LogWrapper.getInstance().warning(msg, th);
    }
  }

  protected static void severe(String msg, Throwable th) {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null) {
      gfsh.logSevere(msg, th);
    } else {
      LogWrapper.getInstance().severe(msg, th);
    }
  }
}
