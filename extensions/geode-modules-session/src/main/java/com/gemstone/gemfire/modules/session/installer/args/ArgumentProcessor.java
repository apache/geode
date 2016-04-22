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

package com.gemstone.gemfire.modules.session.installer.args;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * This class is used to process command line arguments for Java programs in a
 * flexible and powerful manner.
 */
public class ArgumentProcessor {
  /**
   * Logger.
   */
  private static final Logger LOG =
      Logger.getLogger(ArgumentProcessor.class.getName());

  /**
   * Description line length.
   */
  private static final int LINE_LENGTH = 60;

  /**
   * Map containing all arguments defined, indexed by their unique IDs.
   */
  private final List<Argument> args = new ArrayList<Argument>();

  /**
   * Unknown argument handler.
   */
  private UnknownArgumentHandler handler;

  /**
   * Program name to display in usage.
   */
  private String programName;

  ///////////////////////////////////////////////////////////////////////////
  // Classes:

  /**
   * Structure used to represent an argument match.
   */
  private static class Match {
    /**
     * The argument which matched.
     */
    private final Argument arg;

    /**
     * The specific form which matched.
     */
    private final String form;

    /**
     * The parameters to the argument form.
     */
    private final String[] params;

    /**
     * Constructor.
     *
     * @param theArgument the argument which matched
     * @param theForm     the form used
     * @param theParams   the parameters supplied
     */
    public Match(
        final Argument theArgument,
        final String theForm, final String[] theParams) {
      arg = theArgument;
      form = theForm;
      params = theParams;
    }

    /**
     * Accessor.
     *
     * @return argument which matched
     */
    public Argument getArgument() {
      return arg;
    }

    /**
     * Accessor.
     *
     * @return form which was used
     */
    public String getForm() {
      return form;
    }

    /**
     * Accessor.
     *
     * @return parameters supplied
     */
    public String[] getParams() {
      return params;
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Constructors:

  /**
   * Creates a new Argument processor instance for te program name given.
   *
   * @param progName program name used in usage
   */
  public ArgumentProcessor(final String progName) {
    programName = progName;
  }


  ///////////////////////////////////////////////////////////////////////////
  // Public methods:

  /**
   * Adds a new argument.
   *
   * @param arg argument to add
   */
  public void addArgument(final Argument arg) {
    args.add(arg);
  }

  /**
   * Sets the handler to call when an unknown argument is encountered.
   *
   * @param aHandler unknown arg handler, or null to unset
   */
  public void setUnknownArgumentHandler(
      final UnknownArgumentHandler aHandler) {
    handler = aHandler;
  }

  /**
   * Process the command line arguments provided.
   *
   * @param programArgs command line arguments supplied to program
   * @return argument values parsed out of command line
   * @throws UsageException when usge sucked
   */
  public ArgumentValues process(final String[] programArgs)
      throws UsageException {
    ArgumentHandler argHandler;
    final ArgumentValues result = new ArgumentValues();
    List<Argument> unmatched;
    List<Match> matches;

    // Find all argument matches and set postArgs
    matches = checkMatches(programArgs, result);

    // Find arguments which didnt match
    unmatched = new ArrayList<Argument>();
    unmatched.addAll(args);
    for (Match match : matches) {
      unmatched.remove(match.getArgument());
    }

    // Error on unmatched yet required args
    for (Argument arg : unmatched) {
      if (arg.isRequired() && !arg.isDefinedInEnv()) {
        final UsageException usageException = new UsageException(
            "Required argument not provided: " + arg);
        usageException.setUsage(getUsage());
        throw usageException;
      }
    }

    // Handle the arguments
    for (Match match : matches) {
      final Argument arg = match.getArgument();
      argHandler = arg.getArgumentHandler();
      if (argHandler != null) {
        argHandler.handleArgument(
            arg, match.getForm(), match.getParams());
      }
      result.addResult(arg, match.getParams());
    }

    return result;
  }


  /**
   * Generates command line usage text for display to user.
   *
   * @return usage to dusplay to user
   */
  public String getUsage() {
    final StringBuilder builder = new StringBuilder();
    List<String> descriptionLines;
    final String blank20 = "                    ";

    builder.append("\nUSAGE: ");
    if (programName == null) {
      builder.append("<program>");
    } else {
      builder.append(programName);
    }
    if (args.isEmpty()) {
      builder.append("\nNo arguments supported.\n");
    } else {
      builder.append(" <args>\nWHERE <args>:\n\n");
      for (Argument arg : args) {
        for (String form : arg.getForms()) {
          builder.append("    ");
          builder.append(form);

          for (int i = 0; i < arg.getParameterCount(); i++) {
            builder.append(" <");
            builder.append(arg.getParameterName(i));
            builder.append(">");
          }
          builder.append("\n");
        }

        descriptionLines =
            breakupString(arg.getDescription(), LINE_LENGTH);
        if (descriptionLines.isEmpty()) {
          builder.append(blank20);
          builder.append("No argument description provided.");
          builder.append("\n\n");
        } else {
          for (String line : descriptionLines) {
            builder.append(blank20);
            builder.append(line.trim());
            builder.append("\n");
          }
          builder.append("\n");
        }
      }
    }
    builder.append("\n");

    return builder.toString();
  }


  ///////////////////////////////////////////////////////////////////////////
  // Private methods:

  /**
   * Builds a listof all argument matches and sets the postArgs array.
   *
   * @param programArgs command line arguments to search through
   * @param values      values object in which to store results
   * @return list of matches
   * @throws UsageException when there is EBKAC
   */
  private List<Match> checkMatches(
      final String[] programArgs, final ArgumentValues values)
      throws UsageException {
    final List<Match> result = new ArrayList<Match>();
    Match match;
    String[] params;
    String[] postArgs;
    int idx = 0;
    int idx2;

    while (idx < programArgs.length) {
      // Check for end-of-parameters arg
      if ("--".equals(programArgs[idx])) {
        if (++idx < programArgs.length) {
          postArgs = new String[programArgs.length - idx];
          System.arraycopy(programArgs, idx,
              postArgs, 0, postArgs.length);
          values.setPostArgs(postArgs);
        }
        // We're done processing args'
        break;
      }

      // Determine parameter count
      idx2 = idx;
      while ((idx2 + 1) < programArgs.length
          && programArgs[idx2 + 1].charAt(0) != '-') {
        idx2++;
      }

      // Generate parameter array
      params = new String[idx2 - idx];
      System.arraycopy(programArgs, idx + 1, params, 0, params.length);

      LOG.fine("Arg: " + programArgs[idx]);
      LOG.fine("Params: " + params.length);

      // Find first argument matches
      match = null;
      for (Argument arg : args) {
        match = checkMatch(programArgs[idx], arg, params);
        if (match != null) {
          result.add(match);
          LOG.fine("Match found: ");
          LOG.fine("     ID: " + arg);
          LOG.fine("   Form: " + match.getForm());
          break;
        }
      }
      if (match == null) {
        if (handler == null) {
          final UsageException usageException = new UsageException(
              "Unknown argument: " + programArgs[idx]
                  + " with " + params.length + " parameters.");
          usageException.setUsage(getUsage());
          throw (usageException);
        } else {
          handler.handleUnknownArgument(programArgs[idx], params);
        }
      }

      idx += params.length + 1;
    }

    return result;
  }

  /**
   * Checks to see if an rgument form matches the suppies parameter list.
   *
   * @param argName argument name
   * @param arg     argument
   * @param params  parameters supplied
   * @return match object on match, null otherwise
   */
  private Match checkMatch(
      final String argName, final Argument arg, final String[] params) {
    // Look for a matching form
    for (String form : arg.getForms()) {
      if (
          form.equals(argName)
              && arg.getParameterCount() == params.length) {
        return new Match(arg, form, params);
      }
    }

    return null;
  }

  /**
   * Breaks up a string into sub-strings, each with a length equal to or less
   * than the max length specified.
   *
   * @param str       string to break up
   * @param maxLength maximum line length to use
   * @return broken up string
   */
  private List<String> breakupString(
      final String str, final int maxLength) {
    final List<String> result = new ArrayList<String>();
    int startIdx = -1;
    int lastIdx;
    int idx;

    if (str == null) {
      return result;
    }

    do {
      idx = startIdx;
      do {
        lastIdx = idx;
        idx = str.indexOf(' ', lastIdx + 1);
        LOG.fine("startIdx=" + startIdx + "  lastIdx=" + lastIdx
            + "  idx=" + idx);
        if (idx < 0) {
          // Canot break line up any further
          result.add(str.substring(startIdx + 1));
          return result;
        }
      } while ((idx - startIdx) <= maxLength);

      result.add(str.substring(startIdx + 1, lastIdx));
      startIdx = lastIdx;
    } while (true);
  }

}
