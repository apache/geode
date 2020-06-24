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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.converters.ArrayConverter;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.Parser;
import org.springframework.shell.core.SimpleParser;
import org.springframework.shell.event.ParseResult;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GeodeCommandMarker;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 * Implementation of the {@link Parser} interface for GemFire SHell (gfsh) requirements.
 *
 * @since GemFire 7.0
 */
public class GfshParser extends SimpleParser {

  public static final String LINE_SEPARATOR = System.lineSeparator();
  public static final String OPTION_VALUE_SPECIFIER = "=";
  public static final String OPTION_SEPARATOR = " ";
  public static final String SHORT_OPTION_SPECIFIER = "-";
  public static final String LONG_OPTION_SPECIFIER = "--";
  public static final String COMMAND_DELIMITER = ";";
  public static final String CONTINUATION_CHARACTER = "\\";

  private static final char ASCII_UNIT_SEPARATOR = '\u001F';
  public static final String J_ARGUMENT_DELIMITER = "" + ASCII_UNIT_SEPARATOR;
  public static final String J_OPTION_CONTEXT = "splittingRegex=" + J_ARGUMENT_DELIMITER;

  private final CommandManager commandManager;

  public GfshParser(CommandManager commandManager) {
    this.commandManager = commandManager;

    for (GeodeCommandMarker command : commandManager.getCommandMarkers()) {
      add(command);
    }

    for (Converter<?> converter : commandManager.getConverters()) {
      if (converter.getClass().isAssignableFrom(ArrayConverter.class)) {
        ArrayConverter arrayConverter = (ArrayConverter) converter;
        arrayConverter.setConverters(new HashSet<>(commandManager.getConverters()));
      }
      add(converter);
    }
  }

  static String convertToSimpleParserInput(String userInput) {
    List<String> inputTokens = splitUserInput(userInput);
    return getSimpleParserInputFromTokens(inputTokens);
  }

  /**
   * it's assumed that the quoted string should not have escaped quotes inside it.
   */
  private static List<String> splitWithWhiteSpace(String input) {
    List<String> tokensList = new ArrayList<>();
    StringBuilder token = new StringBuilder();
    char insideQuoteOf = Character.MIN_VALUE;

    for (char c : input.toCharArray()) {
      if (Character.isWhitespace(c)) {
        // if we are in the quotes
        if (insideQuoteOf != Character.MIN_VALUE) {
          token.append(c);
        }
        // if we are not in the quotes, terminate this token and add it to the list
        else {
          if (token.length() > 0) {
            tokensList.add(token.toString());
          }
          token = new StringBuilder();
        }
      }
      // not a white space
      else {
        token.append(c);
        // if encountering a quote
        if (c == '\'' || c == '\"') {
          // if this is the beginning of quote
          if (insideQuoteOf == Character.MIN_VALUE) {
            insideQuoteOf = c;
          }
          // this is the ending of quote
          else if (insideQuoteOf == c) {
            insideQuoteOf = Character.MIN_VALUE;
          }
        }
      }
    }
    if (token.length() > 0) {
      tokensList.add(token.toString());
    }
    return tokensList;
  }

  static List<String> splitUserInput(String userInput) {
    // first split with whitespaces except in quotes
    List<String> splitWithWhiteSpaces = splitWithWhiteSpace(userInput);

    List<String> furtherSplitWithEquals = new ArrayList<>();
    for (String token : splitWithWhiteSpaces) {
      // do not split with "=" if this part starts with quotes or is part of -D
      if (token.startsWith("'") || token.startsWith("\"") || token.startsWith("-D")) {
        furtherSplitWithEquals.add(token);
        continue;
      }
      // if this token has equal sign, split around the first occurrence of it
      int indexOfFirstEqual = token.indexOf('=');
      if (indexOfFirstEqual < 0) {
        furtherSplitWithEquals.add(token);
        continue;
      }
      String left = token.substring(0, indexOfFirstEqual);
      String right = token.substring(indexOfFirstEqual + 1);
      if (left.length() > 0) {
        furtherSplitWithEquals.add(left);
      }
      if (right.length() > 0) {
        furtherSplitWithEquals.add(right);
      }
    }
    return furtherSplitWithEquals;
  }

  static String getSimpleParserInputFromTokens(List<String> tokens) {
    // make a copy of the input since we need to do add/remove
    List<String> inputTokens = new ArrayList<>();

    // get the --J arguments from the list of tokens
    int firstJIndex = -1;
    List<String> jArguments = new ArrayList<>();

    for (int i = 0; i < tokens.size(); i++) {
      String token = tokens.get(i);
      if ("--J".equals(token)) {
        if (firstJIndex < 1) {
          firstJIndex = i;
        }
        i++;

        if (i < tokens.size()) {
          String jArg = tokens.get(i);
          // remove the quotes around each --J arugments
          if (jArg.charAt(0) == '"' || jArg.charAt(0) == '\'') {
            jArg = jArg.substring(1, jArg.length() - 1);
          }
          if (jArg.length() > 0) {
            jArguments.add(jArg);
          }
        }
      } else {
        inputTokens.add(token);
      }
    }

    // concatenate the remaining tokens with space
    StringBuilder rawInput = new StringBuilder();
    // firstJIndex must be less than or equal to the length of the inputToken
    for (int i = 0; i <= inputTokens.size(); i++) {
      // stick the --J arguments in the orginal first --J position
      if (i == firstJIndex) {
        rawInput.append("--J ");
        if (jArguments.size() > 0) {
          // quote the entire J argument with double quotes, and delimited with a special delimiter,
          // and we
          // need to tell the gfsh parser to use this delimiter when splitting the --J argument in
          // each command
          rawInput.append("\"").append(StringUtils.join(jArguments, J_ARGUMENT_DELIMITER))
              .append("\" ");
        }
      }
      // then add the next inputToken
      if (i < inputTokens.size()) {
        rawInput.append(inputTokens.get(i)).append(" ");
      }
    }

    return rawInput.toString().trim();
  }

  @Override
  public GfshParseResult parse(String userInput) {
    String rawInput = convertToSimpleParserInput(userInput);
    // this tells the simpleParser not to interpret backslash as escaping character
    rawInput = rawInput.replace("\\", "\\\\");
    // User SimpleParser to parse the input
    ParseResult result = super.parse(rawInput);

    if (result == null) {
      // do a quick check for required arguments, since SimpleParser unhelpfully suggests everything
      String missingHelp = commandManager.getHelper().getMiniHelp(userInput);
      if (missingHelp != null) {
        System.out.println(missingHelp);
      }

      return null;
    }

    return new GfshParseResult(result.getMethod(), result.getInstance(), result.getArguments(),
        userInput);
  }

  /**
   *
   * The super class's completeAdvanced has the following limitations: 1) for option name
   * completion, you need to end your buffer with --. 2) For command name completion, you need to
   * end your buffer with a space. 3) the above 2 completions, the returned value is always 0, and
   * the completion is the entire command 4) for value completion, you also need to end your buffer
   * with space, the returned value is the length of the original string, and the completion strings
   * are the possible values.
   *
   * With these limitations, we will need to overwrite this command with some customization
   *
   * @param cursor this input is ignored, we always move the cursor to the end of the userInput
   * @return the cursor point at which the candidate string will begin, this is important if you
   *         have only one candidate, cause tabbing will use it to complete the string for you.
   */

  @Override
  public int completeAdvanced(String userInput, int cursor, final List<Completion> candidates) {
    // move the cursor to the end of the input
    List<String> inputTokens = splitUserInput(userInput);

    // check if the input is before any option is specified, e.g. (start, describe)
    boolean inputIsBeforeOption = true;
    for (String token : inputTokens) {
      if (token.startsWith("--")) {
        inputIsBeforeOption = false;
        break;
      }
    }

    // in the case of we are still trying to complete the command name
    if (inputIsBeforeOption) {
      // workaround for SimpleParser bugs with "" option key, and spaces in option values
      int curs =
          completeSpecial(candidates, userInput, inputTokens, CliStrings.HELP, ConverterHint.HELP);
      if (curs > 0) {
        return curs;
      }
      curs =
          completeSpecial(candidates, userInput, inputTokens, CliStrings.HINT, ConverterHint.HINT);
      if (curs > 0) {
        return curs;
      }

      List<Completion> potentials = getCandidates(userInput);
      if (potentials.size() == 1 && potentials.get(0).getValue().equals(userInput)) {
        potentials = getCandidates(userInput.trim() + " ");
      }

      if (potentials.size() > 0) {
        candidates.addAll(potentials);
        return 0;
      }
      // otherwise, falling down to the potentials.size==0 case below
    }

    // now we are either trying to complete the option or a value
    // trying to get candidates using the converted input
    String buffer = getSimpleParserInputFromTokens(inputTokens);
    String lastToken = inputTokens.get(inputTokens.size() - 1);
    boolean lastTokenIsOption = lastToken.startsWith("--");
    // In the original user input, where to begin the candidate string for completion
    int candidateBeginAt;

    // initially assume we are trying to complete the last token
    List<Completion> potentials = getCandidates(buffer);

    // if the last token is already complete (or user deliberately ends with a space denoting the
    // last token is complete, then add either space or " --" and try again
    if (potentials.size() == 0 || userInput.endsWith(" ")) {
      candidateBeginAt = buffer.length();
      // last token is an option
      if (lastTokenIsOption) {
        // add a space to the buffer to get the option value candidates
        potentials = getCandidates(buffer + " ");
        lastTokenIsOption = false;
      }
      // last token is a value, we need to add " --" to it and retry to get the next list of options
      else {
        potentials = getCandidates(buffer + " --");
        lastTokenIsOption = true;
      }
    } else {
      if (lastTokenIsOption) {
        candidateBeginAt = buffer.length() - lastToken.length();
      } else {
        // need to return the index before the "=" sign, since later on we are going to add the
        // "=" sign to the completion candidates
        candidateBeginAt = buffer.length() - lastToken.length() - 1;
      }
    }

    // manipulate the candidate strings
    if (lastTokenIsOption) {
      // strip off the beginning part of the candidates from the cursor point
      potentials.replaceAll(
          completion -> new Completion(completion.getValue().substring(candidateBeginAt)));
    }
    // if the completed values are option, and the userInput doesn't ends with an "=" sign,
    else if (!userInput.endsWith("=")) {
      // these potentials do not have "=" in front of them, manually add them
      potentials.replaceAll(completion -> new Completion("=" + completion.getValue()));
    }

    candidates.addAll(potentials);

    // usually we want to return the cursor at candidateBeginAt, but since we consolidated
    // --J options into one, and added quotes around we need to consider the length difference
    // between userInput and the converted input
    cursor = candidateBeginAt + (userInput.trim().length() - buffer.length());
    return cursor;
  }

  /**
   * gets a specific String converter from the list of registered converters
   */
  private Converter<?> converterFor(String converterHint) {
    for (Converter<?> candidate : getConverters()) {
      if (candidate.supports(String.class, converterHint)) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * uses a specific converter directly, bypassing the need to find it by the command's options
   */
  private int completeSpecial(List<Completion> candidates, String userInput,
      List<String> inputTokens, String cmd,
      String converterHint) {
    if (inputTokens.get(0).equals(cmd)) {
      String prefix = userInput.equals(cmd) ? " " : "";
      String existing = String.join(" ", inputTokens.subList(1, inputTokens.size())).toLowerCase();
      List<Completion> all = new ArrayList<>();
      Converter<?> converter = converterFor(converterHint);
      if (converter != null) {
        converter.getAllPossibleValues(all, null, null, null, null);
        candidates.addAll(all.stream().filter(c -> c.getValue().toLowerCase().startsWith(existing))
            .map(c -> new Completion(prefix + c.getValue()))
            .collect(Collectors.toList()));
        return Math.min(userInput.length(), cmd.length() + 1);
      }
    }
    return 0;
  }

  /**
   * @param buffer use the buffer to find the completion candidates
   *
   *        Note the cursor may not be the size the buffer
   */
  private List<Completion> getCandidates(String buffer) {
    List<Completion> candidates = new ArrayList<>();
    // always pass the buffer length as the cursor position for simplicity purpose
    super.completeAdvanced(buffer, buffer.length(), candidates);
    // trimming the candidates
    candidates.replaceAll(completion -> new Completion(completion.getValue().trim()));
    return candidates;
  }
}
