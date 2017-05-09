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

import org.apache.commons.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.springframework.shell.converters.ArrayConverter;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.Parser;
import org.springframework.shell.core.SimpleParser;
import org.springframework.shell.event.ParseResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of the {@link Parser} interface for GemFire SHell (gfsh) requirements.
 *
 * @since GemFire 7.0
 */
public class GfshParser extends SimpleParser {

  public static final String LINE_SEPARATOR = System.getProperty("line.separator");
  public static final String OPTION_VALUE_SPECIFIER = "=";
  public static final String OPTION_SEPARATOR = " ";
  public static final String SHORT_OPTION_SPECIFIER = "-";
  public static final String LONG_OPTION_SPECIFIER = "--";
  public static final String COMMAND_DELIMITER = ";";
  public static final String CONTINUATION_CHARACTER = "\\";

  // Make use of LogWrapper
  private static final LogWrapper logWrapper = LogWrapper.getInstance();
  private static final org.apache.logging.log4j.Logger logger = LogService.getLogger();

  // pattern used to split the user input with whitespaces except those in quotes (single or double)
  private static Pattern PATTERN =
      Pattern.compile("\\s*([^\\s']*)'([^']*)'\\s+|\\s*([^\\s\"]*)\"([^\"]*)\"\\s+|\\S+");
  /**
   * Used for warning messages
   */
  private Logger consoleLogger;

  private CommandManager commandManager = null;

  public GfshParser() {
    this(null);
  }

  public GfshParser(Properties cacheProperties) {
    if (CliUtil.isGfshVM()) {
      consoleLogger = Logger.getLogger(this.getClass().getCanonicalName());
    } else {
      consoleLogger = logWrapper.getLogger();
    }

    this.commandManager = new CommandManager(cacheProperties);

    for (CommandMarker command : commandManager.getCommandMarkers()) {
      add(command);
    }

    List<Converter<?>> converters = commandManager.getConverters();
    for (Converter<?> converter : converters) {
      if (converter.getClass().isAssignableFrom(ArrayConverter.class)) {
        ArrayConverter arrayConverter = (ArrayConverter) converter;
        arrayConverter.setConverters(new HashSet<>(converters));
      }
      add(converter);
    }
  }

  public CommandManager getCommandManager() {
    return commandManager;
  }

  static String convertToSimpleParserInput(String userInput) {
    List<String> inputTokens = splitUserInput(userInput);
    return getSimpleParserInputFromTokens(inputTokens);
  }

  static List<String> splitUserInput(String userInput) {
    // make sure the userInput ends with a white space, because our regex expects the the quotes
    // ends with at least one white space. We will trim the results after we found it.
    userInput = userInput + " ";
    // first split with whitespaces except in quotes
    List<String> splitWithWhiteSpaces = new ArrayList<>();
    Matcher m = PATTERN.matcher(userInput);
    while (m.find()) {
      splitWithWhiteSpaces.add(m.group().trim());
    }

    List<String> furtherSplitWithEquals = new ArrayList<>();
    for (String token : splitWithWhiteSpaces) {
      // if this token has equal sign, split around the first occurrance of it
      int indexOfFirstEqual = token.indexOf('=');
      if (indexOfFirstEqual < 0) {
        furtherSplitWithEquals.add(token);
      } else {
        String left = token.substring(0, indexOfFirstEqual);
        String right = token.substring(indexOfFirstEqual + 1);
        if (left.length() > 0) {
          furtherSplitWithEquals.add(left);
        }
        if (right.length() > 0) {
          furtherSplitWithEquals.add(right);
        }
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
    StringBuffer rawInput = new StringBuffer();
    // firstJIndex must be less than or equal to the length of the inputToken
    for (int i = 0; i <= inputTokens.size(); i++) {
      // stick the --J arguments in the orginal first --J position
      if (i == firstJIndex) {
        rawInput.append("--J ");
        if (jArguments.size() > 0) {
          rawInput.append("\"").append(StringUtils.join(jArguments, ",")).append("\" ");
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
    logger.info("parser user input: <" + userInput + ">");
    String rawInput = convertToSimpleParserInput(userInput);

    logger.info("parser raw input: <" + rawInput + ">");
    // User SimpleParser to parse the input
    ParseResult result = super.parse(rawInput);

    if (result != null) {
      return new GfshParseResult(result.getMethod(), result.getInstance(), result.getArguments(),
          userInput);
    }

    return null;
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
   * @param userInput
   * @param cursor this input is ignored, we always move the cursor to the end of the userInput
   * @param candidates
   * @return the cursor point at which the candidate string will begin, this is important if you
   *         have only one candidate, cause tabbing will use it to complete the string for you.
   */

  @Override
  public int completeAdvanced(String userInput, int cursor, final List<Completion> candidates) {
    // move the cursor to the end of the input
    cursor = userInput.length();
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

    if (potentials.size() > 0) {
      if (lastTokenIsOption) {
        candidateBeginAt = buffer.length() - lastToken.length();
      } else {
        // need to return the index before the "=" sign, since later on we are going to add the
        // "=" sign to the completion candidates
        candidateBeginAt = buffer.length() - lastToken.length() - 1;
      }
    }
    // if the last token is already complete, add either space or " --" and try again
    else {
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
    }

    // manipulate the candidate strings
    if (lastTokenIsOption) {
      // strip off the beginning part of the candidates from the cursor point
      potentials.replaceAll(
          completion -> new Completion(completion.getValue().substring(candidateBeginAt)));
    } else {
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

  // convenience method for testing
  int completeAdvanced(String userInput, final List<Completion> candidates) {
    return completeAdvanced(userInput, userInput.length(), candidates);
  }

  /**
   * test only used to demonstrate what's the super class's completeAdvanced behavior
   *
   */
  int completeSuperAdvanced(String userInput, final List<Completion> candidates) {
    return super.completeAdvanced(userInput, userInput.length(), candidates);
  }

  /**
   * @param buffer use the buffer to find the completion candidates
   *
   *        Note the cursor maynot be the size the buffer
   */
  private List<Completion> getCandidates(String buffer) {
    List<Completion> candidates = new ArrayList<>();
    // always pass the buffer length as the cursor position for simplicity purpose
    super.completeAdvanced(buffer, buffer.length(), candidates);
    // trimming the candidates
    candidates.replaceAll(completion -> new Completion(completion.getValue().trim()));
    return candidates;
  }

  @Override
  public void obtainHelp(String command) {
    if (StringUtils.isBlank(command))
      super.obtainHelp(command);
    else
      consoleLogger.info(commandManager.obtainHelp(command));
  }

  public void obtainHint(String topic) {
    consoleLogger.info(commandManager.obtainHint(topic));
  }
}
