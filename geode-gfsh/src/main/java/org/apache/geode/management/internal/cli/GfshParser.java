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

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.management.internal.cli.completion.CompletionProviderRegistry;


/**
 * Implementation of parsing logic for GemFire SHell (gfsh) requirements.
 * Standalone utility class (no longer extends Spring Shell's SimpleParser which was removed in
 * Shell 3.x).
 *
 * Spring Shell 3.x migration: This class has been completely rewritten because:
 * - SimpleParser class was removed from Shell 3.x
 * - Shell 3.x uses a different parsing and command execution model
 * - Manual parsing is now required to bridge gfsh's custom syntax with Shell 3.x expectations
 *
 * @since GemFire 7.0
 */
public class GfshParser {

  public static final String LINE_SEPARATOR = System.lineSeparator();
  public static final String OPTION_VALUE_SPECIFIER = "=";
  public static final String OPTION_SEPARATOR = " ";
  public static final String SHORT_OPTION_SPECIFIER = "-";
  public static final String LONG_OPTION_SPECIFIER = "--";
  public static final String COMMAND_DELIMITER = ";";
  public static final String CONTINUATION_CHARACTER = "\\";

  // Sentinel value to distinguish "--option" (no value) from "--option=''" (empty value)
  private static final String OPTION_NOT_VALUED = "__OPTION_NOT_VALUED__";

  private static final char ASCII_UNIT_SEPARATOR = '\u001F';
  public static final String J_ARGUMENT_DELIMITER = "" + ASCII_UNIT_SEPARATOR;
  public static final String J_OPTION_CONTEXT = "splittingRegex=" + J_ARGUMENT_DELIMITER;

  private final CommandManager commandManager;
  private final CompletionProviderRegistry completionProviderRegistry;

  public GfshParser(CommandManager commandManager) {
    this.commandManager = commandManager;
    this.completionProviderRegistry = new CompletionProviderRegistry();
    // Shell 3.x migration: Command and converter registration now handled by Spring context
    // Shell 1.x required manual registration via add(CommandMarker) and add(Converter)
    // Shell 3.x uses @ShellComponent and @ShellMethod annotations with Spring auto-discovery
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

  /**
   * Parses user input into a GfshParseResult containing the command method, instance, and bound
   * arguments.
   *
   * Spring Shell 3.x migration: Complete reimplementation required because:
   * - Shell 1.x/2.x SimpleParser.parse() no longer exists
   * - Shell 3.x expects direct method invocation rather than ParseResult objects
   * - Custom tokenization needed to handle gfsh-specific syntax (--J options, region paths, etc.)
   * - Manual parameter binding replaces Shell's automatic conversion framework
   */
  public GfshParseResult parse(String userInput) {
    if (userInput == null || userInput.trim().isEmpty()) {
      return null;
    }

    // Preprocess the input to handle multiple --J arguments by joining them with delimiter
    String trimmedInput = convertToSimpleParserInput(userInput).trim();

    // Tokenize the input
    List<String> tokens = tokenize(trimmedInput);

    if (tokens.isEmpty()) {
      return null;
    }

    // Command names can have multiple words (e.g., "list async-event-queues")
    // Try to find the longest matching command name starting from the first tokens
    Method commandMethod = null;
    int commandTokenCount = 0;

    // Find where command name ends (before first option that starts with --)
    int maxCommandTokens = tokens.size();
    for (int i = 0; i < tokens.size(); i++) {
      if (tokens.get(i).startsWith("--")) {
        maxCommandTokens = i;
        break;
      }
    }

    // Try matching progressively longer command names (up to reasonable limit)
    for (int i = Math.min(maxCommandTokens, 5); i >= 1; i--) {
      // Build a potential command name from the first i tokens
      StringBuilder cmdBuilder = new StringBuilder();
      for (int j = 0; j < i; j++) {
        if (j > 0)
          cmdBuilder.append(" ");
        cmdBuilder.append(tokens.get(j));
      }
      String candidateCommand = cmdBuilder.toString();

      // Check if this is a valid command
      Method method = commandManager.getHelper().getCommandMethod(candidateCommand);
      if (method != null) {
        commandMethod = method;
        commandTokenCount = i;
        break; // Found a match
      }
    }

    if (commandMethod == null) {
      return null; // Command not found
    }

    // Find the command instance
    Object commandInstance = findCommandInstance(commandMethod);
    if (commandInstance == null) {
      return null; // Command instance not found
    }

    // Parse arguments (tokens after the command name)
    Object[] arguments;
    try {
      List<String> argTokens = tokens.subList(commandTokenCount, tokens.size());
      arguments = bindArguments(commandMethod, argTokens);

      // Check if validation failed (bindArguments returned null)
      if (arguments == null) {
        return null; // Invalid options - return null to indicate parse failure
      }
    } catch (IllegalArgumentException e) {
      // Parameter conversion/validation errors
      // In Shell 3.x, we need to propagate these errors so they can be displayed to the user
      // rather than just returning null which results in "Command not found"
      // Re-throw the exception so it can be properly handled by the execution layer
      throw e;
    } catch (Exception e) {
      // Other binding errors - return null for backward compatibility
      return null;
    }

    // Create and return GfshParseResult
    return new GfshParseResult(commandMethod, commandInstance, arguments, trimmedInput);
  }

  /**
   * Tokenizes the user input string into a list of tokens (command name and options).
   */
  private List<String> tokenize(String input) {
    return splitUserInput(input);
  }

  /**
   * Finds the command instance that contains the given method.
   */
  private Object findCommandInstance(Method method) {
    // Search through all command markers to find the one containing this method
    for (Object commandMarker : commandManager.getCommandMarkers()) {
      if (commandMarker.getClass().equals(method.getDeclaringClass())) {
        return commandMarker;
      }
    }
    return null;
  }

  /**
   * Binds the token arguments to method parameters.
   * This is a simplified implementation that handles basic option parsing.
   */
  private Object[] bindArguments(Method method, List<String> tokens) throws Exception {
    Parameter[] parameters = method.getParameters();
    Object[] arguments = new Object[parameters.length];

    // Parse tokens into option map (--option=value or --option value)
    Map<String, String> optionValues = parseOptions(tokens);

    // Validate options before binding:
    // 1. Check for unknown options (not in any parameter's aliases)
    // 2. Check for conflicting aliases (multiple aliases of same parameter used together)
    if (!validateOptions(optionValues, parameters)) {
      // Invalid options - return null to indicate parse failure
      return null;
    }

    // Bind to parameters
    for (int i = 0; i < parameters.length; i++) {
      Parameter param = parameters[i];
      ShellOption option = param.getAnnotation(ShellOption.class);

      if (option == null) {
        // Parameter without @ShellOption annotation - set to null
        arguments[i] = null;
        continue;
      }

      // Get the option name - check all aliases
      String value = null;
      for (String alias : option.value()) {
        if (optionValues.containsKey(alias)) {
          value = optionValues.get(alias);
          break; // Found a match
        }
      }
      // Apply region path conversion if this is a region command parameter named "name"
      if (value != null && !value.isEmpty() && isRegionNameParameter(method, option)) {
        value = convertToRegionPath(value);
      }

      // Convert value to parameter type
      // Wrap conversion errors with Spring Shell 1.x compatible error message format
      try {
        // Get the option name (use first alias for error message and special handling)
        String optionName = option.value().length > 0 ? option.value()[0] : "";
        arguments[i] = convertValue(value, param.getType(), option.defaultValue(), optionName);
      } catch (IllegalArgumentException e) {
        // Get the option name (use first alias for error message)
        String optionName = option.value().length > 0 ? option.value()[0] : "unknown";
        // Format: "Failed to convert 'VALUE' to type TYPE for option 'OPTION'"
        String typeName = param.getType().getSimpleName();
        String errorMsg = "Failed to convert '" + value + "' to type " + typeName
            + " for option '" + optionName + "'";
        throw new IllegalArgumentException(errorMsg, e);
      }

    }

    return arguments;
  }

  /**
   * Validates that options are valid and not conflicting.
   * Returns false if:
   * 1. An option is used that doesn't match any parameter's aliases
   * 2. Multiple aliases of the same parameter are used together
   *
   * Shell 3.x migration: Shell 1.x/2.x handled option validation automatically in its parser.
   * With Shell 3.x, we must manually validate to prevent ambiguous or invalid option combinations.
   */
  private boolean validateOptions(Map<String, String> optionValues, Parameter[] parameters) {
    // Build a map of option name -> parameter index for all valid aliases
    Map<String, Integer> optionToParameter = new HashMap<>();

    for (int i = 0; i < parameters.length; i++) {
      Parameter param = parameters[i];
      ShellOption option = param.getAnnotation(ShellOption.class);

      if (option != null) {
        for (String alias : option.value()) {
          optionToParameter.put(alias, i);
        }
      }
    }

    // Check each provided option
    Set<Integer> usedParameters = new HashSet<>();
    for (String optionName : optionValues.keySet()) {
      Integer paramIndex = optionToParameter.get(optionName);

      // Check 1: Unknown option (not in any parameter's aliases)
      if (paramIndex == null) {
        return false;
      }

      // Check 2: Multiple aliases of the same parameter
      if (usedParameters.contains(paramIndex)) {
        // This parameter was already used with a different alias - conflict!
        return false;
      }

      usedParameters.add(paramIndex);
    }

    return true;
  }

  /**
   * Parses command tokens into a map of option name → value pairs.
   *
   * Spring Shell 3.x migration: Shell 1.x/2.x SimpleParser handled option parsing automatically.
   * This custom implementation is needed because:
   * - Shell 3.x removed the SimpleParser class entirely
   * - Gfsh has special syntax requirements (--J for JVM args, negative numbers, etc.)
   * - Must preserve three-way distinction: option absent (null), option present without value
   * (flag), option with empty value ('')
   *
   * This method must distinguish between option flags and option values, which is non-trivial
   * because some valid option values can start with '-' (the option flag prefix). Special cases
   * that require careful handling:
   *
   * - Negative numbers: Values like "-1" or "-0.5" must be recognized as numeric values, not
   * short option flags. Without this, "rebalance --time-out=-1" would fail.
   *
   * - JVM properties: Values like "-Dprop=val" or "-Xmx512m" are values for the --J option,
   * not separate Gfsh options.
   *
   * - Option presence without value: Must distinguish between option not provided (null),
   * option provided as flag (OPTION_NOT_VALUED sentinel), and option with explicit empty
   * value (empty string). This distinction is critical for boolean flags vs string parameters.
   *
   * Handles both compact (--option=value) and space-separated (--option value) formats.
   */
  private Map<String, String> parseOptions(List<String> tokens) {
    Map<String, String> options = new HashMap<>();

    for (int i = 0; i < tokens.size(); i++) {
      String token = tokens.get(i);

      if (token.startsWith(LONG_OPTION_SPECIFIER)) {
        String optionPart = token.substring(LONG_OPTION_SPECIFIER.length());

        if (optionPart.contains(OPTION_VALUE_SPECIFIER)) {
          // Compact format (--option=value) is unambiguous - the '=' clearly separates
          // option name from value. Split with limit=2 to handle values containing '='.
          String[] parts = optionPart.split(OPTION_VALUE_SPECIFIER, 2);
          String value = parts.length > 1 ? parts[1] : "";
          // Users often quote values in shells; strip quotes since we're past shell parsing
          value = stripQuotes(value);
          options.put(parts[0], value);
        } else {
          // Space-separated format (--option value) is ambiguous - need to determine if
          // the next token is a value or another option. This is the complex case.
          String optionName = optionPart;
          String value;

          // Determine if the next token is a value for this option or another option flag.
          // This is complex because some legitimate values can start with '-' or '--',
          // which would normally indicate option flags. We need special handling for:
          //
          // 1. JVM properties (-D, -X) - These are not Gfsh options, they're values for --J
          // 2. --J option values - Can legitimately start with '-' or '--' (e.g., --add-opens)
          // 3. Negative numbers - Must be recognized as values, not short options
          // Without this check, commands like "rebalance --time-out=-1" would fail because
          // "-1" would be interpreted as a short option flag, leaving --time-out without
          // a value and creating a spurious unknown option "1".
          if (i + 1 < tokens.size()) {
            String nextToken = tokens.get(i + 1);
            boolean isJvmProperty = nextToken.startsWith("-D") || nextToken.startsWith("-X");
            boolean isJOptionValue =
                optionName.equals("J") && (nextToken.startsWith("--") || nextToken.startsWith("-"));
            // Regex matches negative integers (-1, -123) and decimals (-1.5, -0.001)
            // but not option flags (-x, --option) or JVM properties (-Dprop=val)
            boolean isNegativeNumber = nextToken.matches("^-\\d+(\\.\\d+)?$");
            boolean isShortOption = nextToken.startsWith(SHORT_OPTION_SPECIFIER)
                && !isJvmProperty && !isJOptionValue && !isNegativeNumber;
            boolean isLongOption = nextToken.startsWith(LONG_OPTION_SPECIFIER) && !isJOptionValue;

            if (!isShortOption && !isLongOption) {
              value = stripQuotes(tokens.get(++i));
            } else {
              // Use sentinel to indicate the option was present but had no value.
              // This distinguishes between three cases that command methods need to differentiate:
              // 1. Option not provided at all (null in map, uses method's default value)
              // 2. Option provided without value: --flag (OPTION_NOT_VALUED, boolean flag)
              // 3. Option provided with explicit empty value: --option='' (empty string)
              value = OPTION_NOT_VALUED;
            }
          } else {
            // Option is at the end of the command with no following tokens.
            // Same sentinel logic applies - the option was explicitly provided but has no value.
            value = OPTION_NOT_VALUED;
          }

          options.put(optionName, value);
        }
      } else if (token.startsWith(SHORT_OPTION_SPECIFIER)) {
        // Short options (-o) have simpler parsing than long options because:
        // 1. They don't support compact format (-o=value is not standard)
        // 2. Single-character names reduce ambiguity with values
        // 3. Negative numbers were already handled in long option logic above
        String optionName = token.substring(SHORT_OPTION_SPECIFIER.length());
        String value;

        // Simple check: if next token exists and doesn't start with '-' or '--',
        // it's the value. Otherwise, this is a flag without a value.
        if (i + 1 < tokens.size()
            && !tokens.get(i + 1).startsWith(SHORT_OPTION_SPECIFIER)
            && !tokens.get(i + 1).startsWith(LONG_OPTION_SPECIFIER)) {
          value = stripQuotes(tokens.get(++i));
        } else {
          // Same sentinel pattern as long options - preserves three-way distinction
          value = OPTION_NOT_VALUED;
        }

        options.put(optionName, value);
      }
    }

    return options;
  }

  /**
   * Strips surrounding quotes (single or double) from a string value.
   *
   * Users often quote option values in their shell to handle spaces or special characters.
   * By the time we receive these tokens, shell parsing is complete, so we remove the quotes
   * to get the actual intended value. For example, --name="my server" should yield the value
   * "my server" not "\"my server\"".
   */
  private String stripQuotes(String value) {
    if (value == null) {
      return null;
    }
    if ((value.startsWith("'") && value.endsWith("'")) ||
        (value.startsWith("\"") && value.endsWith("\""))) {
      if (value.length() >= 2) {
        return value.substring(1, value.length() - 1);
      }
    }
    return value;
  }

  /**
   * Converts a string value to the target type.
   *
   * Spring Shell 3.x migration: Replaces Shell 1.x/2.x Converter framework.
   * Shell 1.x used pluggable Converter instances registered with the parser.
   * Shell 3.x expects direct value conversion, so this method handles all type conversions inline.
   * Supports: primitives, enums, arrays, File, ConnectionEndpoint, ClassName, ExpirationAction.
   */
  /**
   * Converts a string value to the target type.
   *
   * @param value the string value to convert (may be null)
   * @param targetType the target type to convert to
   * @param defaultValue the default value to use if value is null or empty
   * @param optionName the name of the option being converted (used for special handling)
   * @return the converted value
   */
  private Object convertValue(String value, Class<?> targetType, String defaultValue,
      String optionName) {
    // Special handling for option-present-but-no-value: treat as null for most types
    // This distinguishes "--option" (OPTION_NOT_VALUED → null) from "--option=''" (empty string)
    if (OPTION_NOT_VALUED.equals(value)) {
      // For boolean flags, option-present-but-no-value means true
      if (targetType == boolean.class || targetType == Boolean.class) {
        return true;
      }
      // For String types, convert to empty string so validation can detect it
      if (targetType == String.class) {
        value = "";
        // Continue processing as empty string
      } else {
        // For non-String, non-boolean types (including ClassName), return null
        return null;
      }
    }

    // Special handling for boolean flags:
    // If the option is present without a value (empty string), treat it as true
    // This must be checked BEFORE applying default values
    if ((targetType == boolean.class || targetType == Boolean.class) && "".equals(value)) {
      return true;
    }

    // Check if there's a meaningful default value (not null, not empty, not a NULL marker)
    boolean hasDefaultValue = defaultValue != null
        && !defaultValue.isEmpty()
        && !"__NONE__".equals(defaultValue)
        && !ShellOption.NULL.equals(defaultValue);

    // Special handling for String parameters with empty values:
    // If the parameter has a meaningful default value, apply it when empty string is passed.
    // This preserves Spring Shell 2.x behavior where --param='' used the default value.
    // If no default value exists, preserve the empty string (it's meaningful in some contexts).
    if (targetType == String.class && "".equals(value)) {
      if (hasDefaultValue) {
        // Apply default value when empty string is passed
        value = defaultValue;
      } else {
        // No default value - preserve empty string as-is
        return "";
      }
    }

    // Use default value if no value provided (null or empty for non-String, non-boolean, non-array
    // types)
    if (value == null || value.isEmpty()) {
      if (hasDefaultValue) {
        value = defaultValue;
      } else if (!targetType.isArray()
          && targetType != org.apache.geode.management.configuration.ClassName.class) {
        // For non-array, non-ClassName types, return null if no value and no default
        // ClassName needs to handle empty strings specially (returns ClassName.EMPTY)
        // Arrays can handle empty/null values by creating empty arrays
        return null;
      }
      // For ClassName and arrays with null/empty value and no default, continue to type-specific
      // handling
    }

    // For String types, preserve empty strings (they're different from null)
    // Other types (except arrays and ClassName) treat empty string as null
    if (targetType != String.class
        && !targetType.isArray()
        && targetType != org.apache.geode.management.configuration.ClassName.class
        && (value == null || value.isEmpty())) {
      return null;
    }

    // Handle common types
    if (targetType == String.class) {
      // For strings, empty string is valid, but NULL marker constants should return null
      if (value == null || "__NONE__".equals(value) || ShellOption.NULL.equals(value)) {
        return null;
      }
      return value; // Return as-is, including empty strings
    } else if (targetType == int.class || targetType == Integer.class) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        // Invalid number format - throw to cause parse failure
        throw new IllegalArgumentException("Invalid integer value: " + value, e);
      }
    } else if (targetType == long.class || targetType == Long.class) {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid long value: " + value, e);
      }
    } else if (targetType == float.class || targetType == Float.class) {
      try {
        return Float.parseFloat(value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid float value: " + value, e);
      }
    } else if (targetType == double.class || targetType == Double.class) {
      try {
        return Double.parseDouble(value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid double value: " + value, e);
      }
    } else if (targetType == boolean.class || targetType == Boolean.class) {
      // Boolean conversion (note: empty string case already handled above)
      return Boolean.parseBoolean(value);
    } else if (targetType.isEnum()) {
      // Handle enum types
      try {
        // Special handling for IndexType which supports synonyms
        if (targetType == org.apache.geode.cache.query.IndexType.class) {
          @SuppressWarnings("deprecation")
          org.apache.geode.cache.query.IndexType indexType =
              org.apache.geode.cache.query.IndexType.valueOfSynonym(value);
          return indexType;
        }

        // Try case-insensitive enum matching by uppercasing the value
        @SuppressWarnings({"unchecked", "rawtypes"})
        Enum<?> enumValue = Enum.valueOf((Class<Enum>) targetType, value.toUpperCase());
        return enumValue;
      } catch (IllegalArgumentException e) {
        // Invalid enum value - throw to cause parse failure
        throw new IllegalArgumentException(
            "Invalid enum value: " + value + " for type " + targetType.getSimpleName(), e);
      }
    } else if (targetType == org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding.ConfigProperty[].class) {
      // Handle ConfigProperty[] with custom converter
      // Spring Shell 3.x migration: ConfigProperty uses JSON-like syntax with commas inside objects
      // Must parse BEFORE generic array handling which would incorrectly split by comma
      // Example:
      // "{'name':'prop1','value':'v1','type':'t1'},{'name':'prop2','value':'v2','type':'t2'}"

      if (value == null || value.isEmpty()) {
        return new org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding.ConfigProperty[0];
      }

      // Use ConfigPropertyConverter for parsing
      org.apache.geode.management.internal.cli.converters.ConfigPropertyConverter converter =
          new org.apache.geode.management.internal.cli.converters.ConfigPropertyConverter();
      return converter.convert(value);
    } else if (targetType.isArray()) {
      // Handle array types (String[], int[], custom object arrays, etc.)

      // If value is null, return null for the array
      if (value == null) {
        return null;
      }

      Class<?> componentType = targetType.getComponentType();

      // Spring Shell 3.x migration: Special handling for authorizer-parameters option
      // This option uses semicolon (;) as separator instead of comma (,) because the
      // parameter values may contain commas (e.g., regex patterns like "{4,8}")
      String delimiter = ",";
      if ("authorizer-parameters".equals(optionName)) {
        delimiter = ";";
      }

      // Split value by delimiter
      String[] parts = value.split(delimiter);

      // Create array of appropriate type
      Object array = java.lang.reflect.Array.newInstance(componentType, parts.length);

      // Convert each part to the component type
      for (int i = 0; i < parts.length; i++) {
        String part = parts[i].trim();
        Object element = convertValue(part, componentType, "", ""); // Recursive call for each
                                                                    // element
        java.lang.reflect.Array.set(array, i, element);
      }

      return array;
    } else if (targetType == File.class) {
      return new java.io.File(value);
    } else if (targetType == org.apache.geode.management.internal.cli.util.ConnectionEndpoint.class) {
      // Parse ConnectionEndpoint format: "host[port]" or "host:port"
      return parseConnectionEndpoint(value);
    } else if (targetType == org.apache.geode.management.configuration.ClassName.class) {
      // Parse ClassName - validates format and supports JSON properties
      return parseClassName(value);
    } else if (targetType == org.apache.geode.cache.ExpirationAction.class) {
      // Parse ExpirationAction - validates against known constants
      return parseExpirationAction(value);
    }

    // For other types, return the string value and let runtime handle it
    return value;
  }

  /**
   * Parses a connection endpoint string in the format "host[port]" or "host:port"
   * into a ConnectionEndpoint object.
   */
  private org.apache.geode.management.internal.cli.util.ConnectionEndpoint parseConnectionEndpoint(
      String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }

    String host;
    int port;

    // Try format: "host[port]"
    if (value.contains("[") && value.contains("]")) {
      int bracketStart = value.indexOf('[');
      int bracketEnd = value.indexOf(']');

      if (bracketStart < bracketEnd) {
        host = value.substring(0, bracketStart);
        String portStr = value.substring(bracketStart + 1, bracketEnd);
        try {
          port = Integer.parseInt(portStr);
          return new org.apache.geode.management.internal.cli.util.ConnectionEndpoint(host, port);
        } catch (NumberFormatException e) {
          // Invalid port number
          return null;
        }
      }
    }

    // Try format: "host:port"
    if (value.contains(":")) {
      int colonIndex = value.lastIndexOf(':');
      host = value.substring(0, colonIndex);
      String portStr = value.substring(colonIndex + 1);
      try {
        port = Integer.parseInt(portStr);
        return new org.apache.geode.management.internal.cli.util.ConnectionEndpoint(host, port);
      } catch (NumberFormatException e) {
        // Invalid port number
        return null;
      }
    }

    // Invalid format
    return null;
  }

  /**
   * Parses a ClassName string which can be in one of these formats:
   * 1. Simple class name: "com.example.MyClass"
   * 2. Class name with JSON properties: "com.example.MyClass{'key':'value'}"
   *
   * @param value the string to parse
   * @return ClassName object or null if parsing fails
   * @throws IllegalArgumentException if the class name format is invalid
   */
  private org.apache.geode.management.configuration.ClassName parseClassName(String value) {
    if (value == null) {
      return null;
    }

    // Shell 3.x: Empty string means "clear/remove" - return ClassName.EMPTY
    if (value.isEmpty()) {
      return org.apache.geode.management.configuration.ClassName.EMPTY;
    }

    // Check if value contains JSON properties (indicated by '{')
    if (value.contains("{")) {
      // Format: "com.example.MyClass{'key':'value'}"
      int braceIndex = value.indexOf('{');
      String className = value.substring(0, braceIndex).trim();
      String jsonProperties = value.substring(braceIndex);

      // Validate class name format
      if (!org.apache.geode.management.configuration.ClassName.isClassNameValid(className)) {
        throw new IllegalArgumentException("Invalid class name: " + className);
      }

      try {
        // ClassName constructor will parse the JSON
        return new org.apache.geode.management.configuration.ClassName(className, jsonProperties);
      } catch (IllegalArgumentException e) {
        // Re-throw with better context
        throw new IllegalArgumentException(
            "Invalid ClassName format: " + value + ". " + e.getMessage(), e);
      }
    } else {
      // Simple class name without properties
      // Validate class name format
      if (!org.apache.geode.management.configuration.ClassName.isClassNameValid(value)) {
        throw new IllegalArgumentException("Invalid class name: " + value);
      }

      return new org.apache.geode.management.configuration.ClassName(value);
    }
  }

  /**
   * Parses an ExpirationAction string into the corresponding ExpirationAction object.
   * ExpirationAction is not an enum - it's a class with static final fields.
   * Supported values: INVALIDATE, DESTROY, LOCAL_INVALIDATE, LOCAL_DESTROY
   *
   * @param value the string to parse (case-insensitive)
   * @return ExpirationAction object or null if value is null/empty
   * @throws IllegalArgumentException if the expiration action is invalid
   */
  private org.apache.geode.cache.ExpirationAction parseExpirationAction(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }

    // Convert to uppercase for case-insensitive matching
    String upperValue = value.toUpperCase().trim();

    // Match against known ExpirationAction constants
    switch (upperValue) {
      case "INVALIDATE":
        return org.apache.geode.cache.ExpirationAction.INVALIDATE;
      case "DESTROY":
        return org.apache.geode.cache.ExpirationAction.DESTROY;
      case "LOCAL_INVALIDATE":
      case "LOCAL-INVALIDATE": // Support both underscore and hyphen
        return org.apache.geode.cache.ExpirationAction.LOCAL_INVALIDATE;
      case "LOCAL_DESTROY":
      case "LOCAL-DESTROY": // Support both underscore and hyphen
        return org.apache.geode.cache.ExpirationAction.LOCAL_DESTROY;
      default:
        throw new IllegalArgumentException(
            "Invalid ExpirationAction: " + value +
                ". Valid values are: INVALIDATE, DESTROY, LOCAL_INVALIDATE, LOCAL_DESTROY");
    }
  }

  /**
   * Determines if a parameter represents a region name that should be converted to a region path.
   * This checks if the command is a region-related command and the parameter is named
   * appropriately.
   *
   * @param method the command method
   * @param option the parameter's @ShellOption annotation
   * @return true if this parameter should have "/" prefix added for region path conversion
   */
  private boolean isRegionNameParameter(java.lang.reflect.Method method,
      org.springframework.shell.standard.ShellOption option) {
    // Get the declaring class name
    String className = method.getDeclaringClass().getSimpleName();

    // Check if this is a region-related command
    boolean isRegionCommand = className.contains("Region");

    if (!isRegionCommand) {
      return false;
    }

    // Check if the parameter name suggests it's a region path
    // The @ShellOption.value() array contains the option names (e.g., "name", "region")
    for (String optionName : option.value()) {
      if ("name".equals(optionName) || "region".equals(optionName) ||
          "regionName".equals(optionName)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Converts a region name to a region path by adding the "/" separator prefix if not present.
   *
   * Shell 3.x migration: Mimics behavior of Shell 1.x RegionPathConverter which was part of the
   * automatic conversion framework. Since Shell 3.x removed Converters, this logic must be
   * applied manually during parameter binding.
   *
   * @param value the region name value
   * @return region path with "/" prefix, or original value if already has prefix
   * @throws IllegalArgumentException if value is just "/" (invalid region path)
   */
  private String convertToRegionPath(String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }

    // Don't convert sentinel values - they need to be handled by convertValue first
    if (OPTION_NOT_VALUED.equals(value)) {
      return value;
    }

    // Get the region separator from the constant
    final String SEPARATOR = "/"; // org.apache.geode.cache.Region.SEPARATOR

    // Invalid: just the separator alone
    if (value.equals(SEPARATOR)) {
      throw new IllegalArgumentException("invalid region path: " + value);
    }

    // Add separator prefix if not present
    if (!value.startsWith(SEPARATOR)) {
      value = SEPARATOR + value;
    }

    return value;
  }

  /**
   * Command completion implementation for Shell 3.x.
   *
   * Spring Shell 3.x migration: Provides tab completion using JLine 3.x Candidate model.
   * Shell 1.x used Completion objects and SimpleParser's built-in completion.
   * Shell 3.x requires custom completion logic that integrates with JLine 3.x's API.
   * This method analyzes context to determine whether completing command names, option names, or
   * option values.
   *
   * @param userInput the command line input
   * @param cursor the cursor position
   * @return CompletionContext indicating what type of completion is needed
   */
  private CompletionContext analyzeContext(String userInput, int cursor) {
    if (userInput == null || userInput.trim().isEmpty()) {
      return CompletionContext.commandName("");
    }

    List<String> tokens = splitUserInput(userInput.trim());

    if (tokens.isEmpty()) {
      return CompletionContext.commandName("");
    }

    // Find the command name (everything before the first option)
    StringBuilder commandNameBuilder = new StringBuilder();
    for (int i = 0; i < tokens.size(); i++) {
      String token = tokens.get(i);
      if (token.startsWith(LONG_OPTION_SPECIFIER)) {
        break;
      }
      if (i > 0) {
        commandNameBuilder.append(" ");
      }
      commandNameBuilder.append(token);
    }
    String commandName = commandNameBuilder.toString();

    // Get the last token
    String lastToken = tokens.get(tokens.size() - 1);

    // Check if we're completing option value after "="
    if (userInput.endsWith("=")) {
      // The "=" has already been stripped by splitUserInput, so lastToken is the option name
      String optionName = lastToken;
      return CompletionContext.optionValue(commandName, optionName, "");
    }

    // Check if last token contains "=" (completing value with partial input)
    if (lastToken.contains("=")) {
      int equalsIndex = lastToken.indexOf("=");
      String optionName = lastToken.substring(0, equalsIndex);
      String partialValue = lastToken.substring(equalsIndex + 1);
      return CompletionContext.optionValue(commandName, optionName, partialValue);
    }

    // Check if last token is an option name (starts with "--")
    if (lastToken.startsWith(LONG_OPTION_SPECIFIER)) {
      // This is an option without value yet - prepare for value completion
      return CompletionContext.optionValue(commandName, lastToken, "");
    }

    // Check if we should complete option names (user typed command + space)
    // e.g., "configure pdx " or "create region --name=test "
    if (!commandName.isEmpty() && userInput.endsWith(" ")) {
      // Extract partial option name if any
      String partialOption = "";
      return CompletionContext.optionName(commandName, partialOption);
    }

    // Default: completing command name
    return CompletionContext.commandName(commandName);
  }

  /**
   * Completes option values using the completion provider registry.
   *
   * Shell 3.x migration: Shell 1.x Converters provided getAllPossibleValues() for completions.
   * Shell 3.x removed Converters, so this uses CompletionProviderRegistry to supply enum,
   * boolean, and other type-specific completions.
   *
   * @param commandName the command name (e.g., "import cluster-configuration")
   * @param optionName the option name (e.g., "--action")
   * @param partialValue the partial value typed by user (e.g., "AP")
   * @return list of completion candidates
   */
  private List<Completion> completeOptionValue(String commandName, String optionName,
      String partialValue) {
    try {
      // Find the command method through helper
      Method method = commandManager.getHelper().getCommandMethod(commandName);

      if (method == null) {
        return new ArrayList<>();
      }

      // Find the parameter for this option
      Parameter[] parameters = method.getParameters();

      for (Parameter parameter : parameters) {
        ShellOption annotation = parameter.getAnnotation(ShellOption.class);
        if (annotation != null) {
          // Check if any of the option names match
          for (String optName : annotation.value()) {
            String fullOptionName = LONG_OPTION_SPECIFIER + optName;

            if (fullOptionName.equals(optionName)) {
              // Found the parameter! Get its type
              Class<?> parameterType = parameter.getType();

              // Use completion provider registry to get completions
              CompletionContext context =
                  CompletionContext.optionValue(commandName, optionName, partialValue);
              List<Completion> completions = completionProviderRegistry.getCompletions(
                  parameterType, partialValue, context);
              return completions;
            }
          }
        }
      }
    } catch (Exception e) {
      // If anything goes wrong, return empty list
      return new ArrayList<>();
    }

    return new ArrayList<>();
  }

  /**
   * Completes option names for a given command.
   * Returns all available option names (with "--" prefix) for the specified command.
   *
   * Shell 3.x migration: Shell 1.x provided option completion through SimpleParser.
   * This manually extracts option names from @ShellOption annotations for completion suggestions.
   *
   * @param commandName the command name (e.g., "configure pdx")
   * @param partialOption the partial option name typed by user (currently unused, for future
   *        filtering)
   * @return list of option name completions
   */
  private List<Completion> completeOptionName(String commandName, String partialOption) {
    List<Completion> completions = new ArrayList<>();

    try {
      // Find the command method through helper
      Method method = commandManager.getHelper().getCommandMethod(commandName);

      if (method == null) {
        return completions;
      }

      // Get all parameters with ShellOption annotations
      Parameter[] parameters = method.getParameters();

      for (Parameter parameter : parameters) {
        ShellOption annotation = parameter.getAnnotation(ShellOption.class);
        if (annotation != null) {
          // Add all option names (typically there's a primary name and possibly aliases)
          for (String optName : annotation.value()) {
            String fullOptionName = LONG_OPTION_SPECIFIER + optName;
            // Filter by partial input if provided
            if (partialOption.isEmpty()
                || fullOptionName.startsWith(LONG_OPTION_SPECIFIER + partialOption)) {
              completions.add(new Completion(fullOptionName));
            }
          }
        }
      }
    } catch (Exception e) {
      // If anything goes wrong, return empty list
      return new ArrayList<>();
    }

    return completions;
  }

  /**
   * Command completion entry point for Shell 3.x integration.
   *
   * Spring Shell 3.x migration: Provides tab completion compatible with JLine 3.x.
   * Shell 1.x used SimpleParser.completeAdvanced() with Spring Shell's Completion model.
   * Shell 3.x requires custom implementation that:
   * - Analyzes completion context (command name, option name, or option value)
   * - Uses CompletionProviderRegistry for type-specific completions
   * - Returns proper cursor position for JLine 3.x rendering
   *
   * @param userInput the command line input
   * @param cursor the cursor position (typically at the end of userInput)
   * @param candidates the list to populate with completion candidates
   * @return the cursor position where the completion begins in the original string
   */
  public int completeAdvanced(String userInput, int cursor, final List<Completion> candidates) {
    if (userInput == null || userInput.trim().isEmpty()) {
      return -1;
    }

    // Analyze what type of completion is needed
    CompletionContext context = analyzeContext(userInput, cursor);

    // Handle option name completion
    if (context.getType() == CompletionContext.Type.OPTION_NAME) {
      List<Completion> completions = completeOptionName(
          context.getCommandName(),
          context.getPartialInput());

      if (!completions.isEmpty()) {
        candidates.addAll(completions);
        return userInput.length();
      }
    }

    // Handle option value completion (the main case for our tests)
    if (context.getType() == CompletionContext.Type.OPTION_VALUE) {
      List<Completion> completions = completeOptionValue(
          context.getCommandName(),
          context.getOptionName(),
          context.getPartialInput());

      if (!completions.isEmpty()) {
        // Determine cursor position based on input format
        if (userInput.endsWith("=")) {
          // Test 1 case: "... --action=" → cursor at end, add value directly
          candidates.addAll(completions);
          return userInput.length();
        } else if (context.getOptionName() != null && !userInput.endsWith("=")) {
          // Test 2 case: "... --order-policy" → need to add "=" + value
          for (Completion completion : completions) {
            candidates.add(new Completion("=" + completion.getValue()));
          }
          return userInput.length();
        } else {
          // Partial value case: "... --action=AP" → replace from "="
          candidates.addAll(completions);
          int equalsPos = userInput.lastIndexOf('=');
          return equalsPos + 1;
        }
      }
    }

    // No completions found or unsupported completion type
    return -1;
  }
}
