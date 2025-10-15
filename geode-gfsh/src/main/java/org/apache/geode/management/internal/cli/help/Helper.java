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
package org.apache.geode.management.internal.cli.help;

import static org.apache.geode.management.internal.cli.GfshParser.LINE_SEPARATOR;
import static org.apache.geode.management.internal.cli.GfshParser.LONG_OPTION_SPECIFIER;
import static org.apache.geode.management.internal.cli.GfshParser.OPTION_VALUE_SPECIFIER;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 *
 *
 * @since GemFire 7.0
 */
public class Helper {

  static final String NAME_NAME = "NAME";
  static final String SYNONYMS_NAME = "SYNONYMS";
  static final String SYNOPSIS_NAME = "SYNOPSIS";
  static final String SYNTAX_NAME = "SYNTAX";
  static final String OPTIONS_NAME = "PARAMETERS";
  static final String IS_AVAILABLE_NAME = "IS AVAILABLE";

  private static final String REQUIRED_SUB_NAME = "Required: ";
  private static final String SYNONYMS_SUB_NAME = "Synonyms: ";
  private static final String SPECIFIEDDEFAULTVALUE_SUB_NAME =
      "Default (if the parameter is specified without value): ";
  private static final String UNSPECIFIEDDEFAULTVALUE_VALUE_SUB_NAME =
      "Default (if the parameter is not specified): ";

  private static final String VALUE_FIELD = "value";
  private static final String TRUE_TOKEN = "true";
  private static final String FALSE_TOKEN = "false";
  private static final String AVAILABLE = "Available";
  private static final String NOT_AVAILABLE = "Not Available";
  private static final String NO_HELP_EXISTS_FOR_THIS_COMMAND = "No help exists for this command.";
  private static final String HELP_INSTRUCTIONS = LINE_SEPARATOR + "Use " + CliStrings.HELP
      + " <command name> to display detailed usage information for a specific command."
      + LINE_SEPARATOR
      + "Help with command and parameter completion can also be obtained by entering all or a portion of either followed by the \"TAB\" key.";

  private final Map<String, Topic> topics = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  private final Map<String, Method> commands = new TreeMap<>();
  // Spring Shell 3.x: Store availability indicator methods directly
  private final Map<String, AvailabilityTarget> availabilityIndicators = new HashMap<>();

  // Helper class to replace Spring Shell 2.x MethodTarget
  private static class AvailabilityTarget {
    private final Method method;
    private final Object target;

    AvailabilityTarget(Method method, Object target) {
      this.method = method;
      this.target = target;
    }

    Method getMethod() {
      return method;
    }

    Object getTarget() {
      return target;
    }
  }

  public Helper() {
    initTopic(CliStrings.DEFAULT_TOPIC_GEODE, CliStrings.DEFAULT_TOPIC_GEODE__DESC);
    initTopic(CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_REGION__DESC);
    initTopic(CliStrings.TOPIC_GEODE_WAN, CliStrings.TOPIC_GEODE_WAN__DESC);
    initTopic(CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_JMX__DESC);
    initTopic(CliStrings.TOPIC_GEODE_DISKSTORE, CliStrings.TOPIC_GEODE_DISKSTORE__DESC);
    initTopic(CliStrings.TOPIC_GEODE_LOCATOR, CliStrings.TOPIC_GEODE_LOCATOR__DESC);
    initTopic(CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_SERVER__DESC);
    initTopic(CliStrings.TOPIC_GEODE_MANAGER, CliStrings.TOPIC_GEODE_MANAGER__DESC);
    initTopic(CliStrings.TOPIC_GEODE_STATISTICS, CliStrings.TOPIC_GEODE_STATISTICS__DESC);
    initTopic(CliStrings.TOPIC_GEODE_LIFECYCLE, CliStrings.TOPIC_GEODE_LIFECYCLE__DESC);
    initTopic(CliStrings.TOPIC_GEODE_M_AND_M, CliStrings.TOPIC_GEODE_M_AND_M__DESC);
    initTopic(CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_DATA__DESC);
    initTopic(CliStrings.TOPIC_GEODE_CONFIG, CliStrings.TOPIC_GEODE_CONFIG__DESC);
    initTopic(CliStrings.TOPIC_GEODE_FUNCTION, CliStrings.TOPIC_GEODE_FUNCTION__DESC);
    initTopic(CliStrings.TOPIC_GEODE_HELP, CliStrings.TOPIC_GEODE_HELP__DESC);
    initTopic(CliStrings.TOPIC_GEODE_DEBUG_UTIL, CliStrings.TOPIC_GEODE_DEBUG_UTIL__DESC);
    initTopic(CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GFSH__DESC);
    initTopic(CliStrings.TOPIC_LOGS, CliStrings.TOPIC_LOGS__DESC);
    initTopic(CliStrings.TOPIC_CLIENT, CliStrings.TOPIC_CLIENT__DESC);
  }

  private void initTopic(String topic, String desc) {
    topics.put(topic, new Topic(topic, desc));
  }

  public void addCommand(ShellMethod command, Method commandMethod) {
    // Spring Shell 3.x: key contains command names, value contains help text
    // put all the command synonyms in the command map
    String[] keys = command.key();
    if (keys.length == 0) {
      // If no keys specified, use method name
      keys = new String[] {commandMethod.getName()};
    }
    for (String cmd : keys) {
      commands.put(cmd, commandMethod);
    }

    // resolve the hint message for each method
    CliMetaData cliMetaData = commandMethod.getDeclaredAnnotation(CliMetaData.class);
    if (cliMetaData == null) {
      return;
    }
    String[] related = cliMetaData.relatedTopic();

    // for hint message, we only need to show the first synonym
    String commandString = keys[0];
    Arrays.stream(related).forEach(topic -> {
      Topic foundTopic = topics.get(topic);
      if (foundTopic == null) {
        throw new IllegalArgumentException("No such topic found in the initial map: " + topic);
      }
      foundTopic.addRelatedCommand(commandString, command.value());
    });
  }

  public void addAvailabilityIndicator(ShellMethodAvailability availability, Method method,
      Object target) {
    // Spring Shell 3.x: Store method and target for availability checking
    AvailabilityTarget availabilityTarget = new AvailabilityTarget(method, target);
    Arrays.stream(availability.value())
        .forEach(command -> availabilityIndicators.put(command, availabilityTarget));
  }

  /**
   * get mini-help for commands entered without all required parameters
   *
   * @return null if unable to identify anything missing
   */
  public String getMiniHelp(String userInput) {
    if (StringUtils.isBlank(userInput)) {
      return null;
    }

    List<Method> methodList = commands.keySet()
        .stream()
        .filter(key -> key.startsWith(getCommandPart(userInput)))
        .map(commands::get).collect(Collectors.toList());

    if (methodList.size() != 1) {
      // can't validate arguments if buffer is not a single command
      return null;
    }

    Method m = methodList.get(0);
    ShellMethod shellMethod = m.getDeclaredAnnotation(ShellMethod.class);
    Annotation[][] annotations = m.getParameterAnnotations();

    if (annotations == null || annotations.length == 0) {
      // can't validate arguments if command doesn't have any
      return null;
    }

    // loop through the required options and check that they appear in the buffer
    StringBuilder builder = new StringBuilder();
    for (Annotation[] annotation : annotations) {
      ShellOption shellOption = getAnnotation(annotation, ShellOption.class);
      String option = getPrimaryKey(shellOption);
      boolean required = shellOption.defaultValue().equals(ShellOption.NULL);
      // In Shell 3.x, if an option has a non-null default value, it's not required
      boolean requiredWithEquals = required;

      if (required) {
        String lookFor = "--" + option + (requiredWithEquals ? "=" : "");
        if (!userInput.contains(lookFor)) {
          builder.append("  --").append(option).append(requiredWithEquals ? "=" : "")
              .append("  is required").append(LINE_SEPARATOR);
        }
      }
    }
    if (builder.length() > 0) {
      String commandName = shellMethod.key().length > 0 ? shellMethod.key()[0] : m.getName();
      builder.append("Use \"help ").append(commandName)
          .append("\" (without the quotes) for detailed usage information.")
          .append(LINE_SEPARATOR);
      return builder.toString();
    } else {
      return null;
    }
  }

  private String getCommandPart(String userInput) {
    int parms = userInput.indexOf(" --");
    return (parms < 0 ? userInput : userInput.substring(0, parms)).trim();
  }

  /**
   * get help string for a specific command, or a brief description of all the commands if buffer is
   * null or empty
   */
  public String getHelp(String buffer, int terminalWidth) {
    if (StringUtils.isBlank(buffer)) {
      return getHelp().toString(terminalWidth);
    }

    List<Method> methodList = commands.keySet()
        .stream()
        .filter(key -> key.startsWith(buffer))
        .map(commands::get).collect(Collectors.toList());

    boolean summarize = methodList.size() > 1;
    String helpString = methodList.stream()
        .map(m -> getHelp(m.getDeclaredAnnotation(ShellMethod.class),
            summarize ? null : m.getParameterAnnotations(),
            summarize ? null : m.getParameterTypes()))
        .map(helpBlock -> helpBlock.toString(terminalWidth))
        .reduce((s, s2) -> s + s2)
        .orElse(NO_HELP_EXISTS_FOR_THIS_COMMAND);

    if (summarize) {
      helpString += HELP_INSTRUCTIONS;
    }

    return helpString;
  }

  public String getHint(String buffer) {
    List<String> topicKeys = topics.keySet()
        .stream()
        .filter(t -> buffer == null || t.toLowerCase().startsWith(buffer.toLowerCase()))
        .sorted()
        .collect(Collectors.toList());

    StringBuilder builder = new StringBuilder();
    // if no topic is provided, return a list of topics
    if (topicKeys.isEmpty()) {
      builder.append(CliStrings.format(CliStrings.HINT__MSG__UNKNOWN_TOPIC, buffer))
          .append(LINE_SEPARATOR).append(LINE_SEPARATOR);
    } else if (topicKeys.size() == 1) {
      Topic oneTopic = topics.get(topicKeys.get(0));
      builder.append(oneTopic.desc).append(LINE_SEPARATOR)
          .append(LINE_SEPARATOR);
      oneTopic.relatedCommands.stream().sorted().forEach(command -> builder.append(command.command)
          .append(": ").append(command.desc).append(LINE_SEPARATOR));
    } else {
      builder.append(CliStrings.HINT__MSG__TOPICS_AVAILABLE).append(LINE_SEPARATOR)
          .append(LINE_SEPARATOR);

      topicKeys.forEach(topic -> builder.append(topic).append(LINE_SEPARATOR));
    }

    return builder.toString();
  }

  public Set<String> getTopicNames() {
    return topics.keySet();
  }

  private boolean isAvailable(String command) {
    AvailabilityTarget target = availabilityIndicators.get(command);
    if (target == null) {
      return true;
    }
    try {
      return (Boolean) target.getMethod().invoke(target.getTarget());
    } catch (Exception e) {
      return false;
    }
  }

  public boolean hasAvailabilityIndicator(String command) {
    return availabilityIndicators.get(command) != null;
  }

  private HelpBlock getHelp() {
    HelpBlock root = new HelpBlock();
    commands.keySet().stream().sorted().map(commands::get).forEach(method -> root
        .addChild(getHelp(method.getDeclaredAnnotation(ShellMethod.class), null, null)));

    return root;
  }

  /**
   * returns a short description and help string of the command if annotations is null or returns a
   * details description of the command with the syntax and parameter description
   */
  HelpBlock getHelp(ShellMethod shellMethod, Annotation[][] annotations,
      Class<?>[] parameterTypes) {
    // Spring Shell 3.x: key contains command names, value contains help text
    String commandName = shellMethod.key().length > 0 ? shellMethod.key()[0] : "";
    boolean isAvailable = isAvailable(commandName);

    if (annotations == null && parameterTypes == null) {
      String available = isAvailable ? AVAILABLE : NOT_AVAILABLE;
      HelpBlock help = new HelpBlock(commandName + " (" + available + ")");
      help.addChild(new HelpBlock(shellMethod.value()));
      return help;
    }

    HelpBlock root = new HelpBlock();
    // First we will have the block for NAME of the command
    HelpBlock name = new HelpBlock(NAME_NAME);
    name.addChild(new HelpBlock(commandName));
    root.addChild(name);

    // add the availability flag
    HelpBlock availability = new HelpBlock(IS_AVAILABLE_NAME);
    availability.addChild(new HelpBlock(isAvailable + ""));
    root.addChild(availability);

    // Now add synonyms if any
    String[] allNames = shellMethod.key();
    if (allNames.length > 1) {
      HelpBlock synonyms = new HelpBlock(SYNONYMS_NAME);
      for (int i = 1; i < allNames.length; i++) {
        synonyms.addChild(new HelpBlock(allNames[i]));
      }
      root.addChild(synonyms);
    }

    // Now comes the turn to display synopsis if any
    if (StringUtils.isNotBlank(shellMethod.value())) {
      HelpBlock synopsis = new HelpBlock(SYNOPSIS_NAME);
      synopsis.addChild(new HelpBlock(shellMethod.value()));
      root.addChild(synopsis);
    }

    // Now display the syntax for the command
    HelpBlock syntaxBlock = new HelpBlock(SYNTAX_NAME);
    String syntax = getSyntaxString(commandName, annotations, parameterTypes);
    syntaxBlock.addChild(new HelpBlock(syntax));
    root.addChild(syntaxBlock);

    // Detailed description of all the Options
    if (annotations.length > 0) {
      HelpBlock options = new HelpBlock(OPTIONS_NAME);
      for (Annotation[] annotation : annotations) {
        ShellOption shellOption = getAnnotation(annotation, ShellOption.class);
        HelpBlock optionNode = getOptionDetail(shellOption);
        options.addChild(optionNode);
      }

      root.addChild(options);
    }
    return root;
  }

  HelpBlock getOptionDetail(ShellOption shellOption) {
    // Spring Shell 3.x: option name can be an array, use first one
    String optionKey = getPrimaryKey(shellOption);
    HelpBlock optionNode = new HelpBlock(optionKey);

    // Shell 3.x doesn't have help() method on ShellOption, description comes from method param docs
    // We'll just show the option details
    if (getSynonyms(shellOption).size() > 0) {
      StringBuilder builder = new StringBuilder();
      for (String string : getSynonyms(shellOption)) {
        if (builder.length() > 0) {
          builder.append(",");
        }
        builder.append(string);
      }
      optionNode.addChild(new HelpBlock(SYNONYMS_SUB_NAME + builder));
    }

    // In Shell 3.x, mandatory is determined by defaultValue being ShellOption.NULL
    boolean isMandatory = shellOption.defaultValue().equals(ShellOption.NULL);
    optionNode.addChild(
        new HelpBlock(REQUIRED_SUB_NAME + (isMandatory ? TRUE_TOKEN : FALSE_TOKEN)));

    // In Shell 3.x, there's only one defaultValue field
    if (!shellOption.defaultValue().equals(ShellOption.NULL)
        && !shellOption.defaultValue().isEmpty()) {
      optionNode.addChild(
          new HelpBlock(SPECIFIEDDEFAULTVALUE_SUB_NAME + shellOption.defaultValue()));
    }

    return optionNode;
  }

  @SuppressWarnings("unchecked")
  private <T> T getAnnotation(Annotation[] annotations, Class<T> klass) {
    for (Annotation annotation : annotations) {
      if (klass.isAssignableFrom(annotation.getClass())) {
        return (T) annotation;
      }
    }
    return null;
  }

  String getSyntaxString(String commandName, Annotation[][] annotations,
      Class<?>[] parameterTypes) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(commandName);
    for (int i = 0; i < annotations.length; i++) {
      ShellOption shellOption = getAnnotation(annotations[i], ShellOption.class);
      String optionString = getOptionString(shellOption, parameterTypes[i]);
      boolean isMandatory = shellOption.defaultValue().equals(ShellOption.NULL);
      if (isMandatory) {
        buffer.append(" ").append(optionString);
      } else {
        buffer.append(" [").append(optionString).append("]");
      }
    }
    return buffer.toString();
  }

  /**
   * this builds the following format of strings: key (as in sh and help) --key=value --key(=value)?
   * (if has specifiedDefaultValue) --key=value(,value)* (if the value is a list)
   *
   * @return option string
   */
  private static String getOptionString(ShellOption shellOption, Class<?> optionType) {
    // Spring Shell 3.x: value() returns option names
    String[] keys = shellOption.value();
    String key0 = keys.length > 0 ? keys[0] : "";
    if ("".equals(key0) && keys.length > 1) {
      return (keys[1]);
    }

    StringBuilder buffer = new StringBuilder();
    buffer.append(LONG_OPTION_SPECIFIER).append(key0);

    boolean hasDefault = !shellOption.defaultValue().equals(ShellOption.NULL)
        && !shellOption.defaultValue().isEmpty();

    if (hasDefault) {
      buffer.append("(");
    }

    buffer.append(OPTION_VALUE_SPECIFIER).append(VALUE_FIELD);

    if (hasDefault) {
      buffer.append(")?");
    }

    if (isCollectionOrArrayType(optionType)) {
      buffer.append("(").append(",").append(VALUE_FIELD).append(")*");
    }

    return buffer.toString();
  }

  private static boolean isCollectionOrArrayType(Class<?> typeToCheck) {
    return typeToCheck != null
        && (typeToCheck.isArray() || Collection.class.isAssignableFrom(typeToCheck));
  }

  private static String getPrimaryKey(ShellOption option) {
    String[] keys = option.value();
    if (keys.length == 0) {
      throw new RuntimeException("Invalid option keys");
    } else if ("".equals(keys[0]) && keys.length > 1) {
      return keys[1];
    } else {
      return keys[0];
    }
  }

  private static List<String> getSynonyms(ShellOption option) {
    List<String> synonyms = new ArrayList<>();
    String[] keys = option.value();
    if (keys.length < 2) {
      return synonyms;
    }
    // if the primary key is empty (like sh and help command), then there should be no synonyms.
    if ("".equals(keys[0])) {
      return synonyms;
    }

    synonyms.addAll(Arrays.asList(keys).subList(1, keys.length));
    return synonyms;
  }

  private static boolean isNonEmptyAnnotation(String value) {
    return !StringUtils.isBlank(value) && !CliMetaData.ANNOTATION_NULL_VALUE.equals(value);
  }

  public Set<String> getCommands() {
    return commands.keySet();
  }

  public Method getCommandMethod(String command) {
    return commands.get(command);

  }

  // methods added for future option comparison
  public Set<String> getOptions(String command) {
    Method method = getCommandMethod(command);
    Set<String> optionList = new HashSet<>();
    Annotation[][] annotations = method.getParameterAnnotations();
    if (annotations == null || annotations.length == 0) {
      // can't validate arguments if command doesn't have any
      return optionList;
    }

    for (Annotation[] annotation : annotations) {
      ShellOption shellOption = getAnnotation(annotation, ShellOption.class);
      optionList.add(getPrimaryKey(shellOption));
    }
    return optionList;
  }
}
