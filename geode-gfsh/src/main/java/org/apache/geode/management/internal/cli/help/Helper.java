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
import org.springframework.shell.core.MethodTarget;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

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
  private final Map<String, MethodTarget> availabilityIndicators = new HashMap<>();

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

  public void addCommand(CliCommand command, Method commandMethod) {
    // put all the command synonyms in the command map
    Arrays.stream(command.value()).forEach(cmd -> commands.put(cmd, commandMethod));

    // resolve the hint message for each method
    CliMetaData cliMetaData = commandMethod.getDeclaredAnnotation(CliMetaData.class);
    if (cliMetaData == null)
      return;
    String[] related = cliMetaData.relatedTopic();

    // for hint message, we only need to show the first synonym
    String commandString = command.value()[0];
    Arrays.stream(related).forEach(topic -> {
      Topic foundTopic = topics.get(topic);
      if (foundTopic == null) {
        throw new IllegalArgumentException("No such topic found in the initial map: " + topic);
      }
      foundTopic.addRelatedCommand(commandString, command.help());
    });
  }

  public void addAvailabilityIndicator(CliAvailabilityIndicator availability, MethodTarget target) {
    Arrays.stream(availability.value())
        .forEach(command -> availabilityIndicators.put(command, target));
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
    CliCommand cliCommand = m.getDeclaredAnnotation(CliCommand.class);
    Annotation[][] annotations = m.getParameterAnnotations();

    if (annotations == null || annotations.length == 0) {
      // can't validate arguments if command doesn't have any
      return null;
    }

    // loop through the required options and check that they appear in the buffer
    StringBuilder builder = new StringBuilder();
    for (Annotation[] annotation : annotations) {
      CliOption cliOption = getAnnotation(annotation, CliOption.class);
      String option = getPrimaryKey(cliOption);
      boolean required = cliOption.mandatory();
      boolean requiredWithEquals = true;

      if (isNonEmptyAnnotation(cliOption.specifiedDefaultValue())) {
        requiredWithEquals = false;
      }
      if (isNonEmptyAnnotation(cliOption.unspecifiedDefaultValue())) {
        required = false;
      }
      if (required) {
        String lookFor = "--" + option + (requiredWithEquals ? "=" : "");
        if (!userInput.contains(lookFor)) {
          builder.append("  --").append(option).append(requiredWithEquals ? "=" : "")
              .append("  is required").append(LINE_SEPARATOR);
        }
      }
    }
    if (builder.length() > 0) {
      String commandName = cliCommand.value()[0];
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
        .map(m -> getHelp(m.getDeclaredAnnotation(CliCommand.class),
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
    List<String> topicKeys = this.topics.keySet()
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
      Topic oneTopic = this.topics.get(topicKeys.get(0));
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
    MethodTarget target = availabilityIndicators.get(command);
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
        .addChild(getHelp(method.getDeclaredAnnotation(CliCommand.class), null, null)));

    return root;
  }

  /**
   * returns a short description and help string of the command if annotations is null or returns a
   * details description of the command with the syntax and parameter description
   */
  HelpBlock getHelp(CliCommand cliCommand, Annotation[][] annotations, Class<?>[] parameterTypes) {
    String commandName = cliCommand.value()[0];
    boolean isAvailable = isAvailable(commandName);

    if (annotations == null && parameterTypes == null) {
      String available = isAvailable ? AVAILABLE : NOT_AVAILABLE;
      HelpBlock help = new HelpBlock(commandName + " (" + available + ")");
      help.addChild(new HelpBlock(cliCommand.help()));
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
    String[] allNames = cliCommand.value();
    if (allNames.length > 1) {
      HelpBlock synonyms = new HelpBlock(SYNONYMS_NAME);
      for (int i = 1; i < allNames.length; i++) {
        synonyms.addChild(new HelpBlock(allNames[i]));
      }
      root.addChild(synonyms);
    }

    // Now comes the turn to display synopsis if any
    if (StringUtils.isNotBlank(cliCommand.help())) {
      HelpBlock synopsis = new HelpBlock(SYNOPSIS_NAME);
      synopsis.addChild(new HelpBlock(cliCommand.help()));
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
        CliOption cliOption = getAnnotation(annotation, CliOption.class);
        HelpBlock optionNode = getOptionDetail(cliOption);
        options.addChild(optionNode);
      }

      root.addChild(options);
    }
    return root;
  }

  HelpBlock getOptionDetail(CliOption cliOption) {
    HelpBlock optionNode = new HelpBlock(getPrimaryKey(cliOption));
    String help = cliOption.help();
    optionNode.addChild(new HelpBlock((StringUtils.isNotBlank(help) ? help : "")));
    if (getSynonyms(cliOption).size() > 0) {
      StringBuilder builder = new StringBuilder();
      for (String string : getSynonyms(cliOption)) {
        if (builder.length() > 0) {
          builder.append(",");
        }
        builder.append(string);
      }
      optionNode.addChild(new HelpBlock(SYNONYMS_SUB_NAME + builder.toString()));
    }
    optionNode.addChild(
        new HelpBlock(REQUIRED_SUB_NAME + ((cliOption.mandatory()) ? TRUE_TOKEN : FALSE_TOKEN)));
    if (isNonEmptyAnnotation(cliOption.specifiedDefaultValue())) {
      optionNode.addChild(
          new HelpBlock(SPECIFIEDDEFAULTVALUE_SUB_NAME + cliOption.specifiedDefaultValue()));
    }
    if (isNonEmptyAnnotation(cliOption.unspecifiedDefaultValue())) {
      optionNode.addChild(new HelpBlock(
          UNSPECIFIEDDEFAULTVALUE_VALUE_SUB_NAME + cliOption.unspecifiedDefaultValue()));
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

  String getSyntaxString(String commandName, Annotation[][] annotations, Class[] parameterTypes) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(commandName);
    for (int i = 0; i < annotations.length; i++) {
      CliOption cliOption = getAnnotation(annotations[i], CliOption.class);
      String optionString = getOptionString(cliOption, parameterTypes[i]);
      if (cliOption.mandatory()) {
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
  private static String getOptionString(CliOption cliOption, Class<?> optionType) {
    String key0 = cliOption.key()[0];
    if ("".equals(key0)) {
      return (cliOption.key()[1]);
    }

    StringBuilder buffer = new StringBuilder();
    buffer.append(LONG_OPTION_SPECIFIER).append(key0);

    boolean hasSpecifiedDefault = isNonEmptyAnnotation(cliOption.specifiedDefaultValue());

    if (hasSpecifiedDefault) {
      buffer.append("(");
    }

    buffer.append(OPTION_VALUE_SPECIFIER).append(VALUE_FIELD);

    if (hasSpecifiedDefault) {
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

  private static String getPrimaryKey(CliOption option) {
    String[] keys = option.key();
    if (keys.length == 0) {
      throw new RuntimeException("Invalid option keys");
    } else if ("".equals(keys[0])) {
      return keys[1];
    } else {
      return keys[0];
    }
  }

  private static List<String> getSynonyms(CliOption option) {
    List<String> synonyms = new ArrayList<>();
    String[] keys = option.key();
    if (keys.length < 2)
      return synonyms;
    // if the primary key is empty (like sh and help command), then there should be no synonyms.
    if ("".equals(keys[0]))
      return synonyms;

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
      CliOption cliOption = getAnnotation(annotation, CliOption.class);
      optionList.add(getPrimaryKey(cliOption));
    }
    return optionList;
  }
}
