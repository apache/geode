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
package com.gemstone.gemfire.management.internal.cli.help.utils;

import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.internal.cli.help.format.*;
import com.gemstone.gemfire.management.internal.cli.modes.CommandModes;
import com.gemstone.gemfire.management.internal.cli.modes.CommandModes.CommandMode;
import com.gemstone.gemfire.management.internal.cli.parser.Argument;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @since GemFire 7.0
 */
public class HelpUtils {
  public static final String EXE_PREFIX_FOR_EXTERNAL_HELP = com.gemstone.gemfire.management.internal.cli.shell.Gfsh.GFSH_APP_NAME + " ";
  public static final String HELP__COMMAND_AVAILABLE      = "Available";
  public static final String HELP__COMMAND_NOTAVAILABLE   = "Not Available";

  private static final String NAME_NAME         = "NAME";
  private static final String SYNONYMS_NAME     = "SYNONYMS";
  private static final String SYNOPSIS_NAME     = "SYNOPSIS";
  private static final String SYNTAX_NAME       = "SYNTAX";
  private static final String ARGUMENTS_NAME    = "ARGUMENTS";
  private static final String OPTIONS_NAME      = "PARAMETERS";
  private static final String IS_AVAILABLE_NAME = "IS AVAILABLE";
  private static final String MODES = "MODES";

  private static final String REQUIRED_SUB_NAME     = "Required: ";
  private static final String DEFAULTVALUE_SUB_NAME = "Default value: ";
  private static final String SYNONYMS_SUB_NAME     = "Synonyms: ";
  private static final String SPECIFIEDDEFAULTVALUE_SUB_NAME = "Default (if the parameter is specified without value): ";
  private static final String UNSPECIFIEDDEFAULTVALUE_VALUE_SUB_NAME = "Default (if the parameter is not specified): ";

  private static final String VALUE_FIELD = "value";
  private static final String TRUE_TOKEN  = "true";
  private static final String FALSE_TOKEN = "false";
  
  
  private static Help help(Block[] blocks) {
    return new Help().setBlocks(blocks);
  }

  private static Block block(String heading, Row... rows) {
    return new Block().setHeading(heading).setRows(rows);
  }

  private static Row row(String... info) {
    return new Row().setInfo(info);
  }

  @Deprecated
  public static Help getHelp(CommandTarget commandTarget) {
    List<Block> blocks = new ArrayList<Block>();
    // First we will have the block for NAME of the command
    blocks.add(block(NAME_NAME, row(commandTarget.getCommandName())));
    // Now add synonyms if any
    if (commandTarget.getSynonyms() != null) {
      blocks.add(block(SYNONYMS_NAME, row(commandTarget.getSynonyms())));
    }
    
    
    
    // Now comes the turn to display synopsis if any
    if (commandTarget.getCommandHelp() != null
        && !commandTarget.getCommandHelp().equals("")) {
      blocks.add(block(SYNOPSIS_NAME, row(commandTarget.getCommandHelp())));
    }
    // Now display the syntax for the command
    StringBuffer buffer = new StringBuffer();
    buffer.append(commandTarget.getCommandName());
    // Create a list which will store optional arguments
    List<Argument> optionalArguments = new ArrayList<Argument>();
    for (Argument argument : commandTarget.getOptionParser().getArguments()) {
      if (argument.isRequired()) {
        buffer.append(" " + argument.getArgumentName());
      } else {
        optionalArguments.add(argument);
      }
    }
    for (Argument argument : optionalArguments) {
      buffer.append(" " + "[" + argument.getArgumentName() + "]");
    }
    // Create a list which will store optional options
    List<Option> optionalOptions = new ArrayList<Option>();
    for (Option option : commandTarget.getOptionParser().getOptions()) {
      if (option.isRequired()) {
        buffer.append(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
            + option.getLongOption());
//      String temp = SyntaxConstants.OPTION_VALUE_SPECIFIER + VALUE_TOKEN + "("
//      + SyntaxConstants.OPTION_VALUE_SEPARATOR + VALUE_TOKEN + ")*";
        String temp = buildOptionHelpText(option);
//  
        if (option.getSpecifiedDefaultValue() != null
            && !option.getSpecifiedDefaultValue().equals("")) {
          buffer.append("(");
          buffer.append(temp);
          buffer.append(")?");
        } else {
          buffer.append(temp);
        }
      } else {
        optionalOptions.add(option);
      }
    }
    for (Option option : optionalOptions) {
      buffer.append(" " + "[" + SyntaxConstants.LONG_OPTION_SPECIFIER
          + option.getLongOption());
//    String temp = SyntaxConstants.OPTION_VALUE_SPECIFIER + VALUE_TOKEN + "("
//    + SyntaxConstants.OPTION_VALUE_SEPARATOR + VALUE_TOKEN + ")*";
      String temp = buildOptionHelpText(option);
//
      if (option.getSpecifiedDefaultValue() != null
          && !option.getSpecifiedDefaultValue().equals("")) {
        buffer.append("(");
        buffer.append(temp);
        buffer.append(")?");
      } else {
        buffer.append(temp);
      }
      buffer.append("]");
    }
    blocks.add(block(SYNTAX_NAME, row(buffer.toString())));
    // Detailed description of Arguments
    if (commandTarget.getOptionParser().getArguments().size() > 0) {
      List<Row> rows = new ArrayList<Row>();
      for (Argument argument : commandTarget.getOptionParser().getArguments()) {
        rows.add(row(argument.getArgumentName()
            + ((argument.getHelp() != null && !argument.getHelp().equals("")) ? ":"
                + argument.getHelp()
                : "")));
      }
      Row[] rowsArray = new Row[rows.size()];
      blocks.add(block(ARGUMENTS_NAME, rows.toArray(rowsArray)));
    }

    // Detailed description of Options
    if (commandTarget.getOptionParser().getOptions().size() > 0) {
      List<Row> rows = new ArrayList<Row>();
      for (Option option : commandTarget.getOptionParser().getOptions()) {
        rows.add(row(option.getLongOption()
            + ((option.getHelp() != null && !option.getHelp().equals("")) ? ":"
                + option.getHelp() : "")));
      }
      Row[] rowsArray = new Row[rows.size()];
      blocks.add(block(OPTIONS_NAME, rows.toArray(rowsArray)));
    }
    Block[] blocksArray = new Block[blocks.size()];
    for(int i=0;i<blocks.size();i++){
      blocksArray[i] = blocks.get(i);
    }
    return help(blocksArray);
  }
  
  /**
   * Builds help for the specified command.
   * 
   * @param commandTarget
   *          command specific target to use to generate help
   * @param withinShell
   *          if <code>true</code> includes availabilty & doesn't include
   *          application name
   * @return built NewHelp object for the given command target
   */
  public static NewHelp getNewHelp(CommandTarget commandTarget, boolean withinShell) {
    DataNode root = new DataNode(null, new ArrayList<DataNode>());
    // First we will have the block for NAME of the command
    DataNode name = new DataNode(NAME_NAME, new ArrayList<DataNode>());
    name.addChild(new DataNode(commandTarget.getCommandName(), null));
    root.addChild(name);
    if (withinShell) {// include availabilty info
      DataNode availability = new DataNode(IS_AVAILABLE_NAME, new ArrayList<DataNode>());
      boolean isAvailable = false;
      try {
        isAvailable = commandTarget.isAvailable();
      } catch (Exception e) {
        isAvailable = false;
      }
      availability.addChild(new DataNode(String.valueOf(isAvailable), null));
      root.addChild(availability);
    }
    // Now add synonyms if any
    if (commandTarget.getSynonyms() != null) {
      DataNode synonyms = new DataNode(SYNONYMS_NAME, new ArrayList<DataNode>());
      for (String string : commandTarget.getSynonyms()) {
        synonyms.addChild(new DataNode(string, null));
      }
      root.addChild(synonyms);
    }
    
    
    // Now comes the turn to display synopsis if any
    if (commandTarget.getCommandHelp() != null
        && !commandTarget.getCommandHelp().equals("")) {
      DataNode synopsis = new DataNode(SYNOPSIS_NAME, new ArrayList<DataNode>());
      synopsis.addChild(new DataNode(commandTarget.getCommandHelp(), null));
      root.addChild(synopsis);
    }
    
    
    // Now display the syntax for the command
    StringBuffer buffer = new StringBuffer();
    if (withinShell) {
      buffer.append(commandTarget.getCommandName());
    } else { // add app name in the syntax
      buffer.append(EXE_PREFIX_FOR_EXTERNAL_HELP).append(commandTarget.getCommandName());
    }    
    appendArguments(buffer,commandTarget);
    appendOptions(buffer,commandTarget);
    DataNode syntax = new DataNode(SYNTAX_NAME, new ArrayList<DataNode>());
    syntax.addChild(new DataNode(buffer.toString(), null));
    root.addChild(syntax);
    
    
    // Detailed description of Arguments
    if (commandTarget.getOptionParser().getArguments().size() > 0) {
      DataNode arguments = new DataNode(ARGUMENTS_NAME, new ArrayList<DataNode>());
      for (Argument argument : commandTarget.getOptionParser().getArguments()) {
        DataNode argumentNode = new DataNode(argument.getArgumentName(),
            new ArrayList<DataNode>());
        argumentNode
            .addChild(new DataNode(((argument.getHelp() != null && !argument
                .getHelp().equals("")) ? argument.getHelp() : ""), null));
        argumentNode.addChild(new DataNode(REQUIRED_SUB_NAME+((argument.isRequired()) ? TRUE_TOKEN
            : FALSE_TOKEN), null));
        if (argument.getUnspecifiedDefaultValue() != null) {
          argumentNode.addChild(new DataNode(DEFAULTVALUE_SUB_NAME
              + argument.getUnspecifiedDefaultValue(), null));
        }
        arguments.addChild(argumentNode);
      }
      root.addChild(arguments);
    }

    
    try {
      CommandModes modes = CommandModes.getInstance();
      Collection<CommandMode> comModes = modes.getCommandModes(commandTarget
          .getCommandName());
      DataNode modesDN = new DataNode(MODES, new ArrayList<DataNode>());
      if (comModes != null) {
        for (CommandMode cmd : comModes) {
          StringBuffer sb = new StringBuffer();
          List<Option> optionalOptions = new ArrayList<Option>();
          
          sb.append(commandTarget.getCommandName()).append(" ");
          if(!cmd.name.equals("default"))
            appendRequiredOption(sb, getOption(commandTarget, cmd.leadOption));
          
          for (String opt : cmd.options) {
            if (!opt.equals(cmd.leadOption)) {
              Option option = getOption(commandTarget, opt);
              if (option.isRequired()) {
                appendRequiredOption(sb, option);
              } else
                optionalOptions.add(option);              
            }            
          }
          
          for (Option optOpt : optionalOptions)
            appendOption(sb, optOpt);

          DataNode modeDN = new DataNode(cmd.text, new ArrayList<DataNode>());
          modeDN.addChild(new DataNode(sb.toString(), null));
          modesDN.addChild(modeDN);
        }
        root.addChild(modesDN);
      } else {
        //modesDN.addChild(new DataNode("No command modes found", null));
        //root.addChild(modesDN);
      }
      
    } catch (Exception e) {
    } finally{
      
    }
    
    // Detailed description of Options
    if (commandTarget.getOptionParser().getOptions().size() > 0) {
      DataNode options = new DataNode(OPTIONS_NAME, new ArrayList<DataNode>());
      for (Option option : commandTarget.getOptionParser().getOptions()) {
        DataNode optionNode = new DataNode(option.getLongOption(),
            new ArrayList<DataNode>());
        optionNode.addChild(new DataNode(((option.getHelp() != null && !option
            .getHelp().equals("")) ? option.getHelp() : ""), null));
        if (option.getSynonyms() != null && option.getSynonyms().size() > 0) {
          StringBuilder builder = new StringBuilder();
          for (String string : option.getSynonyms()) {
            if (builder.length() > 0) {
              builder.append(",");
            }
            builder.append(string);
          }
          optionNode.addChild(new DataNode(SYNONYMS_SUB_NAME + builder.toString(),
              null));
        }
        optionNode.addChild(new DataNode(REQUIRED_SUB_NAME+((option.isRequired()) ? TRUE_TOKEN
            : FALSE_TOKEN), null));
        if (option.getSpecifiedDefaultValue() != null 
        && !option.getSpecifiedDefaultValue().equals("")) {
          optionNode.addChild(new DataNode(SPECIFIEDDEFAULTVALUE_SUB_NAME
              + option.getSpecifiedDefaultValue(), null));
        }
        if (option.getUnspecifiedDefaultValue() != null 
        && !option.getUnspecifiedDefaultValue().equals("")) {
          optionNode.addChild(new DataNode(UNSPECIFIEDDEFAULTVALUE_VALUE_SUB_NAME
              + option.getUnspecifiedDefaultValue(), null));
        }
        options.addChild(optionNode);
      }
      root.addChild(options);
    }
    return new NewHelp(root);
  }
  
  private static Option getOption(CommandTarget commandTarget, String opt) {
    for(Option option : commandTarget.getOptionParser().getOptions()){
      if(option.getLongOption().equals(opt))
        return option;
    }
    return null;
  }

  private static void appendOptions(StringBuffer buffer,
      CommandTarget commandTarget) {
    List<Option> optionalOptions = new ArrayList<Option>();
    for (Option option : commandTarget.getOptionParser().getOptions()) {
      if (option.isRequired()) {
        appendRequiredOption(buffer,option);
      } else {
        optionalOptions.add(option);
      }
    }
    for (Option option : optionalOptions) {
      appendOption(buffer, option);
    }    
  }
  
  private static void appendRequiredOption(StringBuffer buffer, Option option){
    buffer.append(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
        + option.getLongOption());
    String temp = buildOptionHelpText(option);
    if (option.getSpecifiedDefaultValue() != null
        && !option.getSpecifiedDefaultValue().equals("")) {
      buffer.append("(").append(temp).append(")?");
    } else {
      buffer.append(temp);
    }
  }
  
  private static void appendOption(StringBuffer buffer, Option option){
    buffer.append(" " + "[" + SyntaxConstants.LONG_OPTION_SPECIFIER
        + option.getLongOption());
    String temp = buildOptionHelpText(option);
    if (option.getSpecifiedDefaultValue() != null
        && !option.getSpecifiedDefaultValue().equals("")) {
      buffer.append("(").append(temp).append(")?");
    } else {
      buffer.append(temp);
    }
    buffer.append("]");  
  }

  private static void appendArguments(StringBuffer buffer,
      CommandTarget commandTarget) {
    // Create a list which will store optional arguments
    List<Argument> optionalArguments = new ArrayList<Argument>();
    for (Argument argument : commandTarget.getOptionParser().getArguments()) {
      if (argument.isRequired()) {
        buffer.append(" " + argument.getArgumentName());
      } else {
        optionalArguments.add(argument);
      }
    }
    for (Argument argument : optionalArguments) {
      buffer.append(" " + "[" + argument.getArgumentName() + "]");
    }
  }

  public static String buildOptionHelpText(Option option) {
      String temp = SyntaxConstants.OPTION_VALUE_SPECIFIER + VALUE_FIELD;
      if( (option.getValueSeparator() != null &&
    	 !CliMetaData.ANNOTATION_NULL_VALUE.equals(option.getValueSeparator()) &&
        !option.getValueSeparator().equals("")) || isCollectionOrArrayType(option.getDataType())) {
    	  temp += "(" + option.getValueSeparator() + VALUE_FIELD + ")*";
      }
      return temp;
  }

  private static boolean isCollectionOrArrayType(Class<?> typeToCheck) {
    return typeToCheck != null && (typeToCheck.isArray() || Collection.class.isAssignableFrom(typeToCheck));
  }
}
