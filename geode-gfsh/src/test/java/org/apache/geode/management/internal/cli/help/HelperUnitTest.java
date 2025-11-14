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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.management.internal.cli.commands.DescribeOfflineDiskStoreCommand;


public class HelperUnitTest {
  // SPRING SHELL 3.x: ShellOption.NULL constant value
  private static final String SHELL_OPTION_NULL = "__NULL__";

  private Helper helper;
  private ShellMethod shellMethod;
  private Method method;
  private ShellMethodAvailability availabilityIndicator;

  private Annotation[][] annotations;
  private ShellOption shellOption;

  private Class<?>[] parameterType;
  private HelpBlock optionBlock;

  @Before
  public void before() throws Exception {
    helper = new Helper();

    Method[] methods = DescribeOfflineDiskStoreCommand.class.getMethods();
    for (Method method1 : methods) {
      ShellMethod shellMethod1 = method1.getDeclaredAnnotation(ShellMethod.class);
      if (shellMethod1 != null) {
        helper.addCommand(shellMethod1, method1);
      }
    }

    // SPRING SHELL 3.x MIGRATION NOTES:
    // - ShellMethod.value() returns String (command description), not String[] of command names
    // - ShellMethod.key() returns String[] of command names/aliases
    // - ShellOption.defaultValue() == "__NULL__" means MANDATORY
    // - ShellOption.defaultValue() == other value means OPTIONAL with default
    shellMethod = mock(ShellMethod.class);
    when(shellMethod.value()).thenReturn("This is a test description"); // Description text
    when(shellMethod.key()).thenReturn(new String[] {"test", "test-synonym"}); // Command names

    // the tests will test with one parameter and one annotation at a time.
    // MANDATORY OPTION: defaultValue = "__NULL__"
    shellOption = mock(ShellOption.class);
    when(shellOption.value()).thenReturn(new String[] {"option"});
    when(shellOption.help()).thenReturn("help of option");
    when(shellOption.defaultValue()).thenReturn(SHELL_OPTION_NULL); // Mandatory!

    annotations = new Annotation[1][1];
    annotations[0][0] = shellOption;

    parameterType = new Class[1];
    parameterType[0] = String.class;

    availabilityIndicator = mock(ShellMethodAvailability.class);

  }

  @Test
  public void testGetLongHelp() {
    HelpBlock helpBlock = helper.getHelp(shellMethod, annotations, parameterType);
    String[] helpLines = helpBlock.toString().split(LINE_SEPARATOR);
    assertThat(helpLines.length).isEqualTo(14);
    assertThat(helpLines[0]).isEqualTo(Helper.NAME_NAME);
    assertThat(helpLines[2]).isEqualTo(Helper.IS_AVAILABLE_NAME);
    assertThat(helpLines[4]).isEqualTo(Helper.SYNONYMS_NAME);
    assertThat(helpLines[6]).isEqualTo(Helper.SYNOPSIS_NAME);
    assertThat(helpLines[8]).isEqualTo(Helper.SYNTAX_NAME);
    assertThat(helpLines[10]).isEqualTo(Helper.OPTIONS_NAME);
  }

  @Test
  public void testGetShortHelp() {
    HelpBlock helpBlock = helper.getHelp(shellMethod, null, null);
    String[] helpLines = helpBlock.toString().split(LINE_SEPARATOR);
    assertThat(helpLines.length).isEqualTo(2);
    assertThat(helpLines[0]).isEqualTo("test (Available)");
    assertThat(helpLines[1]).isEqualTo("This is a test description");
  }

  @Test
  public void testGetSyntaxStringWithMandatory() {
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test --option=value");
    optionBlock = helper.getOptionDetail(shellOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithOutMandatory() {
    // SPRING SHELL 3.x: Non-mandatory means defaultValue != ShellOption.NULL
    // Setting to empty string "" means optional with no default value shown
    when(shellOption.defaultValue()).thenReturn("");
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test [--option=value]");
    optionBlock = helper.getOptionDetail(shellOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: false" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithSecondaryOptionNameIgnored() {
    when(shellOption.value()).thenReturn(new String[] {"option", "option2"});
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test --option=value");
    optionBlock = helper.getOptionDetail(shellOption);
    assertThat(optionBlock.toString())
        .isEqualTo("option" + LINE_SEPARATOR + "help of option" + LINE_SEPARATOR
            + "Synonyms: option2" + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithSecondaryOptionName() {
    // SPRING SHELL 3.x: Positional parameter (first element empty in value array)
    when(shellOption.value()).thenReturn(new String[] {"", "option2"});
    when(shellOption.defaultValue()).thenReturn(ShellOption.NULL); // Mandatory positional
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test option2");
    optionBlock = helper.getOptionDetail(shellOption);
    assertThat(optionBlock.toString()).isEqualTo("option2" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithOptionalSecondaryOptionName() {
    // SPRING SHELL 3.x: Optional positional parameter
    when(shellOption.value()).thenReturn(new String[] {"", "option2"});
    when(shellOption.defaultValue()).thenReturn(""); // Optional (empty default)
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test [option2]");
    optionBlock = helper.getOptionDetail(shellOption);
    assertThat(optionBlock.toString()).isEqualTo("option2" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: false" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithStringArray() {
    parameterType[0] = String[].class;
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test --option=value(,value)*");
    optionBlock = helper.getOptionDetail(shellOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithSpecifiedDefault() {
    // SPRING SHELL 3.x: defaultValue="true" means optional with default value
    // Helper wraps optional params in [] and shows "Required: false"
    when(shellOption.defaultValue()).thenReturn("true");
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test [--option(=value)?]"); // [] because optional

    optionBlock = helper.getOptionDetail(shellOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: false" + LINE_SEPARATOR // false because defaultValue !=
                                                              // "__NULL__"
        + "Default (if the parameter is specified without value): true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithDefaultAndStringArray() {
    // SPRING SHELL 3.x: defaultValue="value1,value2" means optional with default value
    // Helper wraps optional params in [] and shows "Required: false"
    parameterType[0] = String[].class;
    when(shellOption.defaultValue()).thenReturn("value1,value2");
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test [--option(=value)?(,value)*]"); // [] because optional

    optionBlock = helper.getOptionDetail(shellOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: false" + LINE_SEPARATOR // false because defaultValue !=
                                                              // "__NULL__"
        + "Default (if the parameter is specified without value): value1,value2" + LINE_SEPARATOR);
  }

  @Test
  public void testGetMiniHelpBothRequiredsMissing() {
    assertThat(toUnix(helper.getMiniHelp("describe offline-disk-store")))
        .isEqualTo("  --name=  is required\n" + "  --disk-dirs=  is required\n"
            + "Use \"help describe offline-disk-store\" (without the quotes) for detailed usage information.\n");
  }

  @Test
  public void testGetMiniHelpDiskDirsMissing() {
    assertThat(toUnix(helper.getMiniHelp("describe offline-disk-store --name=foo")))
        .isEqualTo("  --disk-dirs=  is required\n"
            + "Use \"help describe offline-disk-store\" (without the quotes) for detailed usage information.\n");
  }

  @Test
  public void testGetMiniHelpNameMissing() {
    assertThat(toUnix(helper.getMiniHelp("describe offline-disk-store --disk-dirs=bar")))
        .isEqualTo("  --name=  is required\n"
            + "Use \"help describe offline-disk-store\" (without the quotes) for detailed usage information.\n");
  }

  @Test
  public void testGetMiniHelpPartialCommand() {
    assertThat(toUnix(helper.getMiniHelp("describe offline-disk")))
        .isEqualTo("  --name=  is required\n" + "  --disk-dirs=  is required\n"
            + "Use \"help describe offline-disk-store\" (without the quotes) for detailed usage information.\n");
  }

  @Test
  public void testGetMiniHelpNothingMissing() {
    assertThat(helper.getMiniHelp("describe offline-disk-store --name=foo --disk-dirs=bar"))
        .isNull();
  }

  private static String toUnix(String multiLineText) {
    return multiLineText.replace(LINE_SEPARATOR, "\n");
  }
}
