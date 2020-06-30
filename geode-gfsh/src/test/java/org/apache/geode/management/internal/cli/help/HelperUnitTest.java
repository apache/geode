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
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.GeodeCommandMarker;
import org.apache.geode.management.internal.cli.commands.DescribeOfflineDiskStoreCommand;


public class HelperUnitTest {
  private Helper helper;
  private CliCommand cliCommand;
  private Method method;
  private CliAvailabilityIndicator availabilityIndicator;
  private GeodeCommandMarker commandMarker;

  private Annotation[][] annotations;
  private CliOption cliOption;

  private Class<?>[] parameterType;
  private HelpBlock optionBlock;

  @Before
  public void before() throws Exception {
    helper = new Helper();

    Method[] methods = DescribeOfflineDiskStoreCommand.class.getMethods();
    for (Method method1 : methods) {
      CliCommand cliCommand1 = method1.getDeclaredAnnotation(CliCommand.class);
      if (cliCommand1 != null) {
        helper.addCommand(cliCommand1, method1);
      }
    }

    cliCommand = mock(CliCommand.class);
    when(cliCommand.value()).thenReturn("test,test-synonym".split(","));
    when(cliCommand.help()).thenReturn("This is a test description");

    // the tests will test with one parameter and one annotation at a time.
    cliOption = mock(CliOption.class);
    when(cliOption.key()).thenReturn("option".split(","));
    when(cliOption.help()).thenReturn("help of option");
    when(cliOption.mandatory()).thenReturn(true);

    annotations = new Annotation[1][1];
    annotations[0][0] = cliOption;

    parameterType = new Class[1];
    parameterType[0] = String.class;

    availabilityIndicator = mock(CliAvailabilityIndicator.class);

  }

  @Test
  public void testGetLongHelp() {
    HelpBlock helpBlock = helper.getHelp(cliCommand, annotations, parameterType);
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
    HelpBlock helpBlock = helper.getHelp(cliCommand, null, null);
    String[] helpLines = helpBlock.toString().split(LINE_SEPARATOR);
    assertThat(helpLines.length).isEqualTo(2);
    assertThat(helpLines[0]).isEqualTo("test (Available)");
    assertThat(helpLines[1]).isEqualTo("This is a test description");
  }

  @Test
  public void testGetSyntaxStringWithMandatory() {
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test --option=value");
    optionBlock = helper.getOptionDetail(cliOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithOutMandatory() {
    when(cliOption.mandatory()).thenReturn(false);
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test [--option=value]");
    optionBlock = helper.getOptionDetail(cliOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: false" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithSecondaryOptionNameIgnored() {
    when(cliOption.key()).thenReturn("option,option2".split(","));
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test --option=value");
    optionBlock = helper.getOptionDetail(cliOption);
    assertThat(optionBlock.toString())
        .isEqualTo("option" + LINE_SEPARATOR + "help of option" + LINE_SEPARATOR
            + "Synonyms: option2" + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithSecondaryOptionName() {
    when(cliOption.key()).thenReturn(",option2".split(","));
    when(cliOption.mandatory()).thenReturn(true);
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test option2");
    optionBlock = helper.getOptionDetail(cliOption);
    assertThat(optionBlock.toString()).isEqualTo("option2" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithOptionalSecondaryOptionName() {
    when(cliOption.key()).thenReturn(",option2".split(","));
    when(cliOption.mandatory()).thenReturn(false);
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test [option2]");
    optionBlock = helper.getOptionDetail(cliOption);
    assertThat(optionBlock.toString()).isEqualTo("option2" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: false" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithStringArray() {
    parameterType[0] = String[].class;
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test --option=value(,value)*");
    optionBlock = helper.getOptionDetail(cliOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithSpecifiedDefault() {
    when(cliOption.specifiedDefaultValue()).thenReturn("true");
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test --option(=value)?");

    optionBlock = helper.getOptionDetail(cliOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR
        + "Default (if the parameter is specified without value): true" + LINE_SEPARATOR);
  }

  @Test
  public void testGetSyntaxStringWithDefaultAndStringArray() {
    parameterType[0] = String[].class;
    when(cliOption.specifiedDefaultValue()).thenReturn("value1,value2");
    String syntax = helper.getSyntaxString("test", annotations, parameterType);
    assertThat(syntax).isEqualTo("test --option(=value)?(,value)*");

    optionBlock = helper.getOptionDetail(cliOption);
    assertThat(optionBlock.toString()).isEqualTo("option" + LINE_SEPARATOR + "help of option"
        + LINE_SEPARATOR + "Required: true" + LINE_SEPARATOR
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
