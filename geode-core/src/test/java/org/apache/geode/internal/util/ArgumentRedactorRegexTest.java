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
package org.apache.geode.internal.util;

import static org.apache.geode.internal.util.ArgumentRedactorRegex.getPattern;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.geode.internal.util.ArgumentRedactorRegex.Group;

public class ArgumentRedactorRegexTest {

  @Test
  public void hasFourCaptureGroups() {
    assertThat(Group.values()).hasSize(4);
  }

  @Test
  public void matchesOptionWithHyphenD() {
    String input = "-Doption=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesOptionWithTwoHyphens() {
    String input = "--password=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesOptionWithHyphenD_prefixedByDoubleHyphenJ() {
    String input = "--J=-Doption=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesInputWithSpaceBeforeAssignment() {
    String input = "--option =argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesInputWithSpaceAfterAssignment() {
    String input = "--option= argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesInputWithSpacesAroundAssignment() {
    String input = "--option = argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void doesNotMatchInputWithMultiplePairs() {
    String input = "--option1=argument1 --option2=argument2";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void matchesOptionContainingHyphens() {
    String input = "--this-is-the-option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentContainingHyphens() {
    String input = "--option=this-is-the-argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void doesNotMatchUnquotedArgumentStartingWithHyphen() {
    String input = "--option=-argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void matchesArgumentStartingWithHyphenInQuotes() {
    String input = "--option=\"-argument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void doesNotMatchUnquotedArgumentStartingWithMultipleHyphens() {
    String input = "--option=---argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void matchesArgumentStartingWithMultipleHyphensInQuotes() {
    String input = "--option=\"---argument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentStartingWithHyphensAndSpaceInQuotes() {
    String input = "--option=\"-foo -bar\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesOptionContainingUnderlines() {
    String input = "--this_is_the_option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentContainingUnderlines() {
    String input = "--option=this_is_the_argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentStartingWithUnderline() {
    String input = "--option=_argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentStartingWithMultipleUnderlines() {
    String input = "--option=___argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesOptionContainingPeriods() {
    String input = "--this.is.the.option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentContainingPeriods() {
    String input = "--option=this.is.the.argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentStartingWithPeriod() {
    String input = "--option=.argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentStartingWithMultiplePeriods() {
    String input = "--option=...argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentContainingHyphen() {
    String input = "--option=foo-bar";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void matchesArgumentContainingMultipleHyphens() {
    String input = "--option=foo-bar-is-found";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void groupMatchesInput() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(0)).isEqualTo(input);
  }

  @Test
  public void groupCountEqualsGroupsLength() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(Group.values()).hasSize(matcher.groupCount());
  }

  @Test
  public void prefixGroupIndexCapturesPrefix() {
    String prefix = "--";
    String input = prefix + "option=argument";
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo(prefix);
  }

  @Test
  public void optionGroupIndexCapturesOption() {
    String option = "option";
    String input = "--" + option + "=argument";
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(Group.OPTION.getIndex())).isEqualTo(option);
  }

  @Test
  public void assignmentGroupIndexCapturesOption() {
    String assignment = "=";
    String input = "--option" + assignment + "argument";
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(Group.ASSIGNMENT.getIndex())).isEqualTo(assignment);
  }

  @Test
  public void argumentGroupIndexCapturesOption() {
    String argument = "argument";
    String input = "--option=" + argument;
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(Group.ARGUMENT.getIndex())).isEqualTo(argument);
  }

  @Test
  public void prefixGroupNameCapturesPrefix() {
    String prefix = "--";
    String input = prefix + "option=argument";
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(Group.PREFIX.getName())).isEqualTo(prefix);
  }

  @Test
  public void optionGroupNameCapturesOption() {
    String option = "option";
    String input = "--" + option + "=argument";
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(Group.OPTION.getName())).isEqualTo(option);
  }

  @Test
  public void assignmentGroupNameCapturesOption() {
    String assignment = "=";
    String input = "--option" + assignment + "argument";
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(Group.ASSIGNMENT.getName())).isEqualTo(assignment);
  }

  @Test
  public void argumentGroupNameCapturesOption() {
    String argument = "argument";
    String input = "--option=" + argument;
    Matcher matcher = getPattern().matcher(input);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group(Group.ARGUMENT.getName())).isEqualTo(argument);
  }

  @Test
  public void prefixRegexMatchesPrefix() {
    String prefix = "--";
    Matcher matcher = Pattern.compile(Group.PREFIX.getRegex()).matcher(prefix);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void prefixRegexGroupCapturesPrefix() {
    String prefix = "--";
    Matcher matcher = Pattern.compile(Group.PREFIX.getRegex()).matcher(prefix);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(prefix);
  }

  @Test
  public void optionRegexMatchesOption() {
    String option = "option";
    Matcher matcher = Pattern.compile(Group.OPTION.getRegex()).matcher(option);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void optionRegexGroupCapturesOption() {
    String option = "option";
    Matcher matcher = Pattern.compile(Group.OPTION.getRegex()).matcher(option);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(option);
  }

  @Test
  public void assignmentRegexMatchesAssignment() {
    String assignment = " = ";
    Matcher matcher = Pattern.compile(Group.ASSIGNMENT.getRegex()).matcher(assignment);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void assignmentRegexGroupCapturesAssignment() {
    String assignment = " = ";
    Matcher matcher = Pattern.compile(Group.ASSIGNMENT.getRegex()).matcher(assignment);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(assignment);
  }

  @Test
  public void argumentRegexMatchesArgument() {
    String argument = "argument";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void argumentRegexGroupCapturesArgument() {
    String argument = "argument";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void argumentRegexMatchesArgumentWithHyphen() {
    String argument = "-argument";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);

    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void argumentRegexGroupCapturesArgumentWithMultipleHyphens() {
    String argument = "---argument";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void argumentRegexGroupDoesNotMatchArgumentWithMultipleWords() {
    String argument = "foo bar oi vey";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void argumentRegexGroupDoesNotMatchArgumentWithMultipleWordsAndHyphens() {
    String argument = "--foo --bar --oi --vey";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void argumentRegexGroupCapturesArgumentWithMultipleWordsAndHyphens() {
    String argument = "\"--foo --bar --oi --vey\"";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void argumentRegexGroupCapturesArgumentContainingTrailingHyphen() {
    String argument = "seekrit-";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void argumentRegexGroupCapturesArgumentContainingTrailingQuote() {
    String argument = "seekrit\"";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void argumentRegexGroupCapturesArgumentContainingSymbols() {
    String argument = "'s#kr!\"t";
    Matcher matcher = Pattern.compile(Group.ARGUMENT.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo("'s#kr!\"t");
  }

  @Test
  public void findsMultiplePairs() {
    String input = "-DmyArg -Duser-password=foo --classpath=.";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
  }

  @Test
  public void findsMultiplePairsInScriptSyntax() {
    String input = "connect --password=test --user=test";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
  }
}
