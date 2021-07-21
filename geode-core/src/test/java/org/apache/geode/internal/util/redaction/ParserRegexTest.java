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
package org.apache.geode.internal.util.redaction;

import static org.apache.geode.internal.util.redaction.ParserRegex.getPattern;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.geode.internal.util.redaction.ParserRegex.Group;

public class ParserRegexTest {

  @Test
  public void capturesOption() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenPrefixIsHyphenD() {
    String input = "-Doption=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenPrefixIsHyphensJD() {
    String input = "--J=-Doption=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignIsSpace() {
    String input = "--option argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignIsSpaceEquals() {
    String input = "--option =argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" =");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignIsEqualsSpace() {
    String input = "--option= argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("= ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignIsSpaceEqualsSpace() {
    String input = "--option = argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" = ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenKeyContainsHyphens() {
    String input = "--this-is-the-option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("this-is-the-option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenValueContainsHyphens() {
    String input = "--option=this-is-the-argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("this-is-the-argument");
  }

  @Test
  public void capturesOptionWhenValueIsQuoted() {
    String input = "--option=\"argument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"argument\"");
  }

  @Test
  public void capturesOptionWhenValueIsQuotedAndAssignIsSpace() {
    String input = "--option \"argument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"argument\"");
  }

  @Test
  public void capturesOptionWhenAssignContainsTwoSpaces() {
    String input = "--option  argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("  ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignContainsManySpaces() {
    String input = "--option   argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("   ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithHyphen() {
    String input = "--option=-argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("-argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithHyphenAndAssignIsSpace() {
    String input = "--option -argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("-argument");
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithHyphen() {
    String input = "--option=\"-argument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"-argument\"");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithTwoHyphens() {
    String input = "--option=--argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("--argument");
  }

  @Test
  public void capturesFlag() {
    String input = "--flag";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesFlagWhenPrefixIsHyphenD() {
    String input = "-Dflag";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesFlagWhenPrefixIsHyphensJD() {
    String input = "--J=-Dflag";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesTwoFlags() {
    String input = "--option --argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("argument");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesTwoFlagsWhenPrefixIsHyphenD() {
    String input = "-Doption -Dargument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("argument");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesTwoFlagsWhenPrefixIsHyphensJD() {
    String input = "--J=-Doption --J=-Dargument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("argument");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithTwoHyphens() {
    String input = "--option=\"--argument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"--argument\"");
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithHyphenD() {
    String input = "--option=\"-Dargument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"-Dargument\"");
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithHyphensJD() {
    String input = "--option=\"--J=-Dargument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"--J=-Dargument\"");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyHyphens() {
    String input = "--option=---argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("---argument");
  }

  @Test
  public void capturesTwoFlagsWhenValueBeginsWithManyHyphensAndAssignIsSpace() {
    String input = "--option ---argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("-argument");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithManyHyphens() {
    String input = "--option=\"---argument\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"---argument\"");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithHyphenD() {
    String input = "--option=-Dargument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("-Dargument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithHyphensJD() {
    String input = "--option=--J=-Dargument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("--J=-Dargument");
  }

  @Test
  public void capturesOptionWithPartialValueWhenValueContainsSpace() {
    String input = "--option=foo bar";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("foo");

    assertThat(matcher.find()).isFalse();
  }

  @Test
  public void capturesOptionWithPartialValueWhenValueContainsSpaceAndSingleHyphens() {
    String input = "--option=-foo -bar";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("-foo");

    assertThat(matcher.find()).isFalse();
  }

  @Test
  public void capturesOptionWhenQuotedValueContainsSpaceAndSingleHyphens() {
    String input = "--option=\"-foo -bar\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"-foo -bar\"");
  }

  @Test
  public void capturesOptionAndFlagWhenValueContainsSpaceAndDoubleHyphens() {
    String input = "--option=--foo --bar";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("--foo");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("bar");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesOptionWhenQuotedValueContainsSpaceAndDoubleHyphens() {
    String input = "--option=\"--foo --bar\"";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"--foo --bar\"");
  }

  @Test
  public void capturesOptionWhenKeyContainsUnderscores() {
    String input = "--this_is_the_option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("this_is_the_option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenValueContainsUnderscores() {
    String input = "--option=this_is_the_argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("this_is_the_argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithUnderscore() {
    String input = "--option=_argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("_argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithUnderscoreAndAssignIsSpace() {
    String input = "--option _argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("_argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyUnderscores() {
    String input = "--option=___argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("___argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyUnderscoresAndAssignIsSpace() {
    String input = "--option ___argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("___argument");
  }

  @Test
  public void capturesOptionWhenKeyContainsPeriods() {
    String input = "--this.is.the.option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("this.is.the.option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenValueContainsPeriods() {
    String input = "--option=this.is.the.argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("this.is.the.argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithPeriod() {
    String input = "--option=.argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo(".argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithPeriodAndAssignIsSpace() {
    String input = "--option .argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo(".argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyPeriods() {
    String input = "--option=...argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("...argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyPeriodsAndAssignIsSpace() {
    String input = "--option ...argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("...argument");
  }

  @Test
  public void doesNotMatchWhenPrefixIsSingleHyphen() {
    String input = "-option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void doesNotMatchWhenPrefixIsMissing() {
    String input = "option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void groupZeroCapturesFullInputWhenValid() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(0)).isEqualTo(input);
  }

  @Test
  public void groupValuesHasSizeEqualToGroupCount() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(Group.values()).hasSize(matcher.groupCount());
  }

  @Test
  public void groupPrefixCapturesHyphens() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
  }

  @Test
  public void groupPrefixCapturesHyphenD() {
    String input = "-Doption=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
  }

  @Test
  public void groupPrefixCapturesHyphensJD() {
    String input = "--J=-Doption=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
  }

  @Test
  public void groupPrefixCapturesIsolatedHyphens() {
    String prefix = "--";
    Matcher matcher = Pattern.compile(Group.PREFIX.getRegex()).matcher(prefix);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group()).isEqualTo(prefix);
  }

  @Test
  public void groupPrefixCapturesIsolatedHyphenD() {
    String prefix = "-D";
    Matcher matcher = Pattern.compile(Group.PREFIX.getRegex()).matcher(prefix);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group()).isEqualTo(prefix);
  }

  @Test
  public void groupPrefixCapturesIsolatedHyphensJD() {
    String prefix = "--J=-D";
    Matcher matcher = Pattern.compile(Group.PREFIX.getRegex()).matcher(prefix);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group()).isEqualTo(prefix);
  }

  @Test
  public void groupKeyCapturesKey() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
  }

  @Test
  public void groupKeyCapturesIsolatedKey() {
    String option = "option";
    Matcher matcher = Pattern.compile(Group.KEY.getRegex()).matcher(option);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group()).isEqualTo(option);
  }

  @Test
  public void groupAssignCapturesEquals() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
  }

  @Test
  public void groupAssignCapturesSpace() {
    String input = "--option argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
  }

  @Test
  public void groupAssignCapturesIsolatedEqualsSurroundedBySpaces() {
    String assignment = " = ";
    Matcher matcher = Pattern.compile(Group.ASSIGN.getRegex()).matcher(assignment);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(assignment);
  }

  @Test
  public void groupValueCapturesValue() {
    String input = "--option=argument";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void groupValueCapturesIsolatedValue() {
    String argument = "argument";
    Matcher matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueCapturesIsolatedValueStartingWithManyHyphens() {
    String argument = "---argument";
    Matcher matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueDoesNotMatchIsolatedValueContainingSpaces() {
    String argument = "foo bar oi vey";
    Matcher matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void groupValueDoesNotMatchIsolatedValueContainingDoubleHyphensAndSpaces() {
    String argument = "--foo --bar --oi --vey";
    Matcher matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void groupValueCapturesIsolatedQuotedValueContainingDoubleHyphensAndSpaces() {
    String argument = "\"--foo --bar --oi --vey\"";
    Matcher matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueCapturesIsolatedValueEndingWithHyphen() {
    String argument = "value-";
    Matcher matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueCapturesIsolatedValueEndingWithQuote() {
    String argument = "value\"";
    Matcher matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueCapturesIsolatedValueContainingSymbols() {
    String argument = "'v@lu!\"t";
    Matcher matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void capturesMultipleOptions() {
    String input = "--option1=argument1 --option2=argument2";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option1");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument1");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option2");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument2");
  }

  @Test
  public void capturesFlagAfterMultipleOptions() {
    String input = "--option1=argument1 --option2=argument2 --flag";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option1");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument1");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option2");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument2");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesMultipleOptionsAfterFlag() {
    String input = "--flag --option1=foo --option2=.";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option1");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("foo");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option2");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo(".");
  }

  @Test
  public void capturesMultipleOptionsSurroundingFlag() {
    String input = "--option1=foo --flag --option2=.";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option1");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("foo");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option2");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo(".");
  }

  @Test
  public void capturesMultipleOptionsAfterCommand() {
    String input = "command --key=value --foo=bar";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("key");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("value");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("foo");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("bar");
  }

  @Test
  public void capturesMultipleOptionsSurroundingFlagAfterCommand() {
    String input = "command --key=value --flag --foo=bar";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("key");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("value");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("foo");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("bar");
  }

  @Test
  public void capturesMultipleOptionsWithVariousPrefixes() {
    String input = "--key=value -Dflag --J=-Dfoo=bar";
    Matcher matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("key");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("value");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("foo");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("bar");
  }
}
