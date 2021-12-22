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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category(OQLQueryTest.class)
@RunWith(GeodeParamsRunner.class)
public class QCompilerTest {
  private QueryExecutionContext context;

  @Before
  public void setUp() {
    QueryConfigurationService mockService = mock(QueryConfigurationService.class);
    when(mockService.getMethodAuthorizer()).thenReturn(mock(MethodInvocationAuthorizer.class));
    InternalCache mockCache = mock(InternalCache.class);
    when(mockCache.getService(QueryConfigurationService.class)).thenReturn(mockService);

    context = new QueryExecutionContext(null, mockCache);
  }

  @Test
  public void testStringConditioningForLike_1() {
    String s1 = "abc%";
    StringBuilder buffer = new StringBuilder(s1);
    CompiledLike cl = new CompiledLike(null, null);
    int wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(3);
    assertThat(buffer.toString()).isEqualTo(s1);

    s1 = "abc\\%abc";
    buffer = new StringBuilder(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(-1);
    assertThat(buffer.toString()).isEqualTo("abc%abc");

    s1 = "abc\\\\%abc";
    buffer = new StringBuilder(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(4);
    assertThat(buffer.toString()).isEqualTo("abc\\%abc");

    s1 = "abc\\\\\\%abc";
    buffer = new StringBuilder(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(-1);
    assertThat(buffer.toString()).isEqualTo("abc\\%abc");

    s1 = "%";
    buffer = new StringBuilder(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(0);
    assertThat(buffer.toString()).isEqualTo(s1);

    s1 = "%abc";
    buffer = new StringBuilder(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(0);
    assertThat(buffer.toString()).isEqualTo("%abc");

    s1 = "%%abc";
    buffer = new StringBuilder(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(0);
    assertThat(buffer.toString()).isEqualTo("%%abc");

    s1 = "%\\%abc";
    buffer = new StringBuilder(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(0);
    assertThat(buffer.toString()).isEqualTo("%\\%abc");

    s1 = "_abc";
    buffer = new StringBuilder(s1);

    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(0);
    assertThat(buffer.toString()).isEqualTo("_abc");

    s1 = "\\_abc";
    buffer = new StringBuilder(s1);

    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(-1);
    assertThat(buffer.toString()).isEqualTo("_abc");

    s1 = "ab\\%c%d";
    buffer = new StringBuilder(s1);

    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(4);
    assertThat(buffer.toString()).isEqualTo("ab%c%d");

    s1 = "ab\\__d";
    buffer = new StringBuilder(s1);

    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertThat(wildCardPosition).isEqualTo(3);
    assertThat(buffer.toString()).isEqualTo("ab__d");
  }

  @Test
  public void testSargableRange() {
    String pattern = "abc";
    CompiledLiteral literal = new CompiledLiteral(pattern);
    CompiledID cid = new CompiledID("val");
    CompiledLike cl = new CompiledLike(cid, literal);
    CompiledComparison[] cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(1);

    ArrayList cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo(pattern);
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_EQ);

    pattern = "ab%";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(2);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("ab");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    cv = (ArrayList) cc[1].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("ac");
    assertThat(cc[1].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_LT);

    pattern = "a%c";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(3);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("a");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    cv = (ArrayList) cc[1].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("b");
    assertThat(cc[1].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_LT);

    assertThat(cc[2]).isEqualTo(cl);

    pattern = "%bc";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(2);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    assertThat(cc[1]).isEqualTo(cl);

    pattern = "ab_";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(3);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("ab");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    cv = (ArrayList) cc[1].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("ac");
    assertThat(cc[1].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_LT);

    assertThat(cc[2]).isEqualTo(cl);

    pattern = "_bc";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(2);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    assertThat(cc[1]).isEqualTo(cl);

    pattern = "a_c";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(3);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("a");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    cv = (ArrayList) cc[1].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("b");
    assertThat(cc[1].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_LT);

    assertThat(cc[2]).isEqualTo(cl);

    pattern = "_b%";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(2);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    assertThat(cc[1]).isEqualTo(cl);

    pattern = "a\\%bc";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(1);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("a%bc");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_EQ);

    pattern = "ab%";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cl.negate();
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(2);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("ab");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    cv = (ArrayList) cc[1].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("ac");
    assertThat(cc[1].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_LT);

    pattern = "a%c";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cl.negate();
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(2);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    assertThat(cc[1]).isEqualTo(cl);

    pattern = "a_";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cl.negate();
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertThat(cc.length).isEqualTo(2);

    cv = (ArrayList) cc[0].getChildren();
    assertThat(cv.get(0)).isInstanceOf(CompiledID.class);
    assertThat(((CompiledID) cv.get(0)).getId()).isEqualTo("val");
    assertThat(cv.get(1)).isInstanceOf(CompiledLiteral.class);
    assertThat(((CompiledLiteral) cv.get(1))._obj).isEqualTo("");
    assertThat(cc[0].getOperator()).isEqualTo(OQLLexerTokenTypes.TOK_GE);

    assertThat(cc[1]).isEqualTo(cl);
  }

  /**
   * This test is no more valid. From 6.6 Like is enhanced to support special chars (% and _) at any
   * place in the string pattern. With this the Like predicate is not transformed to
   * compiled-junction with > and < operator.
   */
  @Ignore
  @Test
  public void testStringConditioningForLike_2() {
    CompiledValue var = new CompiledPath(new CompiledID("p"), "ID");
    String s1 = "abc%";
    CompiledLiteral literal = new CompiledLiteral(s1);
    QCompiler compiler = new QCompiler();
    CompiledValue result = compiler.createCompiledValueForLikePredicate(var, literal);
    validationHelperForCompiledJunction((CompiledJunction) result, "abc", "abd");

    s1 = "abc\\\\%";
    literal = new CompiledLiteral(s1);
    compiler = new QCompiler();
    result = compiler.createCompiledValueForLikePredicate(var, literal);
    validationHelperForCompiledJunction((CompiledJunction) result, "abc\\\\", "abc\\]");

    s1 = "abc" + new String(new char[] {(char) 255, '%'});
    literal = new CompiledLiteral(s1);
    compiler = new QCompiler();
    result = compiler.createCompiledValueForLikePredicate(var, literal);
    String lowerBoundKey = "abc" + (char) 255;
    validationHelperForCompiledJunction((CompiledJunction) result, lowerBoundKey, "abd");

    s1 = "abc"
        + new String(new char[] {(char) 255, (char) 255, (char) 255, (char) 255, (char) 255, '%'});
    literal = new CompiledLiteral(s1);
    compiler = new QCompiler();
    result = compiler.createCompiledValueForLikePredicate(var, literal);
    lowerBoundKey =
        "abc" + new String(new char[] {(char) 255, (char) 255, (char) 255, (char) 255, (char) 255});
    validationHelperForCompiledJunction((CompiledJunction) result, lowerBoundKey, "abd");

    s1 = "%";
    literal = new CompiledLiteral(s1);
    compiler = new QCompiler();
    result = compiler.createCompiledValueForLikePredicate(var, literal);
    assertThat(result).isInstanceOf(CompiledComparison.class);
    CompiledComparison cc = (CompiledComparison) result;
    assertThat(cc.reflectOnOperator((CompiledValue) cc.getChildren().get(1)))
        .isEqualTo(OQLLexerTokenTypes.TOK_GE);
    assertThat(((CompiledLiteral) cc.getChildren().get(1))._obj).isEqualTo("");
  }

  private void validationHelperForCompiledJunction(CompiledJunction result, String lowerBoundKey,
      String upperBoundKey) {
    assertThat(result.getOperator()).isEqualTo(OQLLexerTokenTypes.LITERAL_and);
    List list = result.getChildren();
    CompiledComparison lowerBound = (CompiledComparison) list.get(0);
    CompiledComparison upperBound = (CompiledComparison) list.get(1);
    assertThat(lowerBound.reflectOnOperator((CompiledValue) lowerBound.getChildren().get(1)))
        .isEqualTo(OQLLexerTokenTypes.TOK_GE);
    assertThat(upperBound.reflectOnOperator((CompiledValue) upperBound.getChildren().get(1)))
        .isEqualTo(OQLLexerTokenTypes.TOK_LT);
    assertThat(((CompiledLiteral) lowerBound.getChildren().get(1))._obj).isEqualTo(lowerBoundKey);
    assertThat(((CompiledLiteral) upperBound.getChildren().get(1))._obj).isEqualTo(upperBoundKey);
  }

  @Test
  public void testLowestString() {
    SortedMap<String, String> map = new TreeMap<>();
    map.put("ab", "value");
    map.put("z", "value");
    map.put("v", "value");
    SortedMap returnMap = map.tailMap(CompiledLike.LOWEST_STRING);
    assertThat(returnMap.size()).isEqualTo(3);
  }

  @Test
  public void testBoundaryChar() {
    String s1 = "!";
    String s2 = "9";
    String s3 = "A";
    String s4 = "[";
    String s5 = "a";
    String s6 = "{";
    String s7 = String.valueOf((char) 255);
    assertThat(s2.compareTo(s1) > 0).isTrue();
    assertThat(s3.compareTo(s2) > 0).isTrue();
    assertThat(s4.compareTo(s3) > 0).isTrue();
    assertThat(s5.compareTo(s4) > 0).isTrue();
    assertThat(s6.compareTo(s5) > 0).isTrue();
    assertThat(s7.compareTo(s6) > 0).isTrue();
  }

  @Test
  @Parameters({"MIN", "MAX", "AVG", "SUM", "COUNT"})
  public void parsingShouldSucceedWhenAggregatesAreUsedAsPartOfTheWhereClauseWithinAnInnerSelectQuery(
      String function) {
    String[] queries = new String[] {
        "SELECT * FROM " + SEPARATOR + "region WHERE id IN (SELECT " + function + "(id) FROM "
            + SEPARATOR + "region)",
        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE id IN (SELECT " + function
            + "(id) FROM " + SEPARATOR + "region) GROUP BY status",

        "SELECT * FROM " + SEPARATOR + "region WHERE id = ELEMENT(SELECT " + function + "(id) FROM "
            + SEPARATOR + "region)",
        "SELECT * FROM " + SEPARATOR + "region WHERE id < ELEMENT(SELECT " + function + "(id) FROM "
            + SEPARATOR + "region)",
        "SELECT * FROM " + SEPARATOR + "region WHERE id > ELEMENT(SELECT " + function + "(id) FROM "
            + SEPARATOR + "region)",
        "SELECT * FROM " + SEPARATOR + "region WHERE id <> ELEMENT(SELECT " + function
            + "(id) FROM " + SEPARATOR + "region)",

        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE id = ELEMENT(SELECT "
            + function
            + "(id) FROM " + SEPARATOR + "region) GROUP BY status",
        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE id < ELEMENT(SELECT "
            + function
            + "(id) FROM " + SEPARATOR + "region) GROUP BY status",
        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE id > ELEMENT(SELECT "
            + function
            + "(id) FROM " + SEPARATOR + "region) GROUP BY status",
        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE id <> ELEMENT(SELECT "
            + function
            + "(id) FROM " + SEPARATOR + "region) GROUP BY status",
    };

    for (String queryString : queries) {
      QCompiler compiler = new QCompiler();
      assertThatCode(() -> compiler.compileQuery(queryString))
          .as(String.format("Query parsing failed for %s but should have succeeded.", queryString))
          .doesNotThrowAnyException();
    }
  }

  @Test
  @Parameters({"MIN", "MAX", "AVG", "SUM", "COUNT"})
  public void parsingShouldThrowExceptionWhenAggregatesAreUsedAsPartOfTheWhereClauseWithoutAnInnerSelectQuery(
      String function) {
    String[] queries = new String[] {
        "SELECT * FROM " + SEPARATOR + "region WHERE " + function + "(id) > 0",
        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE " + function
            + "(id) > 0 GROUP BY status",
        "SELECT * FROM " + SEPARATOR + "region WHERE " + function + "(id) > 0 OR id < 0",
        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE " + function
            + "(id) > 0 OR id < 0 GROUP BY status",
        "SELECT * FROM " + SEPARATOR + "region WHERE id > 0 AND " + function + "(id) > 0",
        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE id > 0 AND " + function
            + "(id) > 0 GROUP BY status",
        "SELECT * FROM " + SEPARATOR + "region WHERE " + function + "(id) < ELEMENT(SELECT "
            + function
            + "(id) FROM " + SEPARATOR + "region)",
        "SELECT COUNT(id), status FROM " + SEPARATOR + "region WHERE " + function
            + "(id) < ELEMENT(SELECT "
            + function + "(id) FROM " + SEPARATOR + "region) GROUP BY status",
        "SELECT * FROM " + SEPARATOR + "region WHERE id > ELEMENT(SELECT id FROM " + SEPARATOR
            + "region WHERE " + function
            + "(id) > 0)",
        "SELECT id, status FROM " + SEPARATOR + "region WHERE id > ELEMENT(SELECT id FROM "
            + SEPARATOR + "region WHERE " + function
            + "(id) > 0)"
    };

    for (String queryString : queries) {
      QCompiler compiler = new QCompiler();
      assertThatThrownBy(() -> compiler.compileQuery(queryString))
          .as(String.format("Query parsing succeeded for %s but should have failed.", queryString))
          .isInstanceOf(QueryInvalidException.class)
          .hasMessageContaining("Aggregate functions can not be used as part of the WHERE clause");
    }
  }
}
