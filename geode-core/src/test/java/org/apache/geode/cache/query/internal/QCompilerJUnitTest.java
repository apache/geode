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
/*
 * Created on Oct 13, 2005
 * 
 * 
 */
package com.gemstone.gemfire.cache.query.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * 
 */
@Category(IntegrationTest.class)
public class QCompilerJUnitTest {

  QueryExecutionContext context = new QueryExecutionContext(null, null);
  @Test
  public void testStringConditioningForLike_1() {
    String s1 = "abc%";
    StringBuffer buffer = new StringBuffer(s1);
    CompiledLike cl = new CompiledLike(null, null);
    int wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(3,wildCardPosition);
    assertEquals(s1, buffer.toString());
    
    s1 = "abc\\%abc";
    buffer = new StringBuffer(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(-1,wildCardPosition);
    assertEquals(buffer.toString(), "abc%abc");
    
    s1 = "abc\\\\%abc";
    buffer = new StringBuffer(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(4,wildCardPosition);
    assertEquals(buffer.toString(), "abc\\%abc");

    s1 = "abc\\\\\\%abc";
    buffer = new StringBuffer(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(-1,wildCardPosition);
    assertEquals(buffer.toString(), "abc\\%abc");

    s1 = "%";
    buffer = new StringBuffer(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(0,wildCardPosition);
    assertEquals(buffer.toString(), s1);

    s1 = "%abc";
    buffer = new StringBuffer(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(0,wildCardPosition);
    assertEquals(buffer.toString(), "%abc");

    s1 = "%%abc";
    buffer = new StringBuffer(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(0,wildCardPosition);
    assertEquals(buffer.toString(), "%%abc");

    s1 = "%\\%abc";
    buffer = new StringBuffer(s1);
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(0,wildCardPosition);
    assertEquals(buffer.toString(), "%\\%abc");

    s1 = "_abc";
    buffer = new StringBuffer(s1);

    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(0,wildCardPosition);
    assertEquals(buffer.toString(), "_abc");
    
    s1 = "\\_abc";
    buffer = new StringBuffer(s1);
    
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(-1,wildCardPosition);
    assertEquals(buffer.toString(), "_abc");
    
    s1 = "ab\\%c%d";
    buffer = new StringBuffer(s1);
    
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(4,wildCardPosition);
    assertEquals(buffer.toString(), "ab%c%d");
    
    s1 = "ab\\__d";
    buffer = new StringBuffer(s1);
    
    wildCardPosition = cl.checkIfSargableAndRemoveEscapeChars(context, buffer);
    assertEquals(3,wildCardPosition);
    assertEquals(buffer.toString(), "ab__d");
  }
  
  @Test
  public void testSargableRange() throws Exception{
    String pattern = "abc";
    CompiledLiteral literal = new CompiledLiteral(pattern);
    CompiledID cid = new CompiledID("val");
    CompiledLike cl = new CompiledLike(cid, literal);
    CompiledComparison[] cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(1, cc.length);
   
    ArrayList cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals(pattern, ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_EQ,cc[0].getOperator());
    
    pattern = "ab%";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(2, cc.length);
    
    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("ab", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    cv = (ArrayList) cc[1].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("ac", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_LT,cc[1].getOperator());
     
    pattern = "a%c";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(3, cc.length);

    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("a", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    cv = (ArrayList) cc[1].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("b", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_LT,cc[1].getOperator());

    assertEquals(cc[2],cl);    
    
    pattern = "%bc";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(2, cc.length);
 
    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    assertEquals(cc[1],cl);
    
    pattern = "ab_";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(3, cc.length);
    
    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("ab", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    cv = (ArrayList) cc[1].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("ac", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_LT,cc[1].getOperator());

    assertEquals(cc[2],cl);
    
    pattern = "_bc";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(2, cc.length);

    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    assertEquals(cc[1],cl);
    
    pattern = "a_c";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(3, cc.length);
    
    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("a", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    cv = (ArrayList) cc[1].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("b", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_LT,cc[1].getOperator());

    assertEquals(cc[2],cl);    
    
    pattern = "_b%";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(2, cc.length);

    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    assertEquals(cc[1],cl);
    
    pattern = "a\\%bc";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(1, cc.length);

    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("a%bc", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_EQ,cc[0].getOperator());
    
    pattern = "ab%";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cl.negate();
    cc = cl.getRangeIfSargable(context,cid, pattern);
    assertEquals(2, cc.length);
    
    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("ab", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    cv = (ArrayList) cc[1].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("ac", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_LT,cc[1].getOperator());
    
    pattern = "a%c";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cl.negate();
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertEquals(2, cc.length);

    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    assertEquals(cc[1],cl);
    
    pattern = "a_";
    literal = new CompiledLiteral(pattern);
    cl = new CompiledLike(cid, literal);
    cl.negate();
    cc = cl.getRangeIfSargable(context, cid, pattern);
    assertEquals(2, cc.length);

    cv = (ArrayList) cc[0].getChildren();
    assertTrue(cv.get(0) instanceof CompiledID);     
    assertEquals("val", ((CompiledID)cv.get(0)).getId());
    assertTrue(cv.get(1) instanceof CompiledLiteral);     
    assertEquals("", ((CompiledLiteral)cv.get(1))._obj);
    assertEquals(OQLLexerTokenTypes.TOK_GE,cc[0].getOperator());
    
    assertEquals(cc[1],cl);
  }

  /**
   * This test is no more valid. From 6.6 Like is enhanced to support 
   * special chars (% and _) at any place in the string pattern. With 
   * this the Like predicate is not transformed to compiled-junction
   * with > and < operator.
   */
  @Ignore
  @Test
  public void testStringConditioningForLike_2() {
    CompiledValue var = new CompiledPath(new CompiledID("p"), "ID");
    String s1 = "abc%";
    CompiledLiteral literal = new CompiledLiteral(s1);
    QCompiler compiler = new QCompiler();
    CompiledValue result = compiler.createCompiledValueForLikePredicate(var,
        literal);
    validationHelperForCompiledJunction((CompiledJunction)result, "abc", "abd");

    s1 = "abc\\\\%";
    literal = new CompiledLiteral(s1);
    compiler = new QCompiler();
    result = compiler.createCompiledValueForLikePredicate(var, literal);
    validationHelperForCompiledJunction((CompiledJunction)result, "abc\\\\",
        "abc\\]");

    s1 = "abc" + new String(new char[] { (char)255, '%' });
    literal = new CompiledLiteral(s1);
    compiler = new QCompiler();
    result = compiler.createCompiledValueForLikePredicate(var, literal);
    String lowerBoundKey = "abc" + new String(new char[] { (char)255 });
    validationHelperForCompiledJunction((CompiledJunction)result,
        lowerBoundKey, "abd");

    s1 = "abc"
        + new String(new char[] { (char)255, (char)255, (char)255, (char)255,
            (char)255, '%' });
    literal = new CompiledLiteral(s1);
    compiler = new QCompiler();
    result = compiler.createCompiledValueForLikePredicate(var, literal);
    lowerBoundKey = "abc"
        + new String(new char[] { (char)255, (char)255, (char)255, (char)255,
            (char)255 });
    validationHelperForCompiledJunction((CompiledJunction)result,
        lowerBoundKey, "abd");

    s1 = "%";
    literal = new CompiledLiteral(s1);
    compiler = new QCompiler();
    result = compiler.createCompiledValueForLikePredicate(var, literal);
    assertTrue(result instanceof CompiledComparison);
    CompiledComparison cc = (CompiledComparison)result;
    assertTrue(cc.reflectOnOperator((CompiledValue)cc.getChildren().get(1)) == OQLLexerTokenTypes.TOK_GE);
    assertTrue(((CompiledLiteral)cc.getChildren().get(1))._obj.equals(""));
  }

  private void validationHelperForCompiledJunction(CompiledJunction result,
      String lowerBoundKey, String upperBoundKey) {
    CompiledJunction cj = result;
    assertTrue(cj.getOperator() == OQLLexerTokenTypes.LITERAL_and);
    List list = cj.getChildren();
    CompiledComparison lowerBound = (CompiledComparison)list.get(0);
    CompiledComparison upperBound = (CompiledComparison)list.get(1);
    assertTrue(lowerBound.reflectOnOperator((CompiledValue)lowerBound
        .getChildren().get(1)) == OQLLexerTokenTypes.TOK_GE);
    assertTrue(upperBound.reflectOnOperator((CompiledValue)upperBound
        .getChildren().get(1)) == OQLLexerTokenTypes.TOK_LT);
    assertEquals(lowerBoundKey, ((CompiledLiteral)lowerBound.getChildren().get(
        1))._obj);
    assertEquals(upperBoundKey, ((CompiledLiteral)upperBound.getChildren().get(
        1))._obj);

  }

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testLowestString() {
    SortedMap map = new TreeMap();
    map.put("ab", "value");
    map.put("z", "value");
    map.put("v", "value");
    SortedMap returnMap = map.tailMap(CompiledLike.LOWEST_STRING);
    assertEquals(3, returnMap.size());
  }

  @Test
  public void testBoundaryChar() {
    String s1 = "!";
    String s2 = "9";
    String s3 = "A";
    String s4 = "[";
    String s5 = "a";
    String s6 = "{";
    String s7 = new String(new char[] { (char)255 });
    assertTrue(s2.compareTo(s1) > 0);
    assertTrue(s3.compareTo(s2) > 0);
    assertTrue(s4.compareTo(s3) > 0);
    assertTrue(s5.compareTo(s4) > 0);
    assertTrue(s6.compareTo(s5) > 0);
    assertTrue(s7.compareTo(s6) > 0);

  }

}
