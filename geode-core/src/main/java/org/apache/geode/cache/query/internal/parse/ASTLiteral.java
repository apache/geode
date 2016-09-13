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


package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
import java.util.*;
import java.sql.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class ASTLiteral extends GemFireAST {
  private static final long serialVersionUID = 8374021603235812835L;
  
  public ASTLiteral() { }
  
  public ASTLiteral(Token t) {
    super(t);
  }
  
  
  private Object computeValue() throws QueryInvalidException {
    switch (getType()) {
      case OQLLexerTokenTypes.StringLiteral:
        return getString(getText(), '\'');
      case OQLLexerTokenTypes.NUM_INT:
        return getInt(getText());
      case OQLLexerTokenTypes.NUM_DOUBLE:
        return getDouble(getText());
      case OQLLexerTokenTypes.NUM_FLOAT:
        return getFloat(getText());
      case OQLLexerTokenTypes.NUM_LONG:
        return getLong(getText());
      case OQLLexerTokenTypes.LITERAL_nil:
      case OQLLexerTokenTypes.LITERAL_null:
        return null;
      case OQLLexerTokenTypes.LITERAL_undefined:
        return QueryService.UNDEFINED;
      case OQLLexerTokenTypes.LITERAL_true:
        return Boolean.TRUE;
      case OQLLexerTokenTypes.LITERAL_false:
        return Boolean.FALSE;
      case OQLLexerTokenTypes.LITERAL_char:
        return getChar(getFirstChild().getText());
      case OQLLexerTokenTypes.LITERAL_date:
        return getDate(getFirstChild().getText());
      case OQLLexerTokenTypes.LITERAL_timestamp:
        return getTimestamp(getFirstChild().getText());
      case OQLLexerTokenTypes.LITERAL_time:
        return getTime(getFirstChild().getText());
      default:
        throw new Error("unknown literal type: " + getType());
    }
  }
  
  public Integer getInt(String s)  throws QueryInvalidException {
    try {
      return Integer.valueOf(s);
    } catch (NumberFormatException e) {
      throw new QueryInvalidException(LocalizedStrings.ASTLiteral_UNABLE_TO_PARSE_INTEGER_0.toLocalizedString(s), e);
    }
  }
  
  public Long getLong(String s)
  throws QueryInvalidException {
    char last = s.charAt(s.length() - 1);
    boolean strip = last == 'L' || last == 'l';
    
    if (strip)
      s = s.substring(0, s.length() - 1);
    try {
      return Long.valueOf(s);
    } catch (NumberFormatException e) {
      throw new QueryInvalidException(LocalizedStrings.ASTLiteral_UNABLE_TO_PARSE_FLOAT_0.toLocalizedString(s));
    }
  }
  
  public Float getFloat(String s)
  throws QueryInvalidException {
    char last = s.charAt(s.length() - 1);
    boolean strip = last == 'F' || last == 'f';
    
    if (strip)
      s = s.substring(0, s.length() - 1);
    try {
      return new Float(s);
    } catch (NumberFormatException e) {
      throw new QueryInvalidException(s);
    }
  }
  
  
  public Double getDouble(String s)
  throws QueryInvalidException {
    char last = s.charAt(s.length() - 1);
    boolean strip = last == 'D' || last == 'd';
    if (strip)
      s = s.substring(0, s.length() - 1);
    try {
      return Double.valueOf(s);
    } catch (NumberFormatException e) {
      throw new QueryInvalidException(LocalizedStrings.ASTLiteral_UNABLE_TO_PARSE_DOUBLE_0.toLocalizedString(s));
    }
  }
  
  
  private String getString(String s, char delim) {
    // replace embedded double delims  with delim
    StringBuffer buf = new StringBuffer(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == delim && i < (s.length() - 1) &&  s.charAt(i+1) == delim)
        i++;
      buf.append(c);
    }
    return buf.toString();
  }
  
  @Override
  public void compile(QCompiler compiler) throws QueryInvalidException {
    Object value = computeValue();
    compiler.pushLiteral(value);
  }
  
  
  
  private Character getChar(String s) {
    if (s.length() != 1)
      throw new QueryInvalidException(LocalizedStrings.ASTLiteral_ILLEGAL_FORMAT_FOR_CHAR_LITERAL_A_CHARACTER_LITERAL_MUST_HAVE_EXACTLY_ONE_CHARACTER.toLocalizedString());
    return new Character(s.charAt(0));
  }
  
  private java.sql.Date getDate(String s) {
    try {
      java.sql.Date date = java.sql.Date.valueOf(s);
      // passed valueOf, now verify args are really in range
      int firstDash = s.indexOf('-');
      int secondDash = s.indexOf('-', firstDash+1);
      int month = Integer.parseInt(s.substring(firstDash+1, secondDash));
      int day = Integer.parseInt(s.substring(secondDash+1));
      if (month < 1 || month > 12)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_MONTH_MUST_BE_112_IN_DATE_LITERAL.toLocalizedString());
      if (day < 1 || day > 31)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_DAY_MUST_BE_131_IN_DATE_LITERAL.toLocalizedString());
      return date;
    } catch (IllegalArgumentException e) {
      throw new QueryInvalidException(LocalizedStrings.ASTLiteral_ILLEGAL_FORMAT_FOR_DATE_LITERAL_0_EXPECTED_FORMAT_IS_YYYYMMDD.toLocalizedString(s));
    }
  }
  
  
  private java.sql.Time getTime(String s)      {
    try {
      Time time = java.sql.Time.valueOf(s);
      // passed valueOf, now verify args are really in range
      int firstColon = s.indexOf(':');
      int secondColon = s.indexOf(':', firstColon+1);
      int hour = Integer.parseInt(s.substring(0, firstColon));
      int minute = Integer.parseInt(s.substring(firstColon+1, secondColon));
      int second = Integer.parseInt(s.substring(secondColon+1));
      if (hour < 0 || hour > 23)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_HOUR_MUST_BE_023_IN_TIME_LITERAL.toLocalizedString());
      if (minute < 0 || minute > 59)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_MINUTE_MUST_BE_059_IN_TIME_LITERAL.toLocalizedString());
      if (second < 0 || second > 59)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_SECOND_MUST_BE_059_IN_TIME_LITERAL.toLocalizedString());
      return time;
    } catch (IllegalArgumentException e) {
      throw new QueryInvalidException(LocalizedStrings.ASTLiteral_ILLEGAL_FORMAT_FOR_TIME_LITERAL_0_EXPECTED_FORMAT_IS_HHMMSS.toLocalizedString(s));
    }
  }
  
  private java.sql.Timestamp getTimestamp(String s) {
    try {
      Timestamp timestamp = java.sql.Timestamp.valueOf(s);
      // passed valueOf, now make sure parameters are in range
      // first rule out negative numbers by counting the dashes
      int count = 0;
      int index = -1;
      while ((index = s.indexOf('-', index + 1)) >= 0)
        count++;
      
      if (count > 2)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_NEGATIVE_NUMBERS_NOT_ALLOWED_IN_TIMESTAMP_LITERAL.toLocalizedString());
      
      // now just get the numbers, ignoring delimiters
      StringTokenizer tokenizer = new StringTokenizer(s, ":-\t\n\r\f ");
      // skip year, anything is valid
      tokenizer.nextToken();
      int month = Integer.parseInt(tokenizer.nextToken());
      if (month < 1 || month > 12)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_MONTH_MUST_BE_112_IN_TIMESTAMP_LITERAL.toLocalizedString());
      int day = Integer.parseInt(tokenizer.nextToken());
      if (day < 1 || day > 31)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_DAY_MUST_BE_131_IN_TIMESTAMP_LITERAL.toLocalizedString());
      int hour = Integer.parseInt(tokenizer.nextToken());
      if (hour < 0 || hour > 23)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_HOUR_MUST_BE_023_IN_TIMESTAMP_LITERAL.toLocalizedString());
      int minute = Integer.parseInt(tokenizer.nextToken());
      if (minute < 0 || minute > 59)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_MINUTE_MUST_BE_059_IN_TIMESTAMP_LITERAL.toLocalizedString());
      String sec_s = tokenizer.nextToken();
      int period = sec_s.indexOf('.');
      int second;
      if (period >= 0)
        second = Integer.parseInt(sec_s.substring(0, period));
      else
        second = Integer.parseInt(sec_s);
      if (second < 0 || second > 59)
        throw new QueryInvalidException(LocalizedStrings.ASTLiteral_SECOND_MUST_BE_059_IN_TIMESTAMP_LITERAL.toLocalizedString());
      return timestamp;
    } catch (IllegalArgumentException e) {
      throw new QueryInvalidException(LocalizedStrings.ASTLiteral_ILLEGAL_FORMAT_FOR_TIMESTAMP_LITERAL_0_EXPECTED_FORMAT_IS_YYYYMMDD_HHMMSSFFFFFFFFF.toLocalizedString(s));
    }
  }
}
