/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser.preprocessor;

/**
 * Used for Supporting enclosed input
 * 
 * @author Nikhil Jadhav
 * @since 7.0
 *
 */
public final class EnclosingCharacters {
  public static final Character DOUBLE_QUOTATION = '\"';
  public static final Character SINGLE_QUOTATION = '\'';
  public static final Character OPENING_CURLY_BRACE = '{';
  public static final Character CLOSING_CURLY_BRACE = '}';
  public static final Character OPENING_SQUARE_BRACKET = '[';
  public static final Character CLOSING_SQUARE_BRACKET = ']';
  public static final Character OPENING_CIRCULAR_BRACKET = '(';
  public static final Character CLOSING_CIRCULAR_BRACKET = ')';
}
