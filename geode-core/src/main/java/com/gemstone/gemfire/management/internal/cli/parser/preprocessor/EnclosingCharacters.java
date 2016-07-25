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
package com.gemstone.gemfire.management.internal.cli.parser.preprocessor;

/**
 * Used for Supporting enclosed input
 * 
 * @since GemFire 7.0
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
