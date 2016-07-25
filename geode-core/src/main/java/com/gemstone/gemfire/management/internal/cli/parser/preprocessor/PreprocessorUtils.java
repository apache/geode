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


import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;

/**
 * The methods in this class will be used by the {@link Preprocessor} class to
 * perform various trivial operations
 * 
 * @since GemFire 7.0
 */
public class PreprocessorUtils {

  public static TrimmedInput simpleTrim(String input) {
    if (input != null) {
      // First remove the trailing white spaces, we do not need those
      if (!containsOnlyWhiteSpaces(input)) {
        input = StringUtils.stripEnd(input, null);
      }
      String output = input.trim();
      return new TrimmedInput(output, input.length() - output.length());
    } else {
      return null;
    }
  }
  
  /**
   * 
   * This function will trim the given input string. It will not only remove the
   * spaces and tabs at the end but also compress multiple spaces and tabs to a
   * single space
   * 
   * @param input
   *          The input string on which the trim operation needs to be performed
   * @return String
   */
  public static TrimmedInput trim(final String input) {
    return trim(input, true);
  }

  /**
   * 
   * This function will trim the given input string. It will not only remove the
   * spaces and tabs at the end but also compress multiple spaces and tabs to a
   * single space
   * 
   * @param input
   *          The input string on which the trim operation needs to be performed
   * @param retainLineSeparator
   *          whether to retain the line separator.
   * 
   * @return String
   */
  public static TrimmedInput trim(final String input, final boolean retainLineSeparator) {
    if (input != null) {
      String inputCopy = input;
      StringBuffer output = new StringBuffer();
      // First remove the trailing white spaces, we do not need those
      inputCopy = StringUtils.stripEnd(inputCopy, null);
      // As this parser is for optionParsing, we also need to remove
      // the trailing optionSpecifiers provided it has previous
      // options. Remove the trailing LONG_OPTION_SPECIFIERs
      // in a loop. It is necessary to check for previous options for
      // the case of non-mandatory arguments.
      // "^(.*)(\\s-+)$" - something that ends with a space followed by a series of hyphens.
      while (Pattern.matches("^(.*)(\\s-+)$", inputCopy)) {
        inputCopy = StringUtils.removeEnd(inputCopy, SyntaxConstants.SHORT_OPTION_SPECIFIER);
        
        // Again we need to trim the trailing white spaces
        // As we are in a loop
        inputCopy = StringUtils.stripEnd(inputCopy, null);
      }
      // Here we made use of the String class function trim to remove the
      // space and tabs if any at the
      // beginning and the end of the string
      int noOfSpacesRemoved = 0;
      {
        int length = inputCopy.length();
        inputCopy = inputCopy.trim();
        noOfSpacesRemoved += length - inputCopy.length();
      }
      // Now we need to compress the multiple spaces and tabs to single space
      // and tabs but we also need to ignore the white spaces inside the
      // quotes and parentheses

      StringBuffer buffer = new StringBuffer();

      boolean startWhiteSpace = false;
      for (int i = 0; i < inputCopy.length(); i++) {
        char ch = inputCopy.charAt(i);
        buffer.append(ch);
        if (PreprocessorUtils.isWhitespace(ch)) {
          if (PreprocessorUtils.isSyntaxValid(buffer.toString())) {
            if (startWhiteSpace) {
              noOfSpacesRemoved++;
            } else {
              startWhiteSpace = true;
              if (ch == '\n') {
                if (retainLineSeparator) {
                  output.append("\n");
                }
              } else {
                output.append(" ");
              }
            }
            buffer.delete(0, buffer.length());
          } else {
            output.append(ch);
          }
        } else {
          startWhiteSpace = false;
          output.append(ch);
        }
      }
      return new TrimmedInput(output.toString(), noOfSpacesRemoved);
    } else {
      return null;
    }
  }

  /**
   * This function just does the simple job of removing white spaces from the
   * given input string
   * 
   * @param input
   *          The input String from which the spaces need to be removed
   * @return String
   */
  public static String removeWhiteSpaces(String input) {
    if (input != null) {
      input = trim(input).getString();
      StringBuffer output = new StringBuffer();
      for (int i = 0; i < input.length(); i++) {
        char ch = input.charAt(i);
        if (PreprocessorUtils.isWhitespace(ch)) {
          continue;
        } else {
          output.append(ch);
        }
      }
      return output.toString();
    } else {
      return null;
    }
  }

  /**
   * 
   * This function will check for the validity of the input provided.
   * 
   * For e.g; '" input"' is valid but '" input' is not a valid input same is the
   * case with input like a JSON string
   * 
   * @param input
   *          The input string which needs to be checked for proper syntax
   * @return Boolean
   */
  public static Boolean isSyntaxValid(String input) {
    if (input != null) {
      // We will need two different stacks one for double quotation
      // and the other one for checking brackets
      Stack<Character> stack = new Stack<Character>();
      if (input.length() > 0) {
        for (int i = 0; i < input.length(); i++) {
          char ch = input.charAt(i);
          if ('\\' == ch) {
            // It means that this is an escape sequence
            // So skip the next character as well
            i++;
            continue;
          }
          if (isValueEnclosingChar(ch)) {
            // First check whether the enclosing character
            // is a double quotation.
            if (EnclosingCharacters.DOUBLE_QUOTATION == ch) {
              Character popped = stack.pop();
              if (popped == EnclosingCharacters.DOUBLE_QUOTATION) {
                // Everything is normal
              } else {
                // We just push both the characters onto the stack
                if (popped != null) {
                  stack.push(popped);
                }
                stack.push(ch);
              }
            } else if (EnclosingCharacters.SINGLE_QUOTATION == ch) {
              Character popped = stack.pop();
              if (popped == EnclosingCharacters.SINGLE_QUOTATION) {
                // Everything is normal
              } else {
                // We just push both the characters onto the stack
                if (popped != null) {
                  stack.push(popped);
                }
                stack.push(ch);
              }
            } else {
              if (isOpeningBracket(ch)) {
                // If this a opening bracket then just push it onto
                // the stack
                stack.push(ch);
              } else {
                // This means that it is a closing bracket.
                // Now pop a character form the stack and check
                // whether both
                // the brackets match each other
                Character popped = stack.pop();
                if (matches(popped, ch)) {
                  // Everything is normal
                } else {
                  return false;
                }
              }
            }
          }
        }
      }
      if (stack.isEmpty()) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  private static boolean matches(Character popped, char ch) {
    if (popped != null) {
      outer: {
        if (isOpeningBracket(popped)) {
          if (EnclosingCharacters.OPENING_SQUARE_BRACKET == popped) {
            if (EnclosingCharacters.CLOSING_SQUARE_BRACKET == ch) {
              return true;
            } else {
              break outer;
            }
          }
          if (EnclosingCharacters.OPENING_CIRCULAR_BRACKET == popped) {
            if (EnclosingCharacters.CLOSING_CIRCULAR_BRACKET == ch) {
              return true;
            } else {
              break outer;
            }
          }
          if (EnclosingCharacters.OPENING_CURLY_BRACE == popped) {
            if (EnclosingCharacters.CLOSING_CURLY_BRACE == ch) {
              return true;
            } else {
              break outer;
            }
          }
        }
      }
      return false;
    } else {
      return false;
    }
  }

  // Not used at present
  @SuppressWarnings("unused")
  private static boolean isClosingBracket(char ch) {
    if (EnclosingCharacters.CLOSING_SQUARE_BRACKET == ch
        || EnclosingCharacters.CLOSING_CIRCULAR_BRACKET == ch
        || EnclosingCharacters.CLOSING_CURLY_BRACE == ch) {
      return true;
    } else {
      return false;
    }
  }

  private static boolean isOpeningBracket(char ch) {
    if (EnclosingCharacters.OPENING_SQUARE_BRACKET == ch
        || EnclosingCharacters.OPENING_CIRCULAR_BRACKET == ch
        || EnclosingCharacters.OPENING_CURLY_BRACE == ch) {
      return true;
    } else {
      return false;
    }
  }

  private static boolean isValueEnclosingChar(char ch) {
    if (EnclosingCharacters.OPENING_SQUARE_BRACKET == ch
        || EnclosingCharacters.CLOSING_SQUARE_BRACKET == ch
        || EnclosingCharacters.OPENING_CIRCULAR_BRACKET == ch
        || EnclosingCharacters.CLOSING_CIRCULAR_BRACKET == ch
        || EnclosingCharacters.OPENING_CURLY_BRACE == ch
        || EnclosingCharacters.CLOSING_CURLY_BRACE == ch
        || EnclosingCharacters.DOUBLE_QUOTATION == ch
        || EnclosingCharacters.SINGLE_QUOTATION == ch) {
      return true;
    }
    return false;
  }

  public static boolean containsOnlyWhiteSpaces(String input) {
    if (input != null) {
      for (int i = 0; i < input.length(); i++) {
        if (!PreprocessorUtils.isWhitespace(input.charAt(i))) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }
  
  public static boolean isWhitespace(char ch) {
    if (ch == ' ' || ch == '\t' || ch == '\n' || (ch == '\r' && SystemUtils.isWindows()) ) {
      return true;
    }
    return false;
  }
}
