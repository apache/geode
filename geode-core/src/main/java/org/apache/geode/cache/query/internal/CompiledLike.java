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

import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.query.internal.index.PrimaryKeyIndex;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.internal.PdxString;

public class CompiledLike extends CompiledComparison {

  private static final Logger logger = LogService.getLogger();

  private static final int WILDCARD_PERCENT = 0;

  private static final int WILDCARD_UNDERSCORE = 1;

  private final Object wildcardTypeKey = new Object();

  private final Object wildcardPositionKey = new Object();

  private final Object patternLengthKey = new Object();

  static final String LOWEST_STRING = "";

  private static final char BOUNDARY_CHAR = (char) 255;

  private static final char UNDERSCORE = '_';

  private static final char PERCENT = '%';

  private static final char BACKSLASH = '\\';

  private final CompiledValue var;

  private final Object isIndexEvaluatedKey = new Object();

  private final CompiledValue bindArg;

  CompiledLike(CompiledValue var, CompiledValue pattern) {
    super(var, pattern, OQLLexerTokenTypes.TOK_EQ);
    this.var = var;
    this.bindArg = pattern;
  }

  private int getWildcardPosition(ExecutionContext context) {
    return (Integer) context.cacheGet(wildcardPositionKey, -1);
  }

  private int getWildcardType(ExecutionContext context) {
    return (Integer) context.cacheGet(wildcardTypeKey, -1);
  }

  private int getPatternLength(ExecutionContext context) {
    return (Integer) context.cacheGet(patternLengthKey, 0);
  }

  private boolean getIsIndexEvaluated(ExecutionContext context) {
    return (Boolean) context.cacheGet(isIndexEvaluatedKey, false);
  }

  OrganizedOperands organizeOperands(ExecutionContext context, boolean completeExpansionNeeded,
      RuntimeIterator[] indpndntItrs) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    CompiledComparison[] cvs = getExpandedOperandsWithIndexInfoSetIfAny(context);
    Filter filter = null;
    if (cvs.length == 1) {
      // For the equality condition
      filter = cvs[0];
    } else {
      // 2 or 3 conditions; create junctions
      if ((getOperator() == OQLLexerTokenTypes.TOK_NE)
          && (getWildcardPosition(context) == getPatternLength(context) - 1)
          && (getWildcardType(context) == WILDCARD_PERCENT)) {
        // negation supported only for trailing %
        // GroupJunction is created since the boundary conditions go out of
        // range and will be evaluated as false if a RangeJunction was used
        // For example, for NOT LIKE a%, the CCs generated would be < A OR >= B
        // which would cause the checkForRangeBoundednessAndTrimNotEqualKeyset
        // method of RangeJunction to return false
        filter = new GroupJunction(OQLLexerTokenTypes.LITERAL_or, indpndntItrs,
            completeExpansionNeeded, cvs);

      } else {
        filter = new RangeJunction(OQLLexerTokenTypes.LITERAL_and, indpndntItrs,
            completeExpansionNeeded, cvs);
      }
    }

    OrganizedOperands result = new OrganizedOperands();
    result.isSingleFilter = true;
    result.filterOperand = filter;
    return result;
  }

  /**
   * Expands the CompiledLike operands based on sargability into multiple CompiledComparisons
   *
   * @return The generated CompiledComparisons
   */
  CompiledComparison[] getExpandedOperandsWithIndexInfoSetIfAny(ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException,
      FunctionDomainException, QueryInvocationTargetException {
    String pattern = (String) this.bindArg.evaluate(context);
    // check if it is filter evaluatable
    CompiledComparison[] cvs = getRangeIfSargable(context, this.var, pattern);

    for (CompiledComparison cc : cvs) {
      // negation supported only for trailing %
      if ((getOperator() == OQLLexerTokenTypes.TOK_NE)
          && (getWildcardPosition(context) == getPatternLength(context) - 1)
          && (getWildcardType(context) == WILDCARD_PERCENT)) {
        cc.negate();
      }
      cc.computeDependencies(context);
      // Set the indexinfo for the newly created CCs with the indexinfo of this
      // CompiledLike object
      IndexInfo[] thisIndexInfo = ((IndexInfo[]) context.cacheGet(this));
      if (thisIndexInfo != null && thisIndexInfo.length > 0) {
        // set the index key in the indexinfo of the CC since the index key
        // in the indexinfo of this object might have been modified in the
        // checkIfSargableAndRemoveEscapeChars method
        IndexInfo indexInfo =
            new IndexInfo(cc.getKey(context), thisIndexInfo[0]._path, thisIndexInfo[0]._index,
                thisIndexInfo[0]._matchLevel, thisIndexInfo[0].mapping, cc.getOperator());
        context.cachePut(cc, new IndexInfo[] {indexInfo});
      }
    }
    if (IndexManager.testHook != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("IndexManager TestHook is set in getExpandedOperandsWithIndexInfoSetIfAny.");
      }
      IndexManager.testHook.hook(12);
    }

    return cvs;
  }

  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults,
      boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
      boolean isIntersection, boolean conditioningNeeded, boolean evaluateProjection)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    OrganizedOperands newOperands =
        organizeOperands(context, completeExpansionNeeded, indpndntItrs);
    assert newOperands.iterateOperand == null;
    SelectResults result = intermediateResults;
    result = (newOperands.filterOperand).filterEvaluate(context, intermediateResults,
        completeExpansionNeeded, iterOperands, indpndntItrs, isIntersection, conditioningNeeded,
        evaluateProjection);

    return result;
  }

  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    RuntimeIterator grpItr = (RuntimeIterator) QueryUtils
        .getCurrentScopeUltimateRuntimeIteratorsIfAny(this, context).iterator().next();
    OrganizedOperands newOperands = organizeOperands(context, true, new RuntimeIterator[] {grpItr});
    assert newOperands.iterateOperand == null;
    SelectResults result = intermediateResults;
    result = (newOperands.filterOperand).filterEvaluate(context, intermediateResults);
    return result;
  }

  /**
   * Breaks down the like predicate (if sargable) into 2 or 3 CompiledComparisons based on the
   * presence of wildcard
   *
   * @return The generated CompiledComparisons
   */
  CompiledComparison[] getRangeIfSargable(ExecutionContext context, CompiledValue var,
      String pattern) {
    CompiledComparison[] cv = null;
    StringBuffer buffer = new StringBuffer(pattern);
    // check if the string has a % or _ anywhere
    int wildcardPosition = checkIfSargableAndRemoveEscapeChars(context, buffer);
    context.cachePut(wildcardPositionKey, wildcardPosition);
    int patternLength = buffer.length();
    context.cachePut(patternLengthKey, patternLength);
    context.cachePut(isIndexEvaluatedKey, true);
    // if wildcardPosition is >= 0 means it is sargable
    if (wildcardPosition >= 0) {
      int len = patternLength;
      if (wildcardPosition == 0) {
        // wildcard is the leading char
        // change the like predicate to >= "" and like
        cv = new CompiledComparison[] {new CompiledComparison(var,
            new CompiledLiteral(LOWEST_STRING), OQLLexerTokenTypes.TOK_GE), this};

      } else {
        // the wildcard is not the first char
        // delete all chars after the wildchar
        for (int k = len - 1; k >= wildcardPosition; k--) {
          buffer.deleteCharAt(k);
          --len;
        }

        String lowerBound = buffer.toString();
        int upperBoundPosition = len - 1;
        char upperBoundChar;
        while (true) {
          upperBoundChar = (buffer.charAt(upperBoundPosition));
          if (upperBoundChar == BOUNDARY_CHAR) {
            --upperBoundPosition;
          } else {
            upperBoundChar = (char) (buffer.charAt(upperBoundPosition) + 1);
            break;
          }
        }
        buffer.delete(upperBoundPosition, len);
        buffer.append(upperBoundChar);
        String upperBound = buffer.toString();
        CompiledComparison c1 =
            new CompiledComparison(var, new CompiledLiteral(lowerBound), OQLLexerTokenTypes.TOK_GE);
        CompiledComparison c2 =
            new CompiledComparison(var, new CompiledLiteral(upperBound), OQLLexerTokenTypes.TOK_LT);

        // if % is not the last char in the string.
        // or the wildchar is _ which could be anywhere
        if (len < (patternLength - 1) || getWildcardType(context) == WILDCARD_UNDERSCORE) {
          // negation not supported if % is not the last char and also for a _
          // anywhere
          if (getOperator() == OQLLexerTokenTypes.TOK_NE) {
            cv = new CompiledComparison[] {new CompiledComparison(var,
                new CompiledLiteral(LOWEST_STRING), OQLLexerTokenTypes.TOK_GE), this};
          } else {
            // the like predicate is broken into 3 compiled comparisons
            cv = new CompiledComparison[] {c1, c2, this};
          }

        } else {
          // % is at the end of the string
          // the like predicate is broken down to 2 compile comparisons
          cv = new CompiledComparison[] {c1, c2};
        }
      }
    } else {
      // not sargable
      // Change the like predicate to equality
      cv = new CompiledComparison[] {
          new CompiledComparison(var, new CompiledLiteral(buffer.toString()), getOperator())};
    }

    return cv;
  }

  private String getRegexPattern(String pattern) {
    StringBuilder sb = new StringBuilder();
    boolean prevMetaChar = false;
    int len = pattern.length();

    for (int i = 0; i < len; i++) {
      char ch = pattern.charAt(i);
      switch (ch) {
        // meta chars: \ ^ * . + ? ( ) | [ ]
        case ']':
        case '[':
        case '^':
        case '*':
        case '.':
        case '+':
        case '?':
        case '(':
        case ')':
        case '|':
        case '{':
        case '}':
        case '\\':
          // if ((ch == '\\') && (i+1) < len && (pattern.charAt(i+1) == '_' || pattern.charAt(i+1)
          // == '%')) {
          if ((ch == '\\')) {
            if (!((i + 1) < len && (pattern.charAt(i + 1) == '\\'))) {
              break;
            }
            i++;
          }
          // Check if subsequent chars are meta chars.
          // \Q is used for start of string literal
          // \E for end of string literal. E.g. \Q+*\E to escape +*
          if (!prevMetaChar) {
            sb.append('\\');
            sb.append('Q');
            prevMetaChar = true;
          }
          sb.append(ch);
          break;
        case '_': // replace with .
        case '%': // replace with .*
          if (prevMetaChar) {
            sb.append('\\');
            sb.append('E');
            prevMetaChar = false;
          }

          // Check if the % has a valid escape. Backtrack to check for \.
          // If the number of \ on back track is odd, then % is escaped.
          int numConsecutiveBackSlash = 0;
          for (int j = i - 1; j > -1; --j) {
            if (pattern.charAt(j) == '\\') {
              ++numConsecutiveBackSlash;
            } else {
              break;
            }
          }

          if ((numConsecutiveBackSlash % 2) == 0) {
            if (ch == '%') {
              sb.append(".*");
              // ignore successive '%'
              while ((i + 1) < len && pattern.charAt(i + 1) == '%') {
                i++;
              }
            } else {
              sb.append(".");
            }
          } else {
            // The percentage or underscore sign is escaped. Hence it is to be un-escaped now
            // So remove the backslash
            // sb.deleteCharAt(sb.length() - 1);
            sb.append(ch);
          }
          break;
        default:
          if (prevMetaChar) {
            sb.append('\\');
            sb.append('E');
            prevMetaChar = false;
          }
          sb.append(ch);
      }
    }
    return sb.toString();
  }

  /**
   * Checks if index can be used for Strings with wildcards. Two wild cards are supported % and _.
   * The wildcard could be at any index position of the string.
   *
   * @return position of wildcard if sargable otherwise -1
   */
  int checkIfSargableAndRemoveEscapeChars(ExecutionContext context, StringBuffer buffer) {
    int len = buffer.length();
    int wildcardPosition = -1;
    for (int i = 0; i < len; ++i) {
      char ch = buffer.charAt(i);
      if (ch == UNDERSCORE) {
        context.cachePut(wildcardTypeKey, WILDCARD_UNDERSCORE);
        wildcardPosition = i; // the position of the wildcard
        break;
      } else if (ch == PERCENT) {
        context.cachePut(wildcardTypeKey, WILDCARD_PERCENT);
        wildcardPosition = i; // the position of the wildcard
        break;
      } else if (ch == BACKSLASH) {
        if (i + 1 < len) {
          if (buffer.charAt(i + 1) == PERCENT || buffer.charAt(i + 1) == UNDERSCORE) {
            wildcardPosition = -1; // escape the wildcard
          }
          buffer.deleteCharAt(i); // one \ escapes next
          len--;
        }
      }
    }
    return wildcardPosition;
  }

  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    // reset the isIndexEvaluated flag here since index is not being used here
    context.cachePut(isIndexEvaluatedKey, false);

    Pattern pattern = (Pattern) context.cacheGet(this.bindArg);
    if (pattern == null) {
      String strPattern = this.bindArg.evaluate(context).toString(); // handles both Strings and
                                                                     // PdxStrings
      if (strPattern == null) {
        throw new UnsupportedOperationException(
            "Null values are not supported with LIKE predicate.");
      }
      pattern = Pattern.compile(getRegexPattern(strPattern), Pattern.MULTILINE | Pattern.DOTALL);
      context.cachePut(this.bindArg, pattern);
    }
    Object value = this.var.evaluate(context);
    if (value == null) {
      return null;
    }

    if (!((value instanceof String) || (value instanceof PdxString)
        || (value == QueryService.UNDEFINED))) {
      // throw new TypeMismatchException(
      // String.format("Unable to compare object of type ' %s ' with object of type ' %s '",
      // "java.lang.String", value.getClass().getName()));
      if (getOperator() == TOK_NE) {
        return true;
      }
      return false;
    }

    // Check if LIKE clause is negated (_operator == TOK_NE) in query.
    boolean isMatched = pattern.matcher(value.toString()).matches();
    if (getOperator() == TOK_NE) {
      isMatched = !isMatched;
    }
    return isMatched;
  }

  /**
   * @since GemFire 6.6
   */
  @Override
  protected PlanInfo protGetPlanInfo(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    /*
     * During filterevaluation, CompiledLike is converted to 2 or 3 CompiledComparisons. One of the
     * CCs could be a CompiledLike itself. For example If the wildcard is _ or the % is anywhere
     * except at the end in the pattern, a GroupJunction is created. For 'ab%cd', the GroupJunction
     * would be ">=ab AND < ac AND LIKE ab%cd". The check avoids the re-filterevaluation of this
     * CompiledLike.
     */
    PlanInfo result = null;
    if (getIsIndexEvaluated(context)) {
      result = new PlanInfo();
      result.evalAsFilter = false;
    } else {
      result = super.protGetPlanInfo(context);
      // CCs created have range conditions which are not supported by PrimaryKey
      // index. So disabling filter when PrimaryKey index is used
      if (result.indexes.size() > 0 && result.indexes.get(0) instanceof PrimaryKeyIndex) {
        result.evalAsFilter = false;
      }
    }
    return result;
  }

  @Override
  public int getType() {
    return LIKE;
  }

  @Override
  public boolean isLimitApplicableAtIndexLevel(ExecutionContext context) {
    return true;
  }

  @Override
  public boolean isOrderByApplicableAtIndexLevel(ExecutionContext context,
      String canonicalizedOrderByClause) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {

    if (this.getPlanInfo(context).evalAsFilter) {
      PlanInfo pi = this.getPlanInfo(context);
      if (pi.indexes.size() == 1) {
        IndexProtocol ip = (IndexProtocol) pi.indexes.get(0);
        if (ip.getCanonicalizedIndexedExpression().equals(canonicalizedOrderByClause)) {
          return true;
        }
      }
    }
    return false;
  }


}
