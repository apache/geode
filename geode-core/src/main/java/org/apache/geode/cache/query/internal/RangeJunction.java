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
/*
 * Created on Jan 27, 2008
 */
package org.apache.geode.cache.query.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;

/**
 * This structure contains all the filter evaluatable CompiledComparision conditions which are using
 * identical index. Presently this Object will be formed only if the junction is an AND and will
 * either be a part of a GroupJunction or can be a stand alone Junction. In case it is a stand alone
 * Junction, then it can possibly have a not null Iter Operand, so that it can be evaluated along
 * with the expansion/truncation of index result.
 *
 */
public class RangeJunction extends AbstractGroupOrRangeJunction {
  private static final int RANGE_SIZE_ESTIMATE = 3;
  // moved to AbstractGroupOrRangeJunction
  // private CompiledValue iterOperands;

  RangeJunction(int operator, RuntimeIterator[] indpndntItr, boolean isCompleteExpansion,
      CompiledValue[] operands) {
    super(operator, indpndntItr, isCompleteExpansion, operands);

  }

  void addUnevaluatedFilterOperands(List unevaluatedFilterOps) {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }

  // moved to AbstractGroupOrRangeJunction
  /*
   * void addIterOperands(CompiledValue iterOps) { this.iterOperands = iterOps; }
   */
  private RangeJunction(AbstractGroupOrRangeJunction oldGJ, boolean completeExpansion,
      RuntimeIterator indpnds[], CompiledValue iterOp) {
    super(oldGJ, completeExpansion, indpnds, iterOp);
  }

  @Override
  AbstractGroupOrRangeJunction recreateFromOld(boolean completeExpansion, RuntimeIterator indpnds[],
      CompiledValue iterOp) {
    return new RangeJunction(this, completeExpansion, indpnds, iterOp);
  }

  @Override
  AbstractGroupOrRangeJunction createNewOfSameType(int operator, RuntimeIterator[] indpndntItr,
      boolean isCompleteExpansion, CompiledValue[] operands) {
    return new RangeJunction(operator, indpndntItr, isCompleteExpansion, operands);
  }

  @Override
  public PlanInfo getPlanInfo(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    /*
     * This function would be called only if the RangeJunction is a part of GroupJunction.It would
     * be invoked in the organized operands method of GroupJunction. In such case it is guaranteed
     * that all the operands are the filter operand using the same index. In such case there is zero
     * possibility o first iterator being either an iter operand or a constant. As those types of
     * Operands would be part of Group Junction
     */
    return this._operands[0].getPlanInfo(context);
  }

  public boolean isConditioningNeededForIndex(RuntimeIterator independentIter,
      ExecutionContext context, boolean completeExpnsNeeded)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    return true;
  }

  public int getOperator() {
    return LITERAL_and;
  }

  public boolean isBetterFilter(Filter comparedTo, ExecutionContext context, final int thisSize)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // If the current filter is equality & comparedTo filter is also equality based , then
    // return the one with lower size estimate is better
    boolean isThisBetter = true;

    // Go with the lowest cost when hint is used.
    if (context instanceof QueryExecutionContext && ((QueryExecutionContext) context).hasHints()) {
      return thisSize <= comparedTo.getSizeEstimate(context);
    }

    int thatOperator = comparedTo.getOperator();
    switch (thatOperator) {
      case TOK_EQ:
        isThisBetter = false;
        break;
      case TOK_NE:
      case TOK_NE_ALT:
        // Give preference to Range
        break;
      case LITERAL_and:
        // Asif: What to do? Let current be the better one for the want of better estimation
        break;
      case TOK_LE:
      case TOK_LT:
      case TOK_GE:
      case TOK_GT:
        // Give preference to this rather than single condition inequalities as a rangejunction
        // would possibly be bounded resulting in lesser values
        break;
      default:
        throw new IllegalArgumentException("The operator type =" + thatOperator + " is unknown");
    }

    return isThisBetter;
  }

  /**
   * Segregates the operands of the RangeJunction into iter evaluatable and filter evaluatable.
   */
  @Override
  OrganizedOperands organizeOperands(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    // get the list of operands to evaluate,
    // and evaluate operands that can use indexes first
    if (getOperator() == LITERAL_and) {
      return organizeOperandsForAndJunction(context);
    } else {
      throw new IllegalStateException(
          "In the case of an OR junction a RangeJunction should not be formed for now");
    }
  }

  // TODO:Asif: Currently the condition a!= null AND a =3, would be evalauted
  // via intersection. This needs to be optmized, but since it is not
  // common use case as != null is a not required operand , for the time being
  // ignoring the optmization.
  /**
   * For the filter evaluatable conditions , it creates the appropriate JunctionEvaluator (
   * NotEqualConditionEvaluator or SingleCondnEvaluator or DoubleCondnRangeJunctionEvaluator ). The
   * junction Evaluator itself is filter evaluatable. The operands which are of type != null , ==
   * null , != undefined, == undefined are left as it is & are not combined into a Junction
   * Evaluator. Thus the organized operand of RangeJunction may created atmost one Condition
   * Evaluator, will retain the operands containing null ,undefined conditions. In case there is a
   * equality condition , then it may result in a filter having just that condition assuming other
   * conditions satisfy the equality. In case it turns out that the conditions are mutually
   * exclusive then the organized operand would just contain a single filter evaluatable
   * CompiledLiteral (false) ( indicating empty resultset).
   */
  private OrganizedOperands organizeOperandsForAndJunction(ExecutionContext context)
      throws AmbiguousNameException, FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    List evalOperands = new ArrayList(_operands.length);
    int evalCount = 0;
    int lessCondnOp = -1;
    int greaterCondnOp = -1;
    CompiledComparison lessCondnOperand = null;
    CompiledComparison greaterCondnOperand = null;
    CompiledComparison equalCondnOperand = null;
    Object equalCondKey = null;
    Object lessCondnKey = null;
    Object greaterCondnKey = null;
    boolean emptyResults = false;
    Set notEqualTypeKeys = null;
    boolean possibleRangeFilter = false;
    IndexInfo indxInfo = null;
    for (int i = 0; i < _operands.length; i++) {
      CompiledValue operand = _operands[i];
      if (operand.getPlanInfo(context).evalAsFilter) {
        Indexable cc = (Indexable) operand;
        if (indxInfo == null) {
          indxInfo = cc.getIndexInfo(context)[0];
        }
        // TODO: Asif :Try to ensure that a CompiledUndefined never
        // goes in the RangeJunction. That means modify the
        // CompiledJunction code to avoid CompiledUndefined's inclusion
        // That way we can ensure that CompiledComparison only become part
        // of RangeJunction
        if (!cc.isRangeEvaluatable()) {
          evalCount++;
          evalOperands.add(0, _operands[i]);
          continue;
        }
        CompiledValue ccKey = ((CompiledComparison) cc).getKey(context);
        Object evaluatedCCKey = ccKey.evaluate(context);
        int operator = ((CompiledComparison) cc).reflectOnOperator(ccKey);
        if (evaluatedCCKey == null) {
          evalCount++;
          evalOperands.add(0, _operands[i]);
          continue;
        }
        if (equalCondnOperand != null) {
          emptyResults = !isConditionSatisfied(equalCondKey, evaluatedCCKey, operator);
          if (emptyResults) {
            break;
          } else {
            continue;
          }
        }

        switch (operator) {
          case TOK_EQ:
            possibleRangeFilter = false;
            equalCondnOperand = (CompiledComparison) cc;
            equalCondKey = evaluatedCCKey;
            break;
          case TOK_NE:
          case TOK_NE_ALT:
            possibleRangeFilter = true;
            if (notEqualTypeKeys == null) {
              notEqualTypeKeys = new HashSet(_operands.length);
            }
            evaluatedCCKey = TypeUtils.indexKeyFor(evaluatedCCKey);
            notEqualTypeKeys.add(evaluatedCCKey);
            break;
          case TOK_GE:
          case TOK_GT:
            possibleRangeFilter = true;
            if (greaterCondnOperand == null) {
              greaterCondnOperand = (CompiledComparison) cc;
              greaterCondnKey = evaluatedCCKey;
              greaterCondnOp = operator;
            } else {
              if (isConditionSatisfied(evaluatedCCKey, greaterCondnKey, greaterCondnOp)) {
                greaterCondnKey = evaluatedCCKey;
                greaterCondnOperand = (CompiledComparison) cc;
                greaterCondnOp = operator;
              }
            }
            break;
          case TOK_LE:
          case TOK_LT:
            // Asif: if there exists a previous equal Operand & current
            // condition's value is greater than the equal operand's value, it
            // will be empty results
            possibleRangeFilter = true;
            if (lessCondnOperand == null) {
              lessCondnOperand = (CompiledComparison) cc;
              lessCondnKey = evaluatedCCKey;
              lessCondnOp = operator;
            } else {
              if (isConditionSatisfied(evaluatedCCKey, lessCondnKey, lessCondnOp)) {
                lessCondnKey = evaluatedCCKey;
                lessCondnOperand = (CompiledComparison) cc;
                lessCondnOp = operator;
              }
            }
            break;
        }

      } else if (!_operands[i].isDependentOnCurrentScope(context)) {
        // TODO: Asif :Remove this Assert & else if condition after successful
        // testing of the build
        Support.assertionFailed(
            "An independentoperand should not ever be present as operand inside a GroupJunction as it should always be present only in CompiledJunction");
      } else {
        evalOperands.add(_operands[i]);
      }
    }
    if (!emptyResults) {
      Filter filter = null;
      if (equalCondnOperand != null) {
        // Check if any of the preceding inequality operands, that have not been
        // checked against the equality operand , are not able to satisfy the
        // equality.
        if (lessCondnOperand != null
            && !this.isConditionSatisfied(equalCondKey, lessCondnKey, lessCondnOp)) {
          emptyResults = true;
        } else if (greaterCondnOperand != null
            && !this.isConditionSatisfied(equalCondKey, greaterCondnKey, greaterCondnOp)) {
          emptyResults = true;
        } else if (notEqualTypeKeys != null) {
          Iterator itr = notEqualTypeKeys.iterator();
          while (itr.hasNext() && !emptyResults) {
            emptyResults =
                !this.isConditionSatisfied(equalCondKey, itr.next(), OQLLexerTokenTypes.TOK_NE);
          }
        }
        if (!emptyResults) {
          filter = equalCondnOperand;
        }
      } else if (possibleRangeFilter) {
        if (lessCondnOperand != null && greaterCondnOperand != null) {
          emptyResults = !checkForRangeBoundednessAndTrimNotEqualKeyset(notEqualTypeKeys,
              lessCondnKey, lessCondnOp, greaterCondnKey, greaterCondnOp);
          if (!emptyResults) {
            filter = new DoubleCondnRangeJunctionEvaluator(lessCondnOp, lessCondnKey,
                greaterCondnOp, greaterCondnKey,
                (notEqualTypeKeys == null || notEqualTypeKeys.isEmpty()) ? null : notEqualTypeKeys,
                indxInfo);
          }
        } else if (greaterCondnOperand != null) {
          filter = generateSingleCondnEvaluatorIfRequired(notEqualTypeKeys, greaterCondnOperand,
              greaterCondnOp, greaterCondnKey, indxInfo);
        } else if (lessCondnOperand != null) {
          filter = generateSingleCondnEvaluatorIfRequired(notEqualTypeKeys, lessCondnOperand,
              lessCondnOp, lessCondnKey, indxInfo);
        } else {
          assert notEqualTypeKeys != null && !notEqualTypeKeys.isEmpty();
          // TODO:Asif Ideally if there is a single NotEqualKey we should
          // not create NotEqualCondnEvaluator instead just add the
          // CompiledComparison
          // operand to the eval operands list. But since we do retain the
          // operand
          // correponding to the NotEqualKey in this function , we are creating
          // the NotEqualCondnEvaluator
          filter = new NotEqualConditionEvaluator(notEqualTypeKeys, indxInfo);
        }

      }
      if (emptyResults) {
        evalOperands.clear();
        evalCount = 1;
        evalOperands.add(new CompiledLiteral(Boolean.FALSE));
      } else if (filter != null) {
        evalCount++;
        evalOperands.add(0, filter);
      }
    } else {
      // Asif: Create a new CompiledLiteral with boolean false
      evalOperands.clear();
      evalCount = 1;
      evalOperands.add(new CompiledLiteral(Boolean.FALSE));
    }

    // If no hints were provided, we continue with our single index solution
    if (!(context instanceof QueryExecutionContext)
        || !((QueryExecutionContext) context).hasMultiHints()) {
      // At the end check if the unevaluatedIterOperand
      // are null or not. This could be the case only if at top level
      // GroupJunction is formed having multiple RangeJunctions & other
      // iter operands & then only one RangeJunction is treated as filter
      // rest all as iter operands. In that case , the only iter operand is
      // that which is added externally to RangeJunction. If the top
      // level was a RangeJunction then the iter operands would have been
      // part of it at the time of creation of RangeJunction & we would not have
      // to add it externally.
      if (getIterOperands() != null) {
        // Commented the assert for CompiledLike which creates 2 or 3 CompiledComparisons
        // for the same operand. The protGetPlanInfo in CompiledLike could return evalAsFilter
        // as true the first time and false the next time for the same operand.
        // Hence the evalOperands could contain CompiledComparisons more than number of indexes.

        // Support.Assert(evalOperands.size() == evalCount);
        evalOperands.add(getIterOperands());
      }
    }
    return createOrganizedOperandsObject(evalCount, evalOperands);

  }

  /**
   * Checks if key1 operator key2 is true or not. The operator could be =, != , <, >,<=,>=
   *
   * @return boolean true if the condition is satisfied else false
   */
  private boolean isConditionSatisfied(Object key1, Object key2, int operator)
      throws TypeMismatchException {
    return ((Boolean) TypeUtils.compare(key1, key2, operator)).booleanValue();
  }

  /**
   * Checks if the Range junction containing less & greater type of inequalities has a lower and
   * upper bound , in the sense that they do not represent a mutually exclusive condition like a> 10
   * and a <9 etc. If the condition is bounded in nature, it further checks if the not equal type
   * keys fall in the bounded range , else it removes it from the Not Equal Keys set
   *
   * @param notEqualKeys Set containing keys of operands having 'Not Equal' (!=) type conditions
   * @param lessCondnKey Key of the 'Less' condition operand
   * @param lessOperator Type of 'less' operator ( < or <=)
   * @param greaterCondnKey Key of the 'greater' condition operand
   * @param greaterCondnOp Type of 'greater' operator ( > or >=)
   * @return boolean true if the nature is bounded else false ( unbounded )
   */
  private boolean checkForRangeBoundednessAndTrimNotEqualKeyset(Set notEqualKeys,
      Object lessCondnKey, int lessOperator, Object greaterCondnKey, int greaterCondnOp)
      throws TypeMismatchException {
    // First check if the range is bounded or (unbounded and mutually
    // exclusive).
    // If it is unbounded immediately return a false
    if (isConditionSatisfied(greaterCondnKey, lessCondnKey, lessOperator)
        && isConditionSatisfied(lessCondnKey, greaterCondnKey, greaterCondnOp)) {
      // Nowremove those not equal conditions which do not satisfy the range
      if (notEqualKeys != null) {
        Iterator itr = notEqualKeys.iterator();
        Object neKey = null;
        while (itr.hasNext()) {
          neKey = itr.next();
          if (!this.isConditionSatisfied(neKey, greaterCondnKey, greaterCondnOp)
              || !this.isConditionSatisfied(neKey, lessCondnKey, lessOperator)) {
            itr.remove();
          }
        }
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Creates a Filter of type SingleCondnEvaluator if there exists atleast one key of type "NOT
   * EQUAL" which satisfies the 'less' or 'greater' type operand. Otherwise the Filter is nothing
   * but the CompiledComparison representing the 'less' or 'greater' inequality
   *
   * @param notEqualKeys Set containing NotEqual type Keys
   * @param operand CompiledValue representing the 'Less' or 'Greater' operand
   * @param operator Type of 'Less' or 'Greater' operand
   * @param condnKey The Key corresponding to the Operand representing the 'Less' or 'Greater'
   *        inequality
   * @param indxInfo The IndexInfo object for this RangeJunction
   * @return Filter object of type CompiledComparison or RangeJunction.SingleCondnEvaluator object
   */
  private Filter generateSingleCondnEvaluatorIfRequired(Set notEqualKeys, CompiledValue operand,
      int operator, Object condnKey, IndexInfo indxInfo) throws TypeMismatchException {
    Filter rangeFilter;
    if (notEqualKeys != null) {
      // Eliminate all the not equal keys which will never be satisfied by
      // the given greater condn
      Iterator itr = notEqualKeys.iterator();
      while (itr.hasNext()) {
        Object neKey = itr.next();
        if (!((Boolean) TypeUtils.compare(neKey, condnKey, operator)).booleanValue()) {
          itr.remove();
        }
      }
      if (notEqualKeys.isEmpty()) {
        notEqualKeys = null;
      }
    }
    rangeFilter = (notEqualKeys != null)
        ? new SingleCondnEvaluator(operator, condnKey, notEqualKeys, indxInfo) : (Filter) operand;
    return rangeFilter;
  }

  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Object r = _operands[0].evaluate(context); // UNDEFINED, null, or a
    // Boolean

    // if it's false and the op in this case will always be AND so return
    // false immediately
    if (r instanceof Boolean && !((Boolean) r).booleanValue())
      return r;
    if (r == null || r == QueryService.UNDEFINED)
      r = QueryService.UNDEFINED; // keep going to see if we hit a
    // short-circuiting truth value
    else if (!(r instanceof Boolean))
      throw new TypeMismatchException(
          "LITERAL_and/LITERAL_or operands must be of type boolean, not type '"
              + r.getClass().getName() + "'");
    for (int i = 1; i < _operands.length; i++) {
      Object ri = _operands[i].evaluate(context); // UNDEFINED, null, or
      // Boolean
      if (ri instanceof Boolean && !((Boolean) ri).booleanValue())
        return ri;
      if (ri == null || ri == QueryService.UNDEFINED || r == QueryService.UNDEFINED) {
        r = QueryService.UNDEFINED;
        continue; // keep going to see if we hit a short-circuiting
        // truth value
      } else if (!(ri instanceof Boolean))
        throw new TypeMismatchException(
            "LITERAL_and/LITERAL_or operands must be of type boolean, not type '"
                + ri.getClass().getName() + "'");
      // now do the actual and

      r = new Boolean(((Boolean) r).booleanValue() && ((Boolean) ri).booleanValue());

    }
    return r;
  }

  @Override
  public int getType() {
    return LITERAL_and;
  }

  @Override
  public void visitNodes(NodeVisitor visitor) {
    Support.assertionFailed("Should not have come here");
  }

  public int getSizeEstimate(ExecutionContext context) {
    // TODO:Asif:Try to estimate better
    return RANGE_SIZE_ESTIMATE;
  }

  /*
   * private organizeOperandsForORJunction() { }
   */
  /**
   * Test method which checks if the Filter operand is of type SingleCondnEvaluator
   */
  static boolean isInstanceOfSingleCondnEvaluator(Object o) {
    return o instanceof RangeJunction.SingleCondnEvaluator;
  }

  /**
   * Test method which checks if the Filter operand is of type NotEqualConditionEvaluator
   */
  static boolean isInstanceOfNotEqualConditionEvaluator(Object o) {
    return o instanceof RangeJunction.NotEqualConditionEvaluator;
  }

  /**
   * Test method which checks if the Filter operand is of type DoubleCondnRangeJunctionEvaluator
   */
  static boolean isInstanceOfDoubleCondnRangeJunctionEvaluator(Object o) {
    return o instanceof RangeJunction.DoubleCondnRangeJunctionEvaluator;
  }

  /**
   * Test function which retrieves the "NOT EQUAL KEYS"
   *
   * @param o Object of type NotEqualConditionEvaluator from which the set containing the keys for
   *        removal need to be retrieved
   * @return Unmodifiable Set containing the keys for removal
   */
  static Set getKeysToBeRemoved(Object o) {
    if (o instanceof NotEqualConditionEvaluator) {
      if (((NotEqualConditionEvaluator) o).notEqualTypeKeys == null) {
        return null;
      }
      return Collections.unmodifiableSet(((NotEqualConditionEvaluator) o).notEqualTypeKeys);
    } else {
      throw new IllegalStateException(
          "The Object is not of type NotEqualConditionEvaluator");
    }
  }

  /**
   * Test function which retrieves the SingleCondnEvaluator operator
   *
   * @param o Object of type SingleCondnEvaluator from which the set containing the keys for removal
   *        need to be retrieved
   * @return int indicating the operator
   */
  static int getSingleCondnEvaluatorOperator(Object o) {
    if (o instanceof SingleCondnEvaluator) {
      return ((SingleCondnEvaluator) o).condnOp;
    } else {
      throw new IllegalStateException(
          "The Object is not of type NotEqualConditionEvaluator");
    }
  }

  /**
   * Test function which retrieves the evaluated Key for a SingleCondnEvaluator operator
   *
   * @param o Object of type SingleCondnEvaluator from which the set containing the keys for removal
   *        need to be retrieved
   * @return Object representing the evaluated Key
   */
  static Object getSingleCondnEvaluatorKey(Object o) {
    if (o instanceof SingleCondnEvaluator) {
      return ((SingleCondnEvaluator) o).condnKey;
    } else {
      throw new IllegalStateException(
          "The Object is not of type NotEqualConditionEvaluator");
    }
  }

  /**
   * Test function which retrieves the LESS type evaluated Key for a DoubleCondnEvaluator operator
   *
   * @param o Object of type DoubleCondnEvaluator
   * @return Object representing the evaluated Key of Less Type
   */
  static Object getDoubleCondnEvaluatorLESSKey(Object o) {
    if (o instanceof DoubleCondnRangeJunctionEvaluator) {
      return ((DoubleCondnRangeJunctionEvaluator) o).lessCondnKey;
    } else {
      throw new IllegalStateException(
          "The Object is not of type NotEqualConditionEvaluator");
    }
  }

  /**
   * Test function which retrieves the GREATER type evaluated Key for a DoubleCondnEvaluator
   * operator
   *
   * @param o Object of type DoubleCondnEvaluator
   * @return Object representing the evaluated Key of GREATER Type
   */
  static Object getDoubleCondnEvaluatorGreaterKey(Object o) {
    if (o instanceof DoubleCondnRangeJunctionEvaluator) {
      return ((DoubleCondnRangeJunctionEvaluator) o).greaterCondnKey;
    } else {
      throw new IllegalStateException(
          "The Object is not of type NotEqualConditionEvaluator");
    }
  }

  /**
   * Test function which retrieves the operator of Less Type
   *
   * @param o Object of type DoubleCondnEvaluator
   * @return int indicating the operator of less Type
   */
  static int getDoubleCondnEvaluatorOperatorOfLessType(Object o) {
    if (o instanceof DoubleCondnRangeJunctionEvaluator) {
      return ((DoubleCondnRangeJunctionEvaluator) o).lessCondnOp;
    } else {
      throw new IllegalStateException(
          "The Object is not of type NotEqualConditionEvaluator");
    }
  }

  /**
   * Test function which retrieves the operator of GREATER Type
   *
   * @param o Object of type DoubleCondnEvaluator
   * @return int indicating the operator of less Type
   */
  static int getDoubleCondnEvaluatorOperatorOfGreaterType(Object o) {
    if (o instanceof DoubleCondnRangeJunctionEvaluator) {
      return ((DoubleCondnRangeJunctionEvaluator) o).greaterCondnOp;
    } else {
      throw new IllegalStateException(
          "The Object is not of type NotEqualConditionEvaluator");
    }
  }

  /**
   * Test function which retrieves the underlying Index for a NotEqualConditionEvaluator operator
   *
   * @param o Object of type NotEqualConditionEvaluator from which the index needs to be retrieved
   */
  static Index getIndex(Object o) {
    if (o instanceof NotEqualConditionEvaluator) {
      return ((NotEqualConditionEvaluator) o).indxInfo._index;
    } else {
      throw new IllegalStateException(
          "The Object is not of type NotEqualConditionEvaluator");
    }
  }

  /**
   * Filter Object created by the RangeJunction on invocation of its organizedOperands method. The
   * object of this class will be created only if RangeJunction contains more than one 'NOT EQUAL' (
   * != ) type conditions ( apart from conditions having null or undefined as key). This class is
   * also extended by SingleCondnEvaluator and DoubleCondnRangeJunctionEvaluator
   *
   *
   */
  private static class NotEqualConditionEvaluator extends AbstractCompiledValue implements Filter {
    final Set notEqualTypeKeys;

    final IndexInfo indxInfo;

    /**
     *
     * @param notEqualTypeKeys java.utils.Set object containing the Keys of the 'NOT EQUAL' type
     *        conditions ( a != 3 and a !=5) For DoubleCondnRangeJunctionEvaluator , this may be
     *        null
     * @param indxInfo The IndexInfo object corresponding to the RangeJunction
     */
    NotEqualConditionEvaluator(Set notEqualTypeKeys, IndexInfo indxInfo) {
      this.notEqualTypeKeys = notEqualTypeKeys;
      this.indxInfo = indxInfo;
    }

    @Override
    public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit,
        boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
        boolean isIntersection, boolean conditioningNeeded, boolean evalProj)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      ObjectType resultType = this.indxInfo._index.getResultSetType();
      int indexFieldsSize = -1;
      SelectResults set = null;
      Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
      boolean useLinkedDataStructure = false;
      boolean nullValuesAtStart = true;
      if (orderByClause != null && orderByClause.booleanValue()) {
        List orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
        useLinkedDataStructure = orderByAttrs.size() == 1;
        nullValuesAtStart = !((CompiledSortCriterion) orderByAttrs.get(0)).getCriterion();
      }

      if (resultType instanceof StructType) {
        if (context.getCache().getLogger().fineEnabled()) {
          context.getCache().getLogger()
              .fine("StructType resultType.class=" + resultType.getClass().getName());
        }
        if (useLinkedDataStructure) {
          set = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
              : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
        } else {
          set = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
        }
        indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
      } else {
        if (context.getCache().getLogger().fineEnabled()) {
          context.getCache().getLogger()
              .fine("non-StructType resultType.class=" + resultType.getClass().getName());
        }
        if (useLinkedDataStructure) {
          set = context.isDistinct() ? new LinkedResultSet(resultType)
              : new SortedResultsBag(resultType, nullValuesAtStart);
        } else {
          set = QueryUtils.createResultCollection(context, resultType);
        }
        indexFieldsSize = 1;
      }
      // actual index lookup
      QueryObserver observer = QueryObserverHolder.getInstance();
      /*
       * Asif : First obtain the match level of index resultset. If the match level happens to be
       * zero , this implies that we just have to change the StructType ( again if only the Index
       * resultset is a StructBag). If the match level is zero & expand to to top level flag is true
       * & iff the total no. of iterators in current scope is greater than the no. of fields in
       * StructBag , then only we need to do any expansion.
       *
       */
      try {
        observer.beforeIndexLookup(this.indxInfo._index, OQLLexerTokenTypes.TOK_NE,
            this.notEqualTypeKeys);
        context.cachePut(CompiledValue.INDEX_INFO, this.indxInfo);
        this.indxInfo._index.query(set, notEqualTypeKeys, context);
      } finally {
        observer.afterIndexLookup(set);
      }
      return QueryUtils.getConditionedIndexResults(set, this.indxInfo, context, indexFieldsSize,
          completeExpansionNeeded, iterOperands, indpndntItrs);
    }

    @Override
    public SelectResults auxFilterEvaluate(ExecutionContext context,
        SelectResults intermediateResults) throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

    public Object evaluate(ExecutionContext context) throws FunctionDomainException,
        TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
      Object evaluatedPath = this.indxInfo._path.evaluate(context);
      return evaluate(context, evaluatedPath);
    }

    public boolean isConditioningNeededForIndex(RuntimeIterator independentIter,
        ExecutionContext context, boolean completeExpnsNeeded)
        throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
      return true;
    }

    public Object evaluate(ExecutionContext context, Object evaluatedPath)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      Iterator itr = this.notEqualTypeKeys.iterator();
      while (itr.hasNext()) {
        Object val = itr.next();
        Object result = TypeUtils.compare(evaluatedPath, val, TOK_NE);
        if (result instanceof Boolean) {
          if (!((Boolean) result).booleanValue()) {
            return Boolean.FALSE;
          }
        } else {
          throw new TypeMismatchException(
              "NotEqualConditionEvaluator should evaluate to boolean type");
        }
      }
      return Boolean.TRUE;

    }

    public int getType() {
      return NOTEQUALCONDITIONEVALUATOR;
    }

    public int getSizeEstimate(ExecutionContext context) {
      return RANGE_SIZE_ESTIMATE;
    }

    @Override
    public void visitNodes(NodeVisitor visitor) {
      Support.assertionFailed("Should not have come here");
    }

    public int getOperator() {
      return LITERAL_and;
    }

    public boolean isBetterFilter(Filter comparedTo, ExecutionContext context, int thisSize)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      // If the current filter is equality & comparedTo filter is also equality based , then
      // return the one with lower size estimate is better
      boolean isThisBetter = true;

      int thatOperator = comparedTo.getOperator();

      // Go with the lowest cost when hint is used.
      if (context instanceof QueryExecutionContext
          && ((QueryExecutionContext) context).hasHints()) {
        return thisSize <= comparedTo.getSizeEstimate(context);
      }

      switch (thatOperator) {
        case TOK_EQ:
          isThisBetter = false;
          break;
        case TOK_NE:
        case TOK_NE_ALT:
          // Give preference to Range
          break;
        default:
          throw new IllegalArgumentException("The operator type =" + thatOperator + " is unknown");
      }

      return isThisBetter;
    }

  }

  /**
   * Filter object of this type gets created if there exists atleast one "NOT EQUAL" type condition
   * and a single condition containing an inequality of type 'Less' or 'Greater'. The Where clause
   * may actually contain multiple 'Less' type inequality or multiple 'Greater' type inequality (
   * though not both 'Less' and 'Greater' together). The RangeJunction will identify the most
   * specific inequality for the AND junction. Thus if something like a > 7 and a >=6 , will be
   * sufficiently represented by a > 7
   *
   *
   */
  private static class SingleCondnEvaluator extends NotEqualConditionEvaluator {
    protected int condnOp = -1;

    protected final Object condnKey;

    @Override
    public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

    /**
     *
     * @param operator integer identifying the type of 'Less' or 'Greater' inequality
     * @param key Object representing the Key for the inequality
     * @param notEqualKeys Set containing the 'NOT EQUAL' Keys accompanying the 'Less' or 'Greater'
     *        inequality
     * @param indxInfo The IndexInfo object corresponding to the RangeJunction
     */
    SingleCondnEvaluator(int operator, Object key, Set notEqualKeys, IndexInfo indxInfo) {
      super(notEqualKeys, indxInfo);
      this.condnOp = operator;
      this.condnKey = key;
    }

    @Override
    public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit,
        boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
        boolean isIntersection, boolean conditioningNeeded, boolean evalProj)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      ObjectType resultType = this.indxInfo._index.getResultSetType();
      int indexFieldsSize = -1;
      SelectResults set = null;
      Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
      boolean useLinkedDataStructure = false;
      boolean nullValuesAtStart = true;
      if (orderByClause != null && orderByClause.booleanValue()) {
        List orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
        useLinkedDataStructure = orderByAttrs.size() == 1;
        nullValuesAtStart = !((CompiledSortCriterion) orderByAttrs.get(0)).getCriterion();
      }
      if (resultType instanceof StructType) {
        if (context.getCache().getLogger().fineEnabled()) {
          context.getCache().getLogger()
              .fine("StructType resultType.class=" + resultType.getClass().getName());
        }
        if (useLinkedDataStructure) {
          set = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
              : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
        } else {
          set = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
        }
        indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
      } else {
        if (context.getCache().getLogger().fineEnabled()) {
          context.getCache().getLogger()
              .fine("non-StructType resultType.class=" + resultType.getClass().getName());
        }
        if (useLinkedDataStructure) {
          set = context.isDistinct() ? new LinkedResultSet(resultType)
              : new SortedResultsBag(resultType, nullValuesAtStart);
        } else {
          set = QueryUtils.createResultCollection(context, resultType);
        }
        indexFieldsSize = 1;
      }
      // actual index lookup
      QueryObserver observer = QueryObserverHolder.getInstance();
      /*
       * Asif : First obtain the match level of index resultset. If the match level happens to be
       * zero , this implies that we just have to change the StructType ( again if only the Index
       * resultset is a StructBag). If the match level is zero & expand to to top level flag is true
       * & iff the total no. of iterators in current scope is greater than the no. of fields in
       * StructBag , then only we need to do any expansion.
       *
       */
      try {
        observer.beforeIndexLookup(this.indxInfo._index, this.condnOp, this.condnKey);
        context.cachePut(CompiledValue.INDEX_INFO, this.indxInfo);
        this.indxInfo._index.query(this.condnKey, this.condnOp, set, notEqualTypeKeys, context);
      } finally {
        observer.afterIndexLookup(set);
      }
      return QueryUtils.getConditionedIndexResults(set, this.indxInfo, context, indexFieldsSize,
          completeExpansionNeeded, iterOperands, indpndntItrs);

    }

    public Object evaluate(ExecutionContext context) throws TypeMismatchException,
        FunctionDomainException, NameResolutionException, QueryInvocationTargetException {
      Object evaluatedPath = this.indxInfo._path.evaluate(context);
      Boolean result = (Boolean) super.evaluate(context, evaluatedPath);
      if (result.booleanValue()) {
        result = (Boolean) TypeUtils.compare(evaluatedPath, this.condnKey, this.condnOp);
      }
      return result;
    }

    @Override
    public int getType() {
      return SINGLECONDNEVALUATOR;
    }

    @Override
    public void visitNodes(NodeVisitor visitor) {
      Support.assertionFailed("Should not have come here");
    }

    @Override
    public SelectResults auxFilterEvaluate(ExecutionContext context,
        SelectResults intermediateResults) throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Filter object of this type gets created if there exists a bounded condition like a >7 and a > 8
   * and a< 10 and a <11. The RangeJunction will identify the most specific inequality of each type
   * for the AND junction. Thus the conditions a > 8 and a <10 will be used to form the Object of
   * this class. For this evaluator only, the notEqualTypeKeys present in its super class may be
   * null ( if there is no 'NOT EQUAL' type condition satisfying the bounded condition)
   *
   *
   */
  private static class DoubleCondnRangeJunctionEvaluator extends NotEqualConditionEvaluator {
    protected final int lessCondnOp;

    protected final int greaterCondnOp;

    protected final Object lessCondnKey;

    protected final Object greaterCondnKey;

    /**
     *
     * @param lessCondnOp integer identifying the upper bound ( < or <= )
     * @param lessCondnKey Object representing the Upper Bound Key
     * @param greaterCondnOp integer identifying the lower bound ( > or >= )
     * @param greaterCondnKey Object representing the lower Bound Key
     * @param notEqualTypeKeys Set containing the 'NOT EQUAL' Keys accompanying the 'Less' or
     *        'Greater' inequality
     * @param indexInfo The IndexInfo object corresponding to the RangeJunction
     */
    DoubleCondnRangeJunctionEvaluator(int lessCondnOp, Object lessCondnKey, int greaterCondnOp,
        Object greaterCondnKey, Set notEqualTypeKeys, IndexInfo indexInfo) {
      super(notEqualTypeKeys, indexInfo);
      this.lessCondnOp = lessCondnOp;
      this.lessCondnKey = lessCondnKey;
      this.greaterCondnOp = greaterCondnOp;
      this.greaterCondnKey = greaterCondnKey;
    }

    @Override
    public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit,
        boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
        boolean isIntersection, boolean conditioningNeeded, boolean evalProj)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      ObjectType resultType = this.indxInfo._index.getResultSetType();
      int indexFieldsSize = -1;
      SelectResults set = null;
      Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
      boolean useLinkedDataStructure = false;
      boolean nullValuesAtStart = true;
      if (orderByClause != null && orderByClause.booleanValue()) {
        List orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
        useLinkedDataStructure = orderByAttrs.size() == 1;
        nullValuesAtStart = !((CompiledSortCriterion) orderByAttrs.get(0)).getCriterion();
      }

      if (resultType instanceof StructType) {
        if (context.getCache().getLogger().fineEnabled()) {
          context.getCache().getLogger()
              .fine("StructType resultType.class=" + resultType.getClass().getName());
        }
        if (useLinkedDataStructure) {
          set = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
              : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
        } else {
          set = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
        }
        indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
      } else {
        if (context.getCache().getLogger().fineEnabled()) {
          context.getCache().getLogger()
              .fine("non-StructType resultType.class=" + resultType.getClass().getName());
        }
        if (useLinkedDataStructure) {
          set = context.isDistinct() ? new LinkedResultSet(resultType)
              : new SortedResultsBag(resultType, nullValuesAtStart);
        } else {
          set = QueryUtils.createResultCollection(context, resultType);
        }
        indexFieldsSize = 1;
      }
      // actual index lookup
      // Shobhit: Limit can not be applied at index level for RangeJunction as
      // other conditions are applied after coming out of index query method.
      context.cachePut(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX, Boolean.FALSE);
      QueryObserver observer = QueryObserverHolder.getInstance();
      /*
       * Asif : First obtain the match level of index resultset. If the match level happens to be
       * zero , this implies that we just have to change the StructType ( again if only the Index
       * resultset is a StructBag). If the match level is zero & expand to to top level flag is true
       * & iff the total no. of iterators in current scope is greater than the no. of fields in
       * StructBag , then only we need to do any expansion.
       *
       */
      try {
        observer.beforeIndexLookup(this.indxInfo._index, this.greaterCondnOp, this.greaterCondnKey,
            this.lessCondnOp, this.lessCondnKey, this.notEqualTypeKeys);
        context.cachePut(CompiledValue.INDEX_INFO, this.indxInfo);
        this.indxInfo._index.query(this.greaterCondnKey, this.greaterCondnOp, this.lessCondnKey,
            this.lessCondnOp, set, notEqualTypeKeys, context);
      } finally {
        observer.afterIndexLookup(set);
      }
      return QueryUtils.getConditionedIndexResults(set, this.indxInfo, context, indexFieldsSize,
          completeExpansionNeeded, iterOperands, indpndntItrs);

    }

    @Override
    public SelectResults auxFilterEvaluate(ExecutionContext context,
        SelectResults intermediateResults) throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

    public Object evaluate(ExecutionContext context) throws FunctionDomainException,
        TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
      Object evaluatedPath = this.indxInfo._path.evaluate(context);
      Boolean result = (Boolean) super.evaluate(context, evaluatedPath);
      if (result.booleanValue()) {
        result = (Boolean) TypeUtils.compare(evaluatedPath, this.lessCondnKey, this.lessCondnOp);
        result = result.booleanValue()
            ? (Boolean) TypeUtils.compare(evaluatedPath, this.greaterCondnKey, this.greaterCondnOp)
            : Boolean.FALSE;
      }
      return result;
    }

    @Override
    public int getType() {
      return DOUBLECONDNRANGEJUNCTIONEVALUATOR;
    }

    @Override
    public void visitNodes(NodeVisitor visitor) {
      Support.assertionFailed("Should not have come here");
    }
  }
}
