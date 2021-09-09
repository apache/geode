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
 * Created on Nov 18, 2005
 *
 */
package org.apache.geode.cache.query.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;

/**
 * An object of this class gets created during the organization of operands in a CompiledJunction.
 * It gets created if there exists an equi join condition across two regions with range indexes
 * available on both LHS as well as RHS of the condition. The CompositeGroupJunction may wrap
 * GroupJunctions belonging to the regions participating in the equi join condition. A single
 * CompositeGroupJunction may span multiple Regions and hence may contain multiple filter
 * evaluatable equi join conditions of different regions as well as their GroupJunctions if any.
 *
 *
 */
public class CompositeGroupJunction extends AbstractCompiledValue
    implements Filter, OQLLexerTokenTypes {

  private int operator = 0;
  // Asif :This class Object creation itself indicates that there will be
  // at least one filterable CC
  private List filterableCC = new ArrayList();
  // Asif : Can be null
  private List groupJunctions = null;
  private boolean completeExpansion = false;
  // TODO:Asif : Convert it into an array of independent iterators if possible.
  private List indpndntItrs = null;
  // Asif : can be null
  private List iterOperands = null;
  private RuntimeIterator[] indpndnts = null;

  CompositeGroupJunction(int operator, CompiledValue filterableCondn) {
    this.operator = operator;
    this.filterableCC.add(filterableCondn);
    this.indpndntItrs = new ArrayList();
  }

  @Override
  public List getChildren() {
    List children = new ArrayList();
    if (this.groupJunctions != null) {
      children.addAll(this.groupJunctions);
    }
    children.addAll(this.filterableCC);
    return children;
  }

  @Override
  public Object evaluate(ExecutionContext context) {
    throw new AssertionError("Should not have come here");
  }

  @Override
  public int getType() {
    return COMPOSITEGROUPJUNCTION;
  }

  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    if (this.operator == LITERAL_and) {
      return evaluateAndJunction(context);
    } else {
      return evaluateOrJunction(context);
    }
  }

  void addFilterableCompositeCondition(CompiledValue cv) {
    this.filterableCC.add(cv);
  }

  /**
   * Add the Independent iterator of the Group which is constituent of this CompositeGroupJunction
   *
   * @param itr RuntimeIterator The independent iterator for the region forming the
   *        CompositeGroupJunction
   */
  void addIndependentIterators(RuntimeIterator itr) {
    this.indpndntItrs.add(itr);
  }

  /**
   * Add the GroupJunction for a Region as a part of CompositeGroupJunction , implying that their
   * exists atleast one equi join condition which involes the Region which the GroupJunction
   * represents
   *
   * @param gj GroupJunction object which is a part of CompositeGroupJunction
   */
  void addGroupOrRangeJunction(AbstractGroupOrRangeJunction gj) {
    if (this.groupJunctions == null) {
      this.groupJunctions = new ArrayList();
    }
    this.groupJunctions.add(gj);
  }

  /**
   *
   * @param iterOps List of CompiledValues representing conditions which are iter evaluatable. This
   *        will be set only if there does not exist any independent condition or filter evaluatabel
   *        subtree CompiledJunction and the complete expansion flag is turned on
   */
  void addIterOperands(List iterOps) {
    if (this.iterOperands == null) {
      this.iterOperands = new ArrayList();
    }
    this.iterOperands.addAll(iterOps);
  }

  /**
   *
   * This flag gets toggled if only a Single CompositeGroupJunction gets created. It means that an
   * AllGroupJunction will not exist and the result obtained from the evaluation of
   * CompositeGroupJunction will be expanded to the level of query from clause ( i.e top level
   * iterators).
   *
   */
  void setCompleteExpansionOn() {
    this.completeExpansion = true;
    if (this.groupJunctions != null && this.groupJunctions.size() == 1) {
      AbstractGroupOrRangeJunction gj = (AbstractGroupOrRangeJunction) this.groupJunctions.get(0);
      gj.setCompleteExpansionOn();
    }
  }

  void setArrayOfIndependentItrs() {
    this.indpndnts = new RuntimeIterator[this.indpndntItrs.size()];
    this.indpndnts = (RuntimeIterator[]) this.indpndntItrs.toArray(this.indpndnts);
    // TODO:Asif identifying a cleaner way of making the
    this.indpndntItrs = null;
  }

  /**
   *
   * @param temp CompositeGroupJunction which gets merged in this CompositeGroupJunction. This
   *        fusion occurs during creation of junction if the order in which the conditions of where
   *        clause arrival are such that condition1 tying region1 & region2 , condition 2 tying
   *        region 3 & region 4 and condition 3 tying region 3 and region 4. In such case , finally
   *        it should result in a single CompositeGroupJunction containing region1, region2 , region
   *        3 and region4 conditions and theire correspodning GroupJunctions if any.
   */
  void mergeFilterableCCsAndIndependentItrs(CompositeGroupJunction temp) {
    this.filterableCC.addAll(temp.filterableCC);
    this.indpndntItrs.addAll(temp.indpndntItrs);
  }

  private SelectResults evaluateAndJunction(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // Asif: We will evaluate the iter operand while getting the result of
    // composite condition iff there exists only one Filterable Composite
    // Condition & no GroupJunction
    CompiledValue iterOp = null;
    SelectResults intermediateResults = null;
    // TODO: Check if null can come for the iter operand List
    if (this.iterOperands != null && !this.iterOperands.isEmpty()) {
      int len = this.iterOperands.size();
      if (len == 1) {
        iterOp = (CompiledValue) this.iterOperands.get(0);
      } else {
        CompiledValue[] newOperands = new CompiledValue[len];
        newOperands = (CompiledValue[]) this.iterOperands.toArray(newOperands);
        iterOp = new CompiledJunction(newOperands, this.operator);
      }
    }
    SortedSet intersectionSet = new TreeSet(new SelectResultsComparator());
    // TODO:Asif: Clean this logic. If the iter operand is not null when should
    // we evaluate it.? Ideally if we were to stick to our logic of delaying
    // evaluation of condition , then we shoudl ideally first do intersection of
    // results obtained using indexes on conditions & then apply iter operand on
    // it. But some how it appears that the final iteration will be more
    // expensive then evaluation. So we use for time being following idea. If
    // the iter condition is not null & there exists atleast one Group Junction
    // ( then we are sure it will need expansion to CompositeGroupJunction
    // level, so the iter operand can be evaluated at that point, else we will
    // evaluate it with the first available filterable cc

    // Asif : Obtain results of Filter evaluatable composite conditions
    // Support.Assert( iterOp == null || ( iterOp !=null &&
    // this.filterableCC.size() == 1 && this.groupJunctions == null), "The Iter
    // operand can be not null only if there exists single filterable CC & no
    // group junction");
    boolean delayIterOpEval = (this.groupJunctions != null && this.groupJunctions.size() != 0);
    Iterator itr = this.filterableCC.iterator();
    int filterableCCSize = this.filterableCC.size();
    if (filterableCCSize > 1) {
      for (int i = 0; i < (filterableCCSize - 1); i++) {
        CompiledValue cc = (CompiledValue) itr.next();
        intermediateResults = ((Filter) cc).filterEvaluate(context, intermediateResults, false,
            null/* iterOpn = null */, null/* send independent itrs null */, false, true, false);
        if (intermediateResults.isEmpty()) {
          StructType structType = QueryUtils.createStructTypeForRuntimeIterators(
              this.completeExpansion ? context.getCurrentIterators()
                  : QueryUtils.getDependentItrChainForIndpndntItrs(this.indpndnts, context));
          return QueryUtils.createStructCollection(context, structType);
        }
      }
    }
    CompiledValue cc = (CompiledValue) itr.next();
    intermediateResults = ((Filter) cc).filterEvaluate(context, intermediateResults,
        this.completeExpansion, delayIterOpEval ? null : iterOp,
        this.indpndnts /*
                        * Since this is the last condition pass the indpndt. grp of itrs so that
                        * result structset with correct placement of itrs can be formed
                        */, false, true, false);
    intersectionSet.add(intermediateResults);
    intermediateResults = null;
    if (iterOp != null && !delayIterOpEval) {
      iterOp = null;
    }
    Support.Assert(iterOp == null || (this.groupJunctions != null),
        "The Iter operand can be not null only if there exists atleast one group junction");
    // TODO:Asif Put this function in some Util class so that both
    // AllGroupJunction & CompositeGroupJunction
    // can use it. Identify a cleaner approach to it other than null check
    if (this.groupJunctions != null) {
      int len = this.groupJunctions.size();
      if (len > 1) {
        // Asif : Create an array of SelectResults for each of the GroupJunction
        // For each Group Junction there will be a corresponding array of
        // RuntimeIterators
        // which will map to the fields of the ResultSet obtained from
        // Group Junction
        SelectResults[] results = new SelectResults[len];
        // Asif : the final list will be some of all the iterators depending on
        // each of indpendent group if complete expansion is not needed
        List finalList = null;
        if (this.completeExpansion) {
          finalList = context.getCurrentIterators();
        } else {
          finalList = QueryUtils.getDependentItrChainForIndpndntItrs(this.indpndnts, context);
        }
        List expansionList = new LinkedList(finalList);
        RuntimeIterator[][] itrsForResultFields = new RuntimeIterator[len][];
        AbstractGroupOrRangeJunction gj = null;
        Iterator junctionItr = this.groupJunctions.iterator();
        List grpItrs = null;
        int j = 0;
        RuntimeIterator tempItr = null;
        while (junctionItr.hasNext()) {
          gj = (AbstractGroupOrRangeJunction) junctionItr.next();
          SelectResults filterResults = ((Filter) gj).filterEvaluate(context, null);
          Support.Assert(filterResults != null, "FilterResults cannot be null here");
          if (filterResults.isEmpty()) {
            // TODO Asif : Compact the code of creation of empty set.
            // Asif: Create an empty resultset of the required type & return
            // it. e cannot use the existing Resultset as it may not be expanded
            // to the required level.
            SelectResults empty = null;
            if (finalList.size() == 1) {
              ObjectType type = ((RuntimeIterator) finalList.iterator().next()).getElementType();
              if (type instanceof StructType) {
                empty = QueryUtils.createStructCollection(context, (StructTypeImpl) type);
              } else {
                empty = QueryUtils.createResultCollection(context, type);
              }
            } else {
              StructType strucType = QueryUtils.createStructTypeForRuntimeIterators(finalList);
              empty = QueryUtils.createStructCollection(context, strucType);
            }
            return empty;
          } else {
            results[j] = filterResults;
            grpItrs = context.getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(
                gj.getIndependentIteratorForGroup()[0]);
            itrsForResultFields[j] = new RuntimeIterator[grpItrs.size()];
            Iterator grpItr = grpItrs.iterator();
            int k = 0;
            while (grpItr.hasNext()) {
              tempItr = (RuntimeIterator) grpItr.next();
              itrsForResultFields[j][k++] = tempItr;
              expansionList.remove(tempItr);
            }
            ++j;
          }
        }
        // Do the cartesian of the different group junction results.
        // TODO:Asif Remove the time
        QueryObserver observer = context.getObserver();
        observer.beforeCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND(results);
        SelectResults grpCartRs = QueryUtils.cartesian(results, itrsForResultFields, expansionList,
            finalList, context, iterOp);
        observer.afterCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND();
        Support.Assert(grpCartRs != null, "ResultsSet obtained was NULL in CompositeGroupJunction");
        intersectionSet.add(grpCartRs);
      } else {
        // TODO:Asif : Examine this logic as expansion of a GroupJunction to a a
        // CompositeGroupJunction level may not be a good idea, as each filter
        // evaluatable condition in a GroupJunction will have to be
        // unnecessarily expanded to the Composite GroupJunction level . This
        // will be a heavy operation & also the size of the sets to be
        // intersected will also be quite large. Ideally we should expand the
        // GroupJunction to CompositeGroupJunction level only if there exists a
        // single filter evaluatable condition in a GroupJunction. Asif : If
        // there exists only one GroupJunction in a CompsoiteGroupJunction ,
        // then we can afford to expand the result of GroupJunction to the level
        // of CompositeGroupJunction directly within the GroupJunction Thus
        // create a new GroupJunction with independent group of iterators same
        // as that of CompositeGroupJunction if complete expansion is false. If
        // complete expansion is true then create a new GroupJunction only if
        // there is residual iter operand. Thus this is the only special case (&
        // iff completeExpansion boolean false) where in a GroupJunction will
        // contain more than one independent iterators TODO:ASIF: CHECK IT
        // OUT..................

        AbstractGroupOrRangeJunction newGJ =
            (AbstractGroupOrRangeJunction) this.groupJunctions.get(0);
        if (!this.completeExpansion) {
          newGJ = newGJ.recreateFromOld(this.completeExpansion, this.indpndnts, iterOp);
        } else if (iterOp != null) {
          // Asif :Complete expansion is true. In this case we should not pass
          // the Group of iterators belonging to the CompositeGroupJunction but
          // stick to independent iterator for the group. Also we are creating a
          // new GroupJunction below only bcoz of extra iter operand. For
          // toggling the complete xpansion flag of Grp Junction to on , that
          // task is alreday done. in setCompleteExpansionOn of CGJ

          newGJ = newGJ.recreateFromOld(this.completeExpansion,
              newGJ.getIndependentIteratorForGroup(), iterOp);
        }
        SelectResults rs = newGJ.filterEvaluate(context, null);
        if (rs.isEmpty()) {
          return rs;
        } else {
          intersectionSet.add(rs);
        }
      }
    }
    Iterator iter = intersectionSet.iterator();
    while (iter.hasNext()) {
      SelectResults sr = (SelectResults) iter.next();
      intermediateResults = (intermediateResults == null) ? sr
          : QueryUtils.intersection(intermediateResults, sr, context);
    }
    return intermediateResults;
  }

  /** invariant: the operand is known to be evaluated by iteration */
  private SelectResults evaluateOrJunction(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    SelectResults intermediateResults = null;
    Iterator itr = this.filterableCC.iterator();
    Support.Assert(this.iterOperands == null || this.iterOperands.isEmpty(),
        "The iter operands shoudl not have been present for OR junction");
    while (itr.hasNext()) {
      CompiledValue cc = (CompiledComparison) itr.next();
      SelectResults sr = ((Filter) cc).filterEvaluate(context, null, this.completeExpansion, null,
          this.indpndnts, false, true, false);
      intermediateResults =
          (intermediateResults == null) ? sr : QueryUtils.union(intermediateResults, sr, context);
    }
    // TODO:Asif Identify a cleaner approach
    if (this.groupJunctions != null) {
      int len = this.groupJunctions.size();
      if (len > 1) {
        // Asif : Create an array of SelectResults for each of the GroupJunction
        // For each Group Junction there will be a corresponding array of
        // RuntimeIterators which will map to the fields of the ResultSet
        // obtained from Group Junction.
        SelectResults[] grpResults = new SelectResults[1];
        // Asif : the final list will be some of all the iterators depending on
        // each of indpendent group if complete expansion is not needed
        List finalList = null;
        if (this.completeExpansion) {
          finalList = context.getCurrentIterators();
        } else {
          finalList = QueryUtils.getDependentItrChainForIndpndntItrs(this.indpndnts, context);
        }
        RuntimeIterator[][] itrsForResultFields = new RuntimeIterator[1][];

        AbstractGroupOrRangeJunction gj = null;
        Iterator junctionItr = this.groupJunctions.iterator();
        List grpItrs = null;
        RuntimeIterator tempItr = null;
        while (junctionItr.hasNext()) {
          List expansionList = new LinkedList(finalList);
          gj = (AbstractGroupOrRangeJunction) junctionItr.next();
          grpResults[0] = ((Filter) gj).filterEvaluate(context, null);
          grpItrs = context.getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(
              gj.getIndependentIteratorForGroup()[0]);
          itrsForResultFields[0] = new RuntimeIterator[grpItrs.size()];
          Iterator grpItr = grpItrs.iterator();
          int k = 0;
          while (grpItr.hasNext()) {
            tempItr = (RuntimeIterator) grpItr.next();
            itrsForResultFields[0][k++] = tempItr;
            expansionList.remove(tempItr);
          }
          SelectResults expandedResult =
              QueryUtils.cartesian(grpResults, itrsForResultFields, expansionList, finalList,
                  context, null/*
                                * Iter oprenad for OR Junction evaluation should be null
                                */);
          intermediateResults = (intermediateResults == null) ? expandedResult
              : QueryUtils.union(expandedResult, intermediateResults, context);
        }
      } else {
        // Asif : If there exists only one GroupJunction in a
        // CompsoiteGroupJunction , then we can afford to expand the
        // result of GroupJunction to the level of CompositeGroupJunction
        // directly within the GroupJunction Thus create a new GroupJunction
        // with independent group of iterators same as that of
        // CompositeGroupJunction This is the only special case where in
        // GroupJunction will contain more than one independent iterators If
        // Complete expansion is true, In this case we should not pass the
        // Group of iterators belonging to the CompositeGroupJunction but stick
        // to independent iterator for the group.
        AbstractGroupOrRangeJunction newGJ =
            (AbstractGroupOrRangeJunction) this.groupJunctions.get(0);
        if (!this.completeExpansion) {
          // TODO:Asif: Check it out ..................
          newGJ = newGJ.recreateFromOld(this.completeExpansion, this.indpndnts, null);
        }
        SelectResults rs = newGJ.filterEvaluate(context, null);
        intermediateResults =
            (intermediateResults == null) ? rs : QueryUtils.union(rs, intermediateResults, context);
      }
    }
    return intermediateResults;
  }

  /* Package methods */
  @Override
  public int getOperator() {
    return operator;
  }

  /**
   * @return Array of RuntimeIterator which represent the independent iterators of each Group which
   *         form the CompositeGroupJunction
   */
  // TODO:Asif : Identify a way to make it unmodifiable
  RuntimeIterator[] getIndependentIteratorsOfCJ() {
    return this.indpndnts;
  }

  /**
   *
   * @return List containing the filter evaluatable Composite Comparison conditions ( equi join
   *         conditions across the regions)
   */
  List getFilterableCCList() {
    // return unmodifiable copy
    return Collections.unmodifiableList(this.filterableCC);
  }

  /**
   *
   * @return List containg GroupJunctions which are part of this CompositeGroupJunction. It can be
   *         null if there does not exist any filter evaluatable condition belonging solely to an
   *         independent region or its group of iterators, constituting the CompositeGroupJunction,
   *         so that no GroupJunctions get assosciated with the CompositeGroupJunction
   *
   */
  List getGroupJunctionList() {
    // return unmodifiable copy
    return this.groupJunctions != null ? Collections.unmodifiableList(this.groupJunctions) : null;
  }

  /**
   *
   * @return List containing the iter evaluatable conditions.This Can be null in case there exists
   *         more than one filter operand in CompiledJunction in which case all the iter ops will
   *         become part of iter operand of CompiledJunction. This can be not null iff the complete
   *         expansion flag is true
   */
  List getIterOperands() {
    return this.iterOperands != null ? Collections.unmodifiableList(this.iterOperands) : null;
  }

  boolean getExpansionFlag() {
    return this.completeExpansion;
  }

  @Override
  public int getSizeEstimate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return 1;
  }
}
