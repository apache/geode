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
package com.gemstone.gemfire.cache.query.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.index.AbstractIndex;
import com.gemstone.gemfire.cache.query.internal.index.IndexData;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.cache.query.internal.index.IndexUtils;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * 
 * @author Eric Zoerner
 * @author Asif
 */
public class QueryUtils {
  private static final Logger logger = LogService.getLogger();
  
  /**
   * Return a SelectResults that is the intersection of c1 and c2. May or may
   * not return a modified c1 or c2.
   */
  public static SelectResults intersection(SelectResults c1,
                                           SelectResults c2,
                                           ExecutionContext contextOrNull) {
    QueryObserverHolder.getInstance().invokedQueryUtilsIntersection(c1,c2);
    assertCompatible(c1, c2);
    if (c1.isEmpty()) { return c1; }
    if (c2.isEmpty()) { return c2; }
    // iterate on the smallest one
    if (c1.size() < c2.size()) {
      return sizeSortedIntersection(c1, c2, contextOrNull);
    }
    else {
      return sizeSortedIntersection(c2, c1, contextOrNull);
    }
  }

  /**
   * Return a SelectResults that is the union of c1 and c2. May or may not
   * return a modified c1 or c2.
   */
  public static SelectResults union(SelectResults c1,
                                    SelectResults c2,
                                    ExecutionContext contextOrNull) {
    QueryObserverHolder.getInstance().invokedQueryUtilsUnion(c1,c2);
    assertCompatible(c1, c2);
    // iterate on the smallest one
    if (c1.size() < c2.size()) {
      return sizeSortedUnion(c1, c2, contextOrNull);
    }
    else {
      return sizeSortedUnion(c2, c1, contextOrNull);
    }
  }

  public static void assertCompatible(SelectResults sr1, SelectResults sr2) {
    Assert.assertTrue(sr1.getCollectionType().getElementType().equals(
        sr2.getCollectionType().getElementType()));
  }
  

  public static SelectResults createResultCollection(ExecutionContext context, ObjectType elementType) {
    return context.isDistinct() ? new ResultsSet(elementType) :
      new ResultsBag(elementType, context.getCachePerfStats());
  }
  
  public static SelectResults createStructCollection(ExecutionContext context, StructType elementType) {
    return context.isDistinct() ? new StructSet(elementType) :
      new StructBag(elementType, context.getCachePerfStats()) ;
  }
  
  public static SelectResults createResultCollection(boolean distinct, ObjectType elementType,
      ExecutionContext context) {
    return distinct ? new ResultsSet(elementType) :
      new ResultsBag(elementType, context.getCachePerfStats());
  }
  
  public static SelectResults createStructCollection(boolean distinct, StructType elementType, 
      ExecutionContext context) {
    return distinct ? new StructSet(elementType) :
      new StructBag(elementType, context.getCachePerfStats()) ;
  }
  

  /**
   * Returns an appropriate, empty <code>SelectResults</code>
   * @param objectType The <code>ObjectType</code> of the query results
   * @return an appropriate, empty <code>SelectResults</code>
   */
  public static SelectResults getEmptySelectResults(ObjectType objectType,
                                                    CachePerfStats statsOrNull) {
    SelectResults emptyResults = null;
    if (objectType instanceof StructType) {
      emptyResults = new StructBag((StructTypeImpl) objectType, statsOrNull);
    } else {
      emptyResults = new ResultsBag(objectType, statsOrNull);
    }
    return emptyResults;
  }

  /**
   * Returns an appropriate, empty <code>SelectResults</code>
   * @param collectionType The <code>CollectionType</code> of the query results
   * @return an appropriate, empty <code>SelectResults</code>
   */
  public static SelectResults getEmptySelectResults(CollectionType collectionType,
                                                    CachePerfStats statsOrNull) {
    SelectResults emptyResults = null;
    if (collectionType.isOrdered()) {
      // The collectionType is ordered.
      // The 'order by' clause was used in the query.
      // Wrap an ArrayList with a ResultsCollectionWrapper
      emptyResults = new ResultsCollectionWrapper(collectionType.getElementType(), new ArrayList());
    }
    else if (!collectionType.allowsDuplicates()) {
      // The collectionType does not allow duplicates.
      // The distinct keyword was used in the query.
      // Wrap a HashSet with a ResultsCollectionWrapper
      emptyResults = new ResultsCollectionWrapper(collectionType.getElementType(), new HashSet());
    }
    else {
      // Use ObjectType to determine the correct SelectResults implementation
      emptyResults = getEmptySelectResults(collectionType.getElementType(),
                                           statsOrNull);
    }
    return emptyResults;
  }

  // convenience method
  private static boolean isBag(SelectResults coln) {
    return coln.getCollectionType().allowsDuplicates();
  }

  /** collections are passed in from smallest to largest */
  // consider: taking intersection of two bags, only retain
  // the number of occurrences in the intersection equal to the
  // minimum number between the two bags
  private static SelectResults sizeSortedIntersection(SelectResults small,
                                                      SelectResults large,
                                                      ExecutionContext contextOrNull) {
    // if one is a set and one is a bag,
    // then treat the set like a bag (and return a bag)
    boolean smallModifiable = small.isModifiable() && (isBag(small) || !isBag(large));
    boolean largeModifiable = large.isModifiable() && (isBag(large) || !isBag(small));
    if (smallModifiable) {
      try {
        for (Iterator itr = small.iterator(); itr.hasNext(); ) {
          Object element = itr.next();
          int count = large.occurrences(element);
          if (small.occurrences(element) > count) { // bag intersection: only retain smaller number of dups
            itr.remove();
          }
        }
        return small;
      }
      catch (UnsupportedOperationException e1) {
        // didn't succeed because small is actually unmodifiable
      }
    }
    if (largeModifiable) {
      try {
        for (Iterator itr = large.iterator(); itr.hasNext(); ) {
          Object element = itr.next();
          int count = small.occurrences(element);
          if (large.occurrences(element) > count) { // bag intersection: only retain smaller number of dups
            itr.remove();
          }
        }
        return large;
      }
      catch (UnsupportedOperationException e2) {
        // didn't succeed because large is actually unmodifiable
      }
    }
    
    SelectResults rs ;
    if(contextOrNull != null) {
        rs =  contextOrNull.isDistinct() ? new ResultsSet(small) :new ResultsBag(small,
            contextOrNull.getCachePerfStats());
    }else {
      rs =new ResultsBag(small,null);
    }
    
    for (Iterator itr = rs.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      int count = large.occurrences(element);
      if (rs.occurrences(element) > count) { // bag intersection: only retain smaller number of dups
        itr.remove();
      }
    }
    return rs;
  }

  /** collections are passed in from smallest to largest */
  // assume we're dealing with bags and/or sets here, number of occurrences in the
  // union should be the sum of the occurrences in the two bags
  //Asif: Is this Ok? There may be tuples which are actually common to both set so
  // union in such cases should not increase count. right.?
  
  private static SelectResults sizeSortedUnion(SelectResults small,
                                               SelectResults large,
                                               ExecutionContext contextOrNull) {
    // if one is a set and one is a bag,
    // then treat the set like a bag (and return a bag)
    boolean smallModifiable = small.isModifiable() && (isBag(small) || !isBag(large));
    boolean largeModifiable = large.isModifiable() && (isBag(large) || !isBag(small));
    if (largeModifiable) {
      try {
        for (Iterator itr = small.iterator(); itr.hasNext(); ) {
          Object element = itr.next();
          int count = small.occurrences(element);
          if (large.occurrences(element) < count) {
            large.add(element);
          }
        }
        return large;
      }
      catch (UnsupportedOperationException e1) {
        // didn't succeed because large is actually unmodifiable
      }
    }
    if (smallModifiable) {
      try {
        for (Iterator itr = large.iterator(); itr.hasNext(); ) {
          Object element = itr.next();
          int count = large.occurrences(element);
          if (small.occurrences(element) < count) {
            small.add(element);
          }
        }
        return small;
      }
      catch (UnsupportedOperationException e2) {
        // didn't succeed because small is actually unmodifiable
      }
    }
    SelectResults rs;
    if(contextOrNull != null) {
      rs =  contextOrNull.isDistinct() ? new ResultsSet(large) :new ResultsBag(large,
          contextOrNull.getCachePerfStats());
    }else {
      rs =new ResultsBag(large, null);
    }
    
   for (Iterator itr = small.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      rs.add(element);
    }
    return rs;
  }

  /**
   * This function returns a list of runtime iterators in current scope which
   * are exclusively dependent on given independent RuntimeIterators. The order
   * of dependent iterators in the List is based on the order of independent
   * Iterators present in the array . For each group the first iterator is its
   * independent iterator
   * 
   * @param indpndntItrs array of independent RuntimeIterators
   * @param context
   */
  public static List getDependentItrChainForIndpndntItrs(
      RuntimeIterator[] indpndntItrs, ExecutionContext context) {
    List ret = new ArrayList();
    for (int k = 0; k < indpndntItrs.length; ++k) {
      ret.addAll(context
          .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(indpndntItrs[k]));
    }
    return ret;
  } 
  

  /**
   * Asif : This util function does a cartesian of the array of SelectResults
   * object , expanding the resultant set to the number of iterators passed in
   * expansionList. The position of the iterator fields in the final result is
   * governed by the order of RuntimeIterators present in the finalList. If any
   * condition needs to be evaluated during cartesian , it can be passed as
   * operand
   * 
   * @param results Array of SelectResults object which are to be cartesianed
   * @param itrsForResultFields A two dimensional array of RuntimeIterator. Each
   *          row of this two dimensional RuntimeIterator array , maps to a
   *          SelectResults object in the results array. Thus the 0th row of two
   *          dimensional RuntimeIterator array will map to the 0th element of
   *          the SelectResults array. The order of RuntimeIterator in a row
   *          will map to the fields in the SelectResults object. The 0th
   *          RuntimeIterator will map to the 0th field of the corresponding
   *          SelectResults object. The number of rows in the two
   *          dimensional array of RuntimeIterator should be equal to the size
   *          of array of SelectResults object passed and the number of
   *          RuntimeIterators in each row should be equal to the number of
   *          fields in the SelectResults object . The SelectResults object
   *          itself may be a ResultBag object or a StructBag object.
   * 
   * @param expansionList List containing RunimeIterators to which the final
   *          Results should be expanded to.
   * @param finalList List containing RuntimeIterators which define the number
   *          of fields to be present in the resultant SelectResults and their
   *          relative positions. The Runtime Iterators present in the List
   *          should be either available in the expansion List or should be
   *          present in each row of the two dimensional RuntimeIterator array.
   * 
   * @param context ExecutionContext object
   * @param operand The CompiledValue which needs to be iter evaluated during
   *          cartesian. Only those tuples will be selected in the final Result
   *          for which oerand evaluates to true.
   * @return SelectResults object representing the final result of the cartesian
   * @throws FunctionDomainException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   * @throws QueryInvocationTargetException
   *  
   */
  public static SelectResults cartesian(SelectResults[] results,
      RuntimeIterator[][] itrsForResultFields, List expansionList,
      List finalList, ExecutionContext context, CompiledValue operand)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    SelectResults returnSet = null;
    if (finalList.size() == 1) {
      ObjectType type = ((RuntimeIterator) finalList.iterator().next())
          .getElementType();
      if (type instanceof StructType) {
        returnSet = QueryUtils.createStructCollection(context,(StructTypeImpl) type) ;
      }
      else {
        return QueryUtils.createResultCollection(context, type);
      }
    }
    else {
      StructType structType = createStructTypeForRuntimeIterators(finalList);
      returnSet = QueryUtils.createStructCollection(context, structType);
    }
    ListIterator expnItr = expansionList.listIterator();
    // RuntimeIterator levelExpnItr =
    // expnItr.hasNext()?(RuntimeIterator)expnItr.next():null;
    doNestedIterations(0, returnSet, results, itrsForResultFields, finalList,
        expnItr, (results.length + expansionList.size()), context, operand);
    return returnSet;
  }

  //TODO:Asif :Optimize the function further in terms of reducing the
  // parameters passed in the function, if possible
  private static void doNestedIterations(int level, SelectResults returnSet,
      SelectResults[] results, RuntimeIterator[][] itrsForResultFields,
      List finalItrs, ListIterator expansionItrs, int finalLevel,
      ExecutionContext context, CompiledValue operand)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    if (level == finalLevel) {
      //end recusrion
      boolean select = true;
      if (operand != null) {
        select = applyCondition(operand, context);
      }
      Iterator itr = finalItrs.iterator();
      int len = finalItrs.size();
      if (len > 1) {
        Object values[] = new Object[len];
        int j = 0;
        while (itr.hasNext()) {
          values[j++] = ((RuntimeIterator) itr.next()).evaluate(context);
        }
        if (select) {
          
            ((StructFields) returnSet).addFieldValues(values);
          
        }
      }
      else {
        if (select)
            returnSet.add(((RuntimeIterator) itr.next()).evaluate(context));
      }
    }
    else if (level < results.length) {
      SelectResults individualResultSet = results[level];
      RuntimeIterator[] itrsForFields = itrsForResultFields[level];
      int len = itrsForFields.length;
      Iterator itr = individualResultSet.iterator();
      while (itr.hasNext()) {
        // Check if query execution on this thread is canceled.
        QueryMonitor.isQueryExecutionCanceled();
        Object value = itr.next();
        if (len == 1) {
          // Asif : this means we have a ResultSet
          itrsForFields[0].setCurrent(value);
        }
        else {
          Struct struct = (Struct) value;
          Object fieldValues[] = struct.getFieldValues();
          int size = fieldValues.length;
          for (int i = 0; i < size; ++i) {
            itrsForFields[i].setCurrent(fieldValues[i]);
          }
        }
        doNestedIterations(level + 1, returnSet, results, itrsForResultFields,
            finalItrs, expansionItrs, finalLevel, context, operand);
      }
    }
    else {
      RuntimeIterator currLevel = (RuntimeIterator) expansionItrs.next();
      SelectResults c = currLevel.evaluateCollection(context);
      if (c == null) {
        //TODO:Asif Check this out
        expansionItrs.previous();
        return;
      }
      Iterator cIter = c.iterator();
      while (cIter.hasNext()) {
        currLevel.setCurrent(cIter.next());
        doNestedIterations(level + 1, returnSet, results, itrsForResultFields,
            finalItrs, expansionItrs, finalLevel, context, operand);
      }
      expansionItrs.previous();
    }
  }

  public static boolean applyCondition(CompiledValue operand,
      ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    if(operand == null) return true;
    Object result = operand.evaluate(context);
    if (result instanceof Boolean) {
      return ((Boolean) result).booleanValue();
    }
    else if (result != null && result != QueryService.UNDEFINED) {
      throw new TypeMismatchException(LocalizedStrings.QueryUtils_ANDOR_OPERANDS_MUST_BE_OF_TYPE_BOOLEAN_NOT_TYPE_0.toLocalizedString(result.getClass().getName()));
    }
    else {
      return false;
    }
  }

  //TODO:Asif This function is used to do cartesian of index resultset while
  // expanding/cutting down index resultset
  // with the intermediate resultset
  //TODO :Asif :Explain the parameters& Unit test it
  private static void mergeRelationshipIndexResultsWithIntermediateResults(
      SelectResults returnSet, SelectResults[] intermediateResults /*
                                                                    * Asif This
                                                                    * should be
                                                                    * a single
                                                                    * element
                                                                    * array
                                                                    */, RuntimeIterator[][] itrsForIntermediateResults /*
                                                           * Asif This should be
                                                           * a two dimensional
                                                           * array but with only
                                                           * one row
                                                           */, Object[][] indexResults, RuntimeIterator[][] indexFieldToItrsMapping,
      ListIterator expansionListItr, List finalItrs, ExecutionContext context,
      List[] checkList, CompiledValue iterOps,
      IndexCutDownExpansionHelper icdeh[], int level, int maxExpnCartesianDepth)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
//    Object[] checkFields = null;
//    int len = indexFieldToItrsMapping[level].length;
//    RuntimeIterator rItr = null;
    int resultSize = indexResults[level].length;
    //TODO ASif : Since this is constant for a given merge call, pass it as a
    // parameter to
    //the function rather than calling it every time
    for (int j = 0; j < resultSize; ++j) {
      if (setIndexFieldValuesInRespectiveIterators(indexResults[level][j],
          indexFieldToItrsMapping[level], icdeh[level])) {
        if (level == (indexResults.length - 1)) {
          //Asif :Set the values in the Intermedaite Resultset
          doNestedIterations(0, returnSet, intermediateResults,
              itrsForIntermediateResults, finalItrs, expansionListItr,
              maxExpnCartesianDepth, context, iterOps);
        }
        else {
          mergeRelationshipIndexResultsWithIntermediateResults(returnSet,
              intermediateResults, itrsForIntermediateResults, indexResults,
              indexFieldToItrsMapping, expansionListItr, finalItrs, context,
              checkList, iterOps, icdeh, level + 1, maxExpnCartesianDepth);
          if (icdeh[level + 1].cutDownNeeded) {
            icdeh[level + 1].checkSet.clear();
          }
        }
      }
    }
  }

  //TODO:Asif : Test the function & write expnanation of the parameters
  private static void mergeAndExpandCutDownRelationshipIndexResults(
      Object[][] values, SelectResults result,
      RuntimeIterator[][] indexFieldToItrsMapping,
      ListIterator expansionListIterator, List finalItrs,
      ExecutionContext context, List[] checkList, CompiledValue iterOps,
      IndexCutDownExpansionHelper icdeh[], int level)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
//    int len = indexFieldToItrsMapping[level].length;
//    RuntimeIterator rItr = null;
    int resultSize = values[level].length;
    int limit = getLimitValue(context);
    //stops recursion if limit has already been met
    if (limit != -1 && result.size() >= limit) {
      return;
    }   
    for (int j = 0; j < resultSize; ++j) {
      // Check if query execution on this thread is canceled.
      QueryMonitor.isQueryExecutionCanceled();

      if (setIndexFieldValuesInRespectiveIterators(values[level][j],
          indexFieldToItrsMapping[level], icdeh[level])) {
        if (level == (values.length - 1)) {
          doNestedIterationsForIndex(expansionListIterator.hasNext(), result,
              finalItrs, expansionListIterator, context, iterOps, limit, null);
          if (limit != -1 && result.size() >= limit) {
            break;
          }     
        }
        else {
          mergeAndExpandCutDownRelationshipIndexResults(values, result,
              indexFieldToItrsMapping, expansionListIterator, finalItrs,
              context, checkList, iterOps, icdeh, level + 1);
          if (icdeh[level + 1].cutDownNeeded) {
            icdeh[level + 1].checkSet.clear();
          }
        }
      }
    }
  }

  //TODO:Asif : Explain the function & write test cases. A boolean false means
  // ,
  // by pass i.e the set value to be ignored
  
  //End result if we have not already expanded is that we have created a new struct and added to a set to prevent future expansions of the same object
  //It also advances the current object for the iterator.
  private static boolean setIndexFieldValuesInRespectiveIterators(Object value,
      RuntimeIterator[] indexFieldToItrsMapping,
      IndexCutDownExpansionHelper icdeh) {
    Object[] checkFields = null;
    boolean select = true;
    int len = indexFieldToItrsMapping.length;
    RuntimeIterator rItr = null;
    if (len == 1) {
      //Asif : this means we have a ResultSet
      Support
          .Assert(
              !icdeh.cutDownNeeded,
              "If the index fields to iter mapping is of of size 1 then cut down cannot occur");
      indexFieldToItrsMapping[0].setCurrent(value);
    }
    else {
      Struct struct = (Struct) value;
      Object fieldValues[] = struct.getFieldValues();
      int size = fieldValues.length;
      if (icdeh.cutDownNeeded) checkFields = new Object[icdeh.checkSize];
      // Object values[] = new Object[numItersInResultSet];
      int j = 0;
      for (int i = 0; i < size; i++) {
        rItr = indexFieldToItrsMapping[i];
        if (rItr != null) {
          rItr.setCurrent(fieldValues[i]);
          if (icdeh.cutDownNeeded) {
            checkFields[j++] = fieldValues[i];
          }
        }
      }
      if (icdeh.cutDownNeeded) {
        Object temp = null;
        if (icdeh.checkSize == 1) {
          temp = checkFields[0];
        }
        else {
          temp = new StructImpl((StructTypeImpl) icdeh.checkType, checkFields);
        }
        if (icdeh.checkSet.contains(temp)) {
          select = false;
        }
        else {
          icdeh.checkSet.add(temp);
        }
      }
    }
    return select;
  }

  //creates the returned set and then calls other methods to do actual work
  private static SelectResults cutDownAndExpandIndexResults(
      SelectResults result, RuntimeIterator[] indexFieldToItrsMapping,
      List expansionList, List finalItrs, ExecutionContext context,
      List checkList, CompiledValue iterOps, IndexInfo theFilteringIndex) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    SelectResults returnSet = null;
    boolean useLinkedDataStructure = false; 
    boolean nullValuesAtStart = true;
    Boolean orderByClause = (Boolean)context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
    if(orderByClause != null && orderByClause.booleanValue()) {
      List orderByAttrs = (List)context.cacheGet(CompiledValue.ORDERBY_ATTRIB);        
      useLinkedDataStructure = orderByAttrs.size()==1; 
      nullValuesAtStart = !((CompiledSortCriterion)orderByAttrs.get(0)).getCriterion();
    }
    if (finalItrs.size() == 1) {
      ObjectType resultType = ((RuntimeIterator) finalItrs.iterator().next()).getElementType();
      if (useLinkedDataStructure) {
        returnSet = context.isDistinct() ? new LinkedResultSet(resultType): 
          new SortedResultsBag(resultType, nullValuesAtStart); 
      } else {
        returnSet =QueryUtils.createResultCollection(context, resultType);
      }
    
    }
    else {
      StructTypeImpl resultType =  (StructTypeImpl) createStructTypeForRuntimeIterators(finalItrs);
      if(useLinkedDataStructure) {
        returnSet = context.isDistinct() ? new LinkedStructSet(resultType)
        : new SortedResultsBag<Struct>((StructTypeImpl)resultType, nullValuesAtStart);
      }else {
        returnSet = QueryUtils.createStructCollection(context, resultType);
      }
      
    }
    cutDownAndExpandIndexResults(returnSet, result, indexFieldToItrsMapping,
        expansionList, finalItrs, context, checkList, iterOps, theFilteringIndex);
    return returnSet;
  }

  //TODO:Asif Explain the parameters passed
  private static void cutDownAndExpandIndexResults(SelectResults returnSet,
      SelectResults result, RuntimeIterator[] indexFieldToItrsMapping,
      List expansionList, List finalItrs, ExecutionContext context,
      List checkList, CompiledValue iterOps, IndexInfo theFilteringIndex) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
//    Object[] checkFields = null;
    IndexCutDownExpansionHelper icdeh =
                                new IndexCutDownExpansionHelper(checkList,
                                                                context);
    int len = indexFieldToItrsMapping.length;
    // don't call instanceof ResultsBag here, since a StructBag is a subtype of ResultsBag
    if (result.getClass() == ResultsBag.class) {
      Support.Assert(len == 1, "The array size of iterators should be 1 here, but got "
                     + len);
    }
    Iterator itr = result.iterator();
    ListIterator expansionListIterator = expansionList.listIterator();
    int limit = getLimitValue(context);
    
    while (itr.hasNext()) {
      DerivedInfo derivedInfo = null;
      if (IndexManager.JOIN_OPTIMIZATION) {
        derivedInfo = new DerivedInfo();
        derivedInfo.setExpansionList(expansionList);
      }
      Object value = itr.next();
      if (setIndexFieldValuesInRespectiveIterators(value,  
          indexFieldToItrsMapping, icdeh)) {  //does that mean we don't get dupes even if they exist in the index?
        //         DO NESTED LOOPING
        if (IndexManager.JOIN_OPTIMIZATION) {
          derivedInfo.computeDerivedJoinResults(theFilteringIndex, context, iterOps);
        }  
        doNestedIterationsForIndex(expansionListIterator.hasNext(), returnSet,
            finalItrs, expansionListIterator, context, iterOps, limit, derivedInfo.derivedResults);
        if (limit != -1 && returnSet.size() >= limit) {
          break;
        }     
      }
    }
  }
  
  //returns the limit value from the context.  This was set in CompiledSelect evaluate
  //We do not apply limit if we have an order by attribute at this time
  //it may be possible but we need better understanding of when ordering is taking place
  //If it's at the index level, we may be able to apply limits at this point
  //however a lot of the code in this class is fragile/unreadable/hard to maintain
  private static int getLimitValue(ExecutionContext context) {
    int limit = -1;
    if (context.cacheGet(CompiledValue.ORDERBY_ATTRIB) == null) {
      limit = ((Integer) context.cacheGet(CompiledValue.RESULT_LIMIT)) != null?((Integer) context.cacheGet(CompiledValue.RESULT_LIMIT)).intValue(): -1;
    }   
    return limit;
  }
  
  public static CompiledID getCompiledIdFromPath(CompiledValue path) {
    int type = path.getType();
    if (type == OQLLexerTokenTypes.Identifier) {
      return (CompiledID) path;
    } 
    return getCompiledIdFromPath(path.getReceiver());
  }

  //Add comments
  private static void doNestedIterationsForIndex(boolean continueRecursion,
      SelectResults resultSet, List finalItrs, ListIterator expansionItrs,
      ExecutionContext context, CompiledValue iterOps, int limit, Map<String, SelectResults> derivedResults)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    
    if (!continueRecursion) {
      //end recusrion
      Iterator itr = finalItrs.iterator();
      int len = finalItrs.size();
      boolean select = true;
      if (iterOps != null) {
        select = applyCondition(iterOps, context);
      }
      if (len > 1) {
        boolean isOrdered = resultSet.getCollectionType().isOrdered();
        StructTypeImpl elementType = (StructTypeImpl)resultSet.getCollectionType().getElementType();
        //TODO:Asif Optimize the LinkedStructSet implementation so that
        // Object[] can be added rather than Struct
        
        Object values[] = new Object[len];
        int j = 0;
        //creates tuple
        while (itr.hasNext()) {
          // Check if query execution on this thread is canceled.
          QueryMonitor.isQueryExecutionCanceled();

          values[j++] = ((RuntimeIterator) itr.next()).evaluate(context);
        }
        if (select) {
          if(isOrdered) {
            //((LinkedStructSet) resultSet).add(new StructImpl(elementType, values));
            // Can be LinkedStructSet or SortedResultsBag ( containing underlying LinkedHashMap)
            resultSet.add(new StructImpl(elementType, values));
          }else {
            
              ((StructFields) resultSet).addFieldValues(values);
            
          }
        }
      }
      else {
        if (select)
           resultSet.add(((RuntimeIterator) itr.next()).evaluate(context));
      }
    }
    else {
      RuntimeIterator currentLevel = (RuntimeIterator) expansionItrs.next();
      SelectResults c = null;
      // Calculate the key to find the derived join results. If we are a non nested lookup it will be a Compiled Region otherwise it will be a CompiledPath that
      // we can extract the id from. In the end the result will be the alias which is used as a prefix
      CompiledValue collectionExpression = currentLevel.getCmpIteratorDefn().getCollectionExpr();
      String key = null;
      boolean useDerivedResults = true;
      if (currentLevel.getCmpIteratorDefn().getCollectionExpr().getType() == OQLLexerTokenTypes.RegionPath) {
        key = currentLevel.getCmpIteratorDefn().getName() + ":" + currentLevel.getDefinition();
      } else if (currentLevel.getCmpIteratorDefn().getCollectionExpr().getType() == OQLLexerTokenTypes.LITERAL_select) {
        useDerivedResults = false;
      } else {
        key = getCompiledIdFromPath(currentLevel.getCmpIteratorDefn().getCollectionExpr()).getId() + ":" + currentLevel.getDefinition();
      }
      if (useDerivedResults && derivedResults != null && derivedResults.containsKey(key)) {
        c = derivedResults.get(key);
      } else {
        c = currentLevel.evaluateCollection(context);
      }
      //  RuntimeIterator next = expansionItrs.hasNext() ?
      // (RuntimeIterator)expansionItrs.next() : null;
      if (c == null) {
        //TODO:Asif Check this out
        expansionItrs.previous();
        return;
      }
      Iterator cIter = c.iterator();
      while (cIter.hasNext()) {
        // Check if query execution on this thread is canceled.
        QueryMonitor.isQueryExecutionCanceled();

        currentLevel.setCurrent(cIter.next());
        doNestedIterationsForIndex(expansionItrs.hasNext(), resultSet,
            finalItrs, expansionItrs, context, iterOps, limit, derivedResults);
        if (limit != -1 && resultSet.size() >= limit) {
          break;
        }     
      }
      expansionItrs.previous();
    }
  }

  /**
   * Ketan/Asif : This function will evaluate the starting CompiledValue for a
   * given CompliedValue. The value returned will always be either the original
   * CompiledValue, or a CompiledID, or a CompiledRegion, or a
   * CompiledBindArgument, or a CompiledOperation. The ExecutionContext passed can
   * be null. If it is null, then for a CompiledOperation , if supposed to get
   * resolved implicitly, will have its receiver as null. This is because in normal
   * cases , a CompiledID marks the termination, but in case of
   * CompiledOperation this is not the case
   * 
   * @param expr CompiledValue object
   * @return CompiledValue
   */
  public static CompiledValue obtainTheBottomMostCompiledValue(
      CompiledValue expr) {
    boolean toContinue = true;
    int exprType = expr.getType();
    while (toContinue) {
      switch (exprType) {
        case OQLLexerTokenTypes.RegionPath:
          toContinue = false;
          break;
        case OQLLexerTokenTypes.METHOD_INV:
          CompiledOperation operation = (CompiledOperation) expr;
          expr = operation.getReceiver(null/*
                                            * pass the ExecutionContext as null,
                                            * thus never implicitly resolving to
                                            * RuntimeIterator
                                            */);
          if (expr == null) {
            expr = operation;
            toContinue = false;
          }
          break;
        case CompiledValue.PATH:
          expr = ((CompiledPath) expr).getReceiver();
          break;
        case OQLLexerTokenTypes.TOK_LBRACK:
          expr = ((CompiledIndexOperation) expr).getReceiver();
          break;
        default:
          toContinue = false;
          break;
      }
      if (toContinue) exprType = expr.getType();
    }
    return expr;
  }

  /**
   * Asif : This function creates a StructType using Internal IDs of the
   * iterators as the field names for the StructType. It should be invoked iff
   * the iterators size is greater than 1
   * 
   * @param runTimeIterators List of RuntimeIterator objects
   * @return StructType object
   *  
   */
  public static StructType createStructTypeForRuntimeIterators(
      List runTimeIterators) {
    Support
        .Assert(runTimeIterators.size() > 1,
            "The number of Iterators passed should be greater than 1 to create a structSet");
    int len = runTimeIterators.size();
    String fieldNames[] = new String[len];
    String[] indexAlternativeFieldNames = new String[len];
    ObjectType fieldTypes[] = new ObjectType[len];
    //Asif : use an Iterator as the chances are that we will be sending
    //LinkedList rather than ArrayList
    Iterator itr = runTimeIterators.iterator();
    int i = 0;
    while (itr.hasNext()) {
      RuntimeIterator iter = (RuntimeIterator) itr.next();
      fieldNames[i] =  iter.getInternalId();
      indexAlternativeFieldNames[i] = iter.getIndexInternalID();
      fieldTypes[i++] = iter.getElementType();
    }
    StructTypeImpl type = new StructTypeImpl(fieldNames, indexAlternativeFieldNames, fieldTypes);
    return type;
  }

  /**
   * Asif :This function returns the ultimate independent RuntimeIterators of
   * current scope on which the CompiledValue passed is dependent upon. This
   * does not return the RuntimeIterators on which it may be dependent but are
   * not part of the current scope. If no such RuntimeIterator exists it returns
   * empty set.
   * 
   * @param compiledValue CompiledValue Object
   * @param context ExecutionContextobject
   * @return Set containing the ultimate independent RuntimeIterators of current
   *         scope
   *  
   */
  public static Set getCurrentScopeUltimateRuntimeIteratorsIfAny(
      CompiledValue compiledValue, ExecutionContext context) {
    HashSet set = new HashSet();
    context.computeUtlimateDependencies(compiledValue, set);
    //if (set.size() != 1) return null;
    Iterator iter = set.iterator();
    while (iter.hasNext()) {
      RuntimeIterator rIter = (RuntimeIterator) iter.next();
      if (rIter.getScopeID() != context.currentScope().getScopeID()/*context.getScopeCount()*/) iter.remove();
    }
    return set;
  }

  /**
   * Asif :Returns the pair of RangeIndexes available for a composite condition (
   * equi join across the region). It will either return two indexes or will
   * return null. *
   * 
   * @param lhs One of the operands of the equi-join condition
   * @param rhs The other operand of the equi-join condition
   * @param context ExecutionContext object
   * @param operator The operator which necesarily has to be an equality ( ' = ' )
   * @return An array of IndexData object with 0th IndexData for the lhs operand &
   *         1th object for rhs operad
   * @throws AmbiguousNameException
   * @throws TypeMismatchException
   *  
   */
  static IndexData[] getRelationshipIndexIfAny(CompiledValue lhs,
      CompiledValue rhs, ExecutionContext context, int operator)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    if (operator != OQLLexerTokenTypes.TOK_EQ) return null;// Operator must be
    // '='
    IndexData lhsIndxData = QueryUtils.getAvailableIndexIfAny(lhs, context,
        false /* Do not use PrimaryKey Index */);// findOnlyFunctionalIndex.
    if (lhsIndxData == null) return null;
    IndexData rhsIndxData = QueryUtils.getAvailableIndexIfAny(rhs, context,
        false /* Do not use PrimaryKey Index */);// findOnlyFunctionalIndex.
    if (rhsIndxData == null) {
      // release the lock held on lhsIndex as it will not be used
      Index index = lhsIndxData.getIndex();
      Index prIndex = ((AbstractIndex) index).getPRIndex();
      if (prIndex != null) {
        ((PartitionedIndex) prIndex).releaseIndexReadLockForRemove();
      } else {      
         ((AbstractIndex) index).releaseIndexReadLockForRemove();
      }
      return null;
    }
    Index lhsIndx = lhsIndxData.getIndex();
    Index rhsIndx = rhsIndxData.getIndex();
    if (((IndexProtocol) lhsIndx).isValid()
       && ((IndexProtocol) rhsIndx).isValid()) { 
       return new IndexData[] {lhsIndxData, rhsIndxData}; 
    }
    return null;
  }

  /**
   * Asif : Gets an Index available for the condition
   * 
   * @param cv Condition on which index needs to be obtained
   * @param context ExecutionContext object
   * @param operator int argument identifying the type of operator
   * @return IndexData object
   * @throws AmbiguousNameException
   * @throws TypeMismatchException
   *  
   */
  static IndexData getAvailableIndexIfAny(CompiledValue cv,
      ExecutionContext context, int operator) throws AmbiguousNameException,
      TypeMismatchException, NameResolutionException {
    // If operator is = or != then first search for PRIMARY_KEY Index
    boolean usePrimaryIndex = (operator == OQLLexerTokenTypes.TOK_EQ || operator == OQLLexerTokenTypes.TOK_NE);
    return getAvailableIndexIfAny(cv, context, usePrimaryIndex);
  }

  //TODO:Asif : Provide description of the function.
  private static IndexData getAvailableIndexIfAny(CompiledValue cv,
      ExecutionContext context, boolean usePrimaryIndex)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    Set set = new HashSet();
    context.computeUtlimateDependencies(cv, set);
    if (set.size() != 1) return null;
    RuntimeIterator rIter = (RuntimeIterator) set.iterator().next();
    String regionPath = null;
    // An Index is not available if the ultimate independent RuntimeIterator is
    // of different scope or if the underlying
    // collection is not a Region
    if (rIter.getScopeID() != context.currentScope().getScopeID() /*context.getScopeCount()*/
        || (regionPath = context
            .getRegionPathForIndependentRuntimeIterator(rIter)) == null) {
      return null;
    }
    //  Asif : The independent iterator is added as the first element
    List groupRuntimeItrs = context
        .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(rIter);
    String[] definitions = new String[groupRuntimeItrs.size()];
    Iterator iterator = groupRuntimeItrs.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      RuntimeIterator rIterator = (RuntimeIterator) iterator.next();
      definitions[i++] = rIterator.getDefinition();
      
    }
    
//    StringBuffer sb = new StringBuffer();
//    cv.generateCanonicalizedExpression(sb, context);
    IndexData indexData = IndexUtils.findIndex(regionPath, definitions, cv,
        "*", context.getCache(), usePrimaryIndex, context);
    if (indexData != null) {
      if (logger.isDebugEnabled() ) {
        logger.debug("Indexed expression for indexed data : {}  for region : {}", indexData.getIndex().getCanonicalizedIndexedExpression(), regionPath);
      }
    }
    
    return indexData;
  }

  /**
   * Asif : Conditions the raw index result obtained on a non composite
   * condition ( i.e a condition with a format of variable = constant . A
   * constant may be either a CompiledLiteral or an expression which is
   * completely dependent on iterators other than the current scope. The
   * variable is a path expression which is completely dependent on iterators
   * belonging only to a single region ( i.e iterators belonging to a Group of
   * iterators only dependent on a single indpendent iterator for the region).
   * The raw index result is appropriately expanded / cutdown with evaluation of
   * iter operand if any , StructType/ObjectType appropriately set, Shuffling of
   * the fields appropriately done, such that the final result is compatible, in
   * terms of the position and names of the fields of SelectResults( StructBag) ,
   * with the Iterators of the query from clause ( if complete expansion flag is
   * true) or the chain of iterators identified by the indpendent iterator for
   * the group.
   * 
   * @param indexResults The raw index results which may be a ResultBag object
   *          or an StructBag object
   * @param indexInfo IndexInfo object containing data such as match level & the
   *          mapping of the position of Runtime Iterators of the group to the
   *          position of the corresponding field in the index result (
   *          StructBag)
   * @param context ExecutionContext object
   * @param indexFieldsSize The number of fields contained in the raw index
   *          resultset
   * @param completeExpansion The boolean indicating whether the index resultset
   *          needs to be expanded to the query from clause level ( i.e top
   *          level)
   * @param iterOperands The CompiledValue representing the iter operand which
   *          needs to be evaluated during conditioning of index resultset
   * @param grpIndpndntItr An Array of Independent Iterators representing their
   *          respective groups. The conditioned Index Resultset will be created
   *          as per the chain of dependent iterators for each group.
   * @return SelectResults object representing the conditioned Results
   * @throws FunctionDomainException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   * @throws QueryInvocationTargetException
   *  
   */
  static SelectResults getconditionedIndexResults(SelectResults indexResults,
      IndexInfo indexInfo, ExecutionContext context, int indexFieldsSize,
      boolean completeExpansion, CompiledValue iterOperands,
      RuntimeIterator[] grpIndpndntItr) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    if (!completeExpansion && grpIndpndntItr != null
        && grpIndpndntItr.length > 1) {
      //Asif : If for a Single Base Collection Index usage we are having
      // independent
      // group of iterators with size greater than 1 , that implies it is being
      // invoked
      // from CompositeGroupJunction with a single GroupJunction present in it.
      //If it so happens that Complete expansion is false, this implies that we
      // need to expand
      //it to CompositeGroupJunction level. We will esnure in
      // CompositeGroupJunction that
      // we pass an array of independent iterators of size > 1 only if complete
      // expansion is false
      IndexConditioningHelper ich = new IndexConditioningHelper(indexInfo,
          context, indexFieldsSize, completeExpansion, iterOperands, null);
      // We will definitely need shuffling as a CompositeGroupJunction itself
      // indicates
      // that there will be at least one independent group to which we need to
      // expand to
      ich.finalList = getDependentItrChainForIndpndntItrs(grpIndpndntItr,
          context);
      //Add the iterators of remaining independent grp to the expansion list
      List newExpList = new ArrayList();
      int len = grpIndpndntItr.length;
      RuntimeIterator tempItr = null;
      for (int i = 0; i < len; ++i) {
        tempItr = grpIndpndntItr[i];
        if (tempItr != ich.indpndntItr) {
          newExpList.addAll(context
              .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(tempItr));
        }
      }
      newExpList.addAll(ich.expansionList);
      ich.expansionList = newExpList;
      QueryObserver observer = QueryObserverHolder.getInstance();
      try {
        observer.beforeCutDownAndExpansionOfSingleIndexResult(indexInfo._index,
            indexResults);
        indexResults = QueryUtils.cutDownAndExpandIndexResults(indexResults,
            ich.indexFieldToItrsMapping, ich.expansionList, ich.finalList,
            context, ich.checkList, iterOperands, indexInfo);
      }
      finally {
        observer.afterCutDownAndExpansionOfSingleIndexResult(indexResults);
      }
    }
    else {
      IndexConditioningHelper ich = new IndexConditioningHelper(indexInfo,
          context, indexFieldsSize, completeExpansion, iterOperands,
          grpIndpndntItr != null ? grpIndpndntItr[0] : null);
      if (ich.shufflingNeeded) {
        QueryObserver observer = QueryObserverHolder.getInstance();
        try {
          observer.beforeCutDownAndExpansionOfSingleIndexResult(
              indexInfo._index, indexResults);
          indexResults = QueryUtils.cutDownAndExpandIndexResults(indexResults,
              ich.indexFieldToItrsMapping, ich.expansionList, ich.finalList,
              context, ich.checkList, iterOperands, indexInfo);
        }
        finally {
          observer.afterCutDownAndExpansionOfSingleIndexResult(indexResults);
        }
      }
      else if (indexInfo.mapping.length > 1) {
        indexResults.setElementType(ich.structType);
      }
    }
    return indexResults;
  }

  /**
   * Asif :This function is used to evaluate a filter evaluatable
   * CompositeCondition(ie Range Indexes available on both LHS & RHS
   * operands).This function is invoked from AND junction evaluation of
   * CompositeGroupJunction. It expands the intermediate resultset passed , to
   * the level of groups determined by the LHS & RHS operand, using the range
   * indexes. It is possible that the group of iterators for an operand of
   * condition already exists in the intermediate resultset passed. In such
   * situation, the intermediate resultset is iterated & the operand ( whose group
   * of iterators are available in the intermediate resultset ) is evaluated.
   * For each such evaluated value , the other operand's Range Index is queried &
   * the Range Index's results are appropriately expanded & cut down & a final
   * tuple obtained( which includes the previously existing fields of
   * intermediate resultset). The array of independent iterators passed from the
   * Composite Group junction will be null, except for the final condition (
   * subject to the fact that complete expansion flag is false. Otherwise even
   * for final condition , the array will be null) as that array will be used to
   * get the final position of iterators in the resultant StructBag
   * 
   * @param intermediateResults SelectResults object containing the intermediate
   *          resultset obtained by evaluation of previous filter evaluatable
   *          composite conditions of the CompositeGroupJunction
   * @param indxInfo Array of IndexInfo objects ( size 2), representing the
   *          range index for the two operands of the condition
   * @param context ExecutionContext object
   * @param completeExpansionNeeded A boolean when true indicates that the final
   *          result from Composite GroupJunction needs to be evaluated to the
   *          query from clause ( top ) level.
   * @param iterOperands CompiledValue representing the conditions which are to
   *          be iter evaluated. This can exist only if instead of
   *          AllGroupJunction we have a single CompositeGroupJunction
   * @param indpdntItrs Array of RuntimeIterators representing the independent
   *          iterators of their representative groups forming the
   *          CompositeGroupJunction *
   * @return SelectResults The Result object created by evaluating the filter
   *         evaluatable condition merged with the intermediate results
   * @throws FunctionDomainException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   * @throws QueryInvocationTargetException
   *  
   */
  static SelectResults getRelationshipIndexResultsMergedWithIntermediateResults(
      SelectResults intermediateResults, IndexInfo[] indxInfo,
      ExecutionContext context, boolean completeExpansionNeeded,
      CompiledValue iterOperands, RuntimeIterator[] indpdntItrs)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    ObjectType resultType1 = indxInfo[0]._index.getResultSetType();
    int indexFieldsSize1 = resultType1 instanceof StructType ? ((StructTypeImpl) resultType1)
        .getFieldNames().length
        : 1;
    ObjectType resultType2 = indxInfo[1]._index.getResultSetType();
    int indexFieldsSize2 = resultType2 instanceof StructType ? ((StructTypeImpl) resultType2)
        .getFieldNames().length
        : 1;
    /*
     * Asif : even if th complete expansion is needed pass the flag of complete
     * expansion as false. Thus for LHS & RHS we will get the expnasionList for
     * that individual group.
     */
    IndexConditioningHelper ich1 = new IndexConditioningHelper(indxInfo[0],
        context, indexFieldsSize1,
        false/* Asif : pass it as false, irrespective of actual value */,
        iterOperands, null);
    IndexConditioningHelper ich2 = new IndexConditioningHelper(indxInfo[1],
        context, indexFieldsSize2,
        false/* Asif : pass it as false, irrespective of actual value */,
        iterOperands, null);
    //Asif : We cannot have a condition where in intermediateResultset is empty
    // or null & complete
    // expansion flag true because in that case instead of this function we should
    // have called
    //getconditionedRelationshipIndexResultsExpandedToTopOrCGJLevel
    int noOfIndexesToUse = (intermediateResults == null || intermediateResults
        .isEmpty()) ? 2 : 0;
    RuntimeIterator[] resultFieldsItrMapping = null;
    List allItrs = context.getCurrentIterators();
    IndexConditioningHelper singleUsableICH = null;
    IndexConditioningHelper nonUsableICH = null;
    List finalList = completeExpansionNeeded ? allItrs
        : indpdntItrs == null ? new ArrayList() : null;
    // Asif : the set will contain those iterators which we don't have to expand
    // to
    // either because they are already present ( because of intermediate results or
    // because index result already contains them
    Set expnItrsToIgnore = null;
    if (noOfIndexesToUse == 0) {
      //If the intermediate Resultset is not empty then check if the resultset
      // fields of intermediate
      //resultset contains any independent iterator of the current condition
      noOfIndexesToUse = 2;
      StructType stype = (StructType) intermediateResults.getCollectionType()
          .getElementType();
      String[] fieldNames = stype.getFieldNames();
      int len = fieldNames.length;
      resultFieldsItrMapping = new RuntimeIterator[len];
      String fieldName = null;
      String lhsID = ich1.indpndntItr.getInternalId();
      String rhsID = ich2.indpndntItr.getInternalId();
      for (int i = 0; i < len; ++i) {
        fieldName = fieldNames[i];
        if (noOfIndexesToUse != 0) {
          if (fieldName.equals(lhsID)) {
            --noOfIndexesToUse;
            singleUsableICH = ich2;
            nonUsableICH = ich1;
          }
          else if (fieldName.equals(rhsID)) {
            --noOfIndexesToUse;
            singleUsableICH = ich1;
            nonUsableICH = ich2;
          }
        }
        int pos = Integer.parseInt(fieldName.substring(4));
        RuntimeIterator itrPrsntInIntermdtRes = (RuntimeIterator) allItrs
            .get(pos - 1);
        resultFieldsItrMapping[i] = itrPrsntInIntermdtRes;
        //Asif : the iterator below is already present in resultset so needs to
        // be ignored for expansion
        if (completeExpansionNeeded) {
          if (expnItrsToIgnore == null) {
            expnItrsToIgnore = new HashSet();
          }
          expnItrsToIgnore.add(itrPrsntInIntermdtRes);
        }
        else if (indpdntItrs == null) {
          // Asif:We will need to know the intermediate iterators so as to know
          // the final list which will be used to obtain the correct structset.
          //But if the independent group of iterators is passed, the final list needs
          // to be calculated
          // on its basis
          finalList.add(itrPrsntInIntermdtRes);
        }
      }
      if (noOfIndexesToUse == 0) {
        singleUsableICH = null;
      }
    }
    QueryObserver observer = QueryObserverHolder.getInstance();
    if (noOfIndexesToUse == 2) {
      List data = null;
      try {
        ArrayList resultData = new ArrayList();
        observer.beforeIndexLookup(indxInfo[0]._index,
            OQLLexerTokenTypes.TOK_EQ, null);
        observer.beforeIndexLookup(indxInfo[1]._index,
            OQLLexerTokenTypes.TOK_EQ, null);
        if (context.getBucketList() != null) {
          data = queryEquijoinConditionBucketIndexes(indxInfo, context);
        } else {
          data = indxInfo[0]._index.queryEquijoinCondition(indxInfo[1]._index, context);
        }
      }
      finally {
        observer.afterIndexLookup(data);
      }
      //For sure we need to evaluate both the conditions & expand it only to
      // its own respective
      // Ignore the boolean of reshuffling needed etc for this case
      List totalExpList = new ArrayList();
      totalExpList.addAll(ich1.expansionList);
      totalExpList.addAll(ich2.expansionList);
      if (completeExpansionNeeded) {
        if (expnItrsToIgnore == null) {
          //Asif : The expnItrsToIgnore set being null at this point implies
          // that though
          // complete expansion flag is true but intermediate result set is
          // empty
          Support
              .Assert(
                  intermediateResults == null || intermediateResults.isEmpty(),
                  "expnItrsToIgnore should not have been null if the intermediate result set is not empty");
          expnItrsToIgnore = new HashSet();
        }
        expnItrsToIgnore.addAll(ich1.finalList);
        expnItrsToIgnore.addAll(ich2.finalList);
        //Asif : identify the iterators which we need to expand to
        //TODO:Asif :Make the code compact by using a common function to take
        // care of this
        int size = finalList.size();
        for (int i = 0; i < size; ++i) {
          RuntimeIterator currItr = (RuntimeIterator) finalList.get(i);
          //Asif :If the runtimeIterators of scope not present in CheckSet add
          // it
          // to
          // the expansion list
          if (!expnItrsToIgnore.contains(currItr)) {
            totalExpList.add(currItr);
          }
        }
      }
      else {
        //Asif : If the independent itrs passed is not null, this implies
        // that we are evaluating the last filterable cc of the AND junction
        // so the final resultset should have placement of iterators in the
        // order of indpendent iterators present in CGJ. Otherwise we will have
        // struct
        //set mismatch while doing intersection with GroupJunction results
        if (indpdntItrs != null) {
          finalList = getDependentItrChainForIndpndntItrs(indpdntItrs, context);
        }
        else {
          finalList.addAll(ich1.finalList);
          finalList.addAll(ich2.finalList);
        }
      }
      List[] checkList = new List[] { ich1.checkList, ich2.checkList};
      StructType stype = createStructTypeForRuntimeIterators(finalList);
      SelectResults returnSet = QueryUtils.createStructCollection(context, stype) ;
      RuntimeIterator[][] mappings = new RuntimeIterator[2][];
      mappings[0] = ich1.indexFieldToItrsMapping;
      mappings[1] = ich2.indexFieldToItrsMapping;
      List[] totalCheckList = new List[] { ich1.checkList, ich2.checkList};
      RuntimeIterator[][] resultMappings = new RuntimeIterator[1][];
      resultMappings[0] = resultFieldsItrMapping;
      Iterator dataItr = data.iterator();
      IndexCutDownExpansionHelper[] icdeh = new IndexCutDownExpansionHelper[] {
          new IndexCutDownExpansionHelper(ich1.checkList, context),
          new IndexCutDownExpansionHelper(ich2.checkList, context)};
      ListIterator expansionListIterator = totalExpList.listIterator();
      if (dataItr.hasNext()) {
        observer = QueryObserverHolder.getInstance();
        try {
          observer.beforeMergeJoinOfDoubleIndexResults(indxInfo[0]._index,
              indxInfo[1]._index, data);
          boolean doMergeWithIntermediateResults = intermediateResults != null
              && !intermediateResults.isEmpty();
          int maxCartesianDepth = totalExpList.size()
              + (doMergeWithIntermediateResults ? 1 : 0);
          while (dataItr.hasNext()) {
            //TODO:Asif Change the code in range Index so that while collecting
            // data
            // instead of creating
            //two dimensional object array , we create one dimensional Object
            // array
            // of size 2, & each elemnt
            //stores an Object array
            Object[][] values = (Object[][]) dataItr.next();
            //Asif : Before doing the cartesian of the Results , we need to
            // clear
            // the CheckSet of InexCutDownExpansionHelper. This is needed
            // because for
            // a new key , the row of sets needs to be considered fresh as
            // presence of old row in
            // checkset may cause us to wrongly skip the similar row of a set ,
            // even when the row in its entirety
            // is unique ( made by different data in the other set)
            if (doMergeWithIntermediateResults) {
              mergeRelationshipIndexResultsWithIntermediateResults(returnSet,
                  new SelectResults[] { intermediateResults}, resultMappings,
                  values, mappings, expansionListIterator, finalList, context,
                  checkList, iterOperands, icdeh, 0, maxCartesianDepth);
            }
            else {
              mergeAndExpandCutDownRelationshipIndexResults(values, returnSet,
                  mappings, expansionListIterator, finalList, context,
                  totalCheckList, iterOperands, icdeh, 0 /* Level */);
            }
            if (icdeh[0].cutDownNeeded) icdeh[0].checkSet.clear();
          }
        }
        finally {
          observer.afterMergeJoinOfDoubleIndexResults(returnSet);
        }
      }
      return returnSet;
    }
    else if (noOfIndexesToUse == 1) {
      //Asif : There exists one independent iterator in the current condition
      // which is also a part of the intermediate resultset
      // Identify the final List which will depend upon the complete expansion
      // flag
      // Identify the iterators to be expanded to, which will also depend upon
      // complete
      // expansion flag..
      List totalExpList = new ArrayList();
      totalExpList.addAll(singleUsableICH.expansionList);
      if (completeExpansionNeeded) {
        Support
            .Assert(
                expnItrsToIgnore != null,
                "expnItrsToIgnore should not have been null as we are in this block itself indicates that intermediate results was not null");
        expnItrsToIgnore.addAll(singleUsableICH.finalList);
        //Asif : identify the iterators which we need to expand to
        //TODO:Asif :Make the code compact by using a common function to take
        // care of this
        int size = finalList.size();
        for (int i = 0; i < size; ++i) {
          RuntimeIterator currItr = (RuntimeIterator) finalList.get(i);
          //Asif :If the runtimeIterators of scope not present in CheckSet add
          // it
          // to
          // the expansion list
          if (!expnItrsToIgnore.contains(currItr)) {
            totalExpList.add(currItr);
          }
        }
      }
      else {
        //        Asif : If the independent itrs passed is not null, this implies
        // that we are evaluating the last filterable cc of the AND junction
        // so the final resultset should have placement of iterators in the
        // order of indpendent iterators present in CGJ. Otherwise we will havve
        // struct
        //set mismatch while doing intersection with GroupJunction results
        if (indpdntItrs != null) {
          finalList = getDependentItrChainForIndpndntItrs(indpdntItrs, context);
        }
        else {
          finalList.addAll(singleUsableICH.finalList);
        }
      }
//      List[] checkList = new List[] { ich1.checkList, ich2.checkList};
      StructType stype = createStructTypeForRuntimeIterators(finalList);
      SelectResults returnSet = QueryUtils.createStructCollection(context, stype) ;
      //Asif :Obtain the empty resultset for the single usable index
      IndexProtocol singleUsblIndex = singleUsableICH.indxInfo._index;
      CompiledValue nonUsblIndxPath = nonUsableICH.indxInfo._path;
      ObjectType singlUsblIndxResType = singleUsblIndex.getResultSetType();
//      int singleUsblIndexFieldsSize = -1;
      SelectResults singlUsblIndxRes = null;
      if (singlUsblIndxResType instanceof StructType) {
        singlUsblIndxRes = QueryUtils.createStructCollection(context,(StructTypeImpl) singlUsblIndxResType);
//        singleUsblIndexFieldsSize = ((StructTypeImpl) singlUsblIndxResType)
//            .getFieldNames().length;
      }
      else {
        singlUsblIndxRes = QueryUtils.createResultCollection(context, singlUsblIndxResType);
//        singleUsblIndexFieldsSize = 1;
      }
      //Asif iterate over the intermediate structset
      Iterator intrmdtRsItr = intermediateResults.iterator();
      observer = QueryObserverHolder.getInstance();
      try {
        observer.beforeIndexLookup(singleUsblIndex, OQLLexerTokenTypes.TOK_EQ,
            null);
        observer.beforeIterJoinOfSingleIndexResults(singleUsblIndex,
            nonUsableICH.indxInfo._index);
        while (intrmdtRsItr.hasNext()) {
          Struct strc = (Struct) intrmdtRsItr.next();
          Object[] val = strc.getFieldValues();
          int len = val.length;
          for (int i = 0; i < len; ++i) {
            resultFieldsItrMapping[i].setCurrent(val[i]);
          }
          //TODO:Asif : Issue relevant index use callbacks to QueryObserver
          Object key = nonUsblIndxPath.evaluate(context);
          //TODO:Asif : Check this logic out
          if (key != null && key.equals(QueryService.UNDEFINED)) continue;
          singleUsblIndex.query(key, OQLLexerTokenTypes.TOK_EQ,
              singlUsblIndxRes, context);
          cutDownAndExpandIndexResults(returnSet, singlUsblIndxRes,
              singleUsableICH.indexFieldToItrsMapping, totalExpList, finalList,
              context, singleUsableICH.checkList, iterOperands, singleUsableICH.indxInfo);
          singlUsblIndxRes.clear();
        }
      }
      finally {
        observer.afterIterJoinOfSingleIndexResults(returnSet);
        observer.afterIndexLookup(returnSet);
      }
      return returnSet;
    }
    else {
      //Asif : This condition is filter evaluatable but both the RHS group as
      // well as
      // LHS group of iterators are present in the intermediate resultset. As a
      // result indexes
      // cannot be used for this condition. This condition needs to be iter
      // evaluated.
      // For BETTER PERF INDEXES SHOULD BE REMOVED FROM THIS CONDITION SO THAT
      // IT BECOMES
      //PART OF ITER OPERANDS
      if (logger.isDebugEnabled()) {
        StringBuffer tempBuffLhs = new StringBuffer();
        StringBuffer tempBuffRhs = new StringBuffer();
        ich1.indxInfo._path.generateCanonicalizedExpression(tempBuffLhs,
            context);
        ich2.indxInfo._path.generateCanonicalizedExpression(tempBuffRhs,
            context);
        logger.debug("For better performance indexes are not used for the condition {} = {}", tempBuffLhs, tempBuffRhs);
      }
      CompiledValue reconstructedVal = new CompiledComparison(
          ich1.indxInfo._path, ich2.indxInfo._path, OQLLexerTokenTypes.TOK_EQ);
      //Add this reconstructed value to the iter operand if any
      CompiledValue finalVal = reconstructedVal;
      if (iterOperands != null) {
        //Asif : The type of CompiledJunction has to be AND junction as this
        // function gets invoked only for AND . Also it is OK if we have
        // iterOperands which
        //itself is a CompiledJunction. We can have a tree of CompiledJunction
        // with its
        // operands being a CompiledComparison & a CompiledJunction. We can live
        // without
        // creating a flat structure
        finalVal = new CompiledJunction(new CompiledValue[] { iterOperands,
            reconstructedVal}, OQLLexerTokenTypes.LITERAL_and);
      }
      RuntimeIterator[][] resultMappings = new RuntimeIterator[1][];
      resultMappings[0] = resultFieldsItrMapping;
      return cartesian(new SelectResults[] { intermediateResults},
          resultMappings, Collections.EMPTY_LIST, finalList, context, finalVal);
    }
  }

  /**
   * Asif :This function is used to evaluate a filter evaluatable composite
   * condition. It gets invoked either from a CompositeGroupJunction of "OR"
   * type or a where clause containing single composite condition. In the later
   * case the boolean completeExpansion flag is always true. While in the former
   * case it may be true or false. If it is false, the array of independent
   * iterators passed is not null.
   * 
   * @param data A List object whose elements are two dimensional object array.
   *          Each element of the List represent a value which satisfies the
   *          equi-join condition. Since there may be more than one tuples on
   *          either side of the equality condition which meet the criteria for
   *          a given value, we require a 2 dimensional Object array. The
   *          cartesian of the two rows will give us the set of tuples
   *          satisfying the join criteria. Each element of the row of Object
   *          Array may be either an Object or a Struct object.
   * 
   * 
   * @param indxInfo An array of IndexInfo objects of size 2 , representing the
   *          range indexes of the two operands. The other Index maps to the 0th
   *          Object array row of the List object ( data ) & so on.
   * @param context ExecutionContext object
   * @param completeExpansionNeeded boolean if true indicates that the CGJ needs
   *          to be expanded to the query from clause ( top level )
   * @param iterOperands This will be null as for OR junction we cannot have
   *          iter operand
   * @param indpdntItrs Array of independent iterators representing the various
   *          Groups forming the composite group junction. It will be null, if
   *          complete expansion flag is true
   * @return SelectResults objet representing the result obtained by evaluating
   *         a filter evaluatable composite condition in an OR junction. The
   *         returned Result is expanded either to the CompositeGroupJunction
   *         level or to the top level as the case may be
   * @throws FunctionDomainException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   * @throws QueryInvocationTargetException
   *  
   */
  static SelectResults getconditionedRelationshipIndexResultsExpandedToTopOrCGJLevel(
      List data, IndexInfo[] indxInfo, ExecutionContext context,
      boolean completeExpansionNeeded, CompiledValue iterOperands,
      RuntimeIterator[] indpdntItrs) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    ObjectType resultType1 = indxInfo[0]._index.getResultSetType();
    int indexFieldsSize1 = resultType1 instanceof StructType ? ((StructTypeImpl) resultType1)
        .getFieldNames().length
        : 1;
    ObjectType resultType2 = indxInfo[1]._index.getResultSetType();
    int indexFieldsSize2 = resultType2 instanceof StructType ? ((StructTypeImpl) resultType2)
        .getFieldNames().length
        : 1;
    /*
     * Asif : even if th complete expansion is needed pass the flag of complete
     * expansion as false. Thus for LHS & RHS we will get the expansionList for
     * that individual group. Thus the total expansion List wil contain sum of
     * the individual expansion lists plus all the iterators of the current
     * scope which are dependent on any other groups or are composite iterators (
     * i.e dependent on both the independent groups currently under
     * consideration
     */
    IndexConditioningHelper ich1 = new IndexConditioningHelper(indxInfo[0],
        context, indexFieldsSize1,
        false/* Asif : pass it as false, irrespective of actual value */,
        iterOperands, null);
    IndexConditioningHelper ich2 = new IndexConditioningHelper(indxInfo[1],
        context, indexFieldsSize2,
        false/* Asif : pass it as false, irrespective of actual value */,
        iterOperands, null);
    List totalExpList = new ArrayList();
    totalExpList.addAll(ich1.expansionList);
    totalExpList.addAll(ich2.expansionList);
//    List[] checkList = new List[] { ich1.checkList, ich2.checkList};
    List totalFinalList = null;
    if (completeExpansionNeeded) {
      totalFinalList = context.getCurrentIterators();
      Set expnItrsAlreadyAccounted = new HashSet();
      expnItrsAlreadyAccounted.addAll(ich1.finalList);
      expnItrsAlreadyAccounted.addAll(ich2.finalList);
      int size = totalFinalList.size();
      for (int i = 0; i < size; ++i) {
        RuntimeIterator currItr = (RuntimeIterator) totalFinalList.get(i);
        //Asif :If the runtimeIterators of scope not present in CheckSet add it
        // to
        // the expansion list
        if (!expnItrsAlreadyAccounted.contains(currItr)) {
          totalExpList.add(currItr);
        }
      }
    }
    else {
      totalFinalList = new ArrayList();
      for (int i = 0; i < indpdntItrs.length; ++i) {
        RuntimeIterator indpndntItr = indpdntItrs[i];
        if (indpndntItr == ich1.finalList.get(0)) {
          totalFinalList.addAll(ich1.finalList);
        }
        else if (indpndntItr == ich2.finalList.get(0)) {
          totalFinalList.addAll(ich2.finalList);
        }
        else {
          List temp = context
              .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(indpndntItr);
          totalFinalList.addAll(temp);
          totalExpList.addAll(temp);
        }
      }
    }
    Support
        .Assert(
            totalFinalList.size() > 1,
            " Since we are in relationship index this itself means that we have atleast two RuntimeIterators");
    StructType stype = createStructTypeForRuntimeIterators(totalFinalList);
    SelectResults returnSet = QueryUtils.createStructCollection(context, stype) ; 
    RuntimeIterator[][] mappings = new RuntimeIterator[2][];
    mappings[0] = ich1.indexFieldToItrsMapping;
    mappings[1] = ich2.indexFieldToItrsMapping;
    List[] totalCheckList = new List[] { ich1.checkList, ich2.checkList};
    Iterator dataItr = data.iterator();
    IndexCutDownExpansionHelper[] icdeh = new IndexCutDownExpansionHelper[] {
        new IndexCutDownExpansionHelper(ich1.checkList, context),
        new IndexCutDownExpansionHelper(ich2.checkList, context)};
    ListIterator expansionListIterator = totalExpList.listIterator();
    if (dataItr.hasNext()) {
      QueryObserver observer = QueryObserverHolder.getInstance();
      try {
        observer.beforeMergeJoinOfDoubleIndexResults(ich1.indxInfo._index,
            ich2.indxInfo._index, data);
        while (dataItr.hasNext()) {
          //TODO:Asif Change the code in range Index so that while collecting
          // data
          // instead of creating
          //two dimensional object array , we create one dimensional Object
          // array
          // of size 2, & each elemnt
          //stores an Object array
          Object[][] values = (Object[][]) dataItr.next();
          //Asif : Before doing the cartesian of the Results , we need to clear
          // the
          // CheckSet of IndexCutDownExpansionHelper. This is needed because for
          // a
          // new key , the row
          // of sets needs to be considered fresh as presence of old row in
          // checkset
          // may cause us to wrongly
          //skip the similar row of a set , even when the row in its entirety
          // is
          // unique ( made by
          // different data in the other set)
          mergeAndExpandCutDownRelationshipIndexResults(values, returnSet,
              mappings, expansionListIterator, totalFinalList, context,
              totalCheckList, iterOperands, icdeh, 0 /* Level */);
          if (icdeh[0].cutDownNeeded) icdeh[0].checkSet.clear();
        }
      }
      finally {
        observer.afterMergeJoinOfDoubleIndexResults(returnSet);
      }
    }
    return returnSet;
  }

  /*
   * Asif : This function is used ony for testing the private visibility
   * function
   */
  static SelectResults testCutDownAndExpandIndexResults(List dataList)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    return cutDownAndExpandIndexResults((SelectResults) dataList.get(0),
        (RuntimeIterator[]) dataList.get(1), (List) dataList.get(2),
        (List) dataList.get(3), (ExecutionContext) dataList.get(4),
        (List) dataList.get(5), null, null);
  } 
  
  static List queryEquijoinConditionBucketIndexes(IndexInfo[] indxInfo,
      ExecutionContext context)
      throws QueryInvocationTargetException, TypeMismatchException,
      FunctionDomainException, NameResolutionException {
    List data = null;
    ArrayList resultData = new ArrayList();
    AbstractIndex index0 = (AbstractIndex) indxInfo[0]._index;
    AbstractIndex index1 = (AbstractIndex) indxInfo[1]._index;
    PartitionedRegion pr0 = null;
    PartitionedRegion pr1 = null;
    IndexProtocol i0 = null;
    IndexProtocol i1 = null;

    if (index0.getRegion() instanceof BucketRegion) {
      pr0 = ((BucketRegion) index0.getRegion()).getPartitionedRegion();
    }

    if (index1.getRegion() instanceof BucketRegion) {
      pr1 = ((BucketRegion) index1.getRegion()).getPartitionedRegion();
    }

    for (Object b : context.getBucketList()) {
      i0 = (pr0 != null) ? PartitionedIndex.getBucketIndex(pr0,
          index0.getName(), (Integer) b) : indxInfo[0]._index;
      i1 = (pr1 != null) ? PartitionedIndex.getBucketIndex(pr1,
          index1.getName(), (Integer) b) : indxInfo[1]._index;

      if (i0 == null || i1 == null) {
        continue;
      }
      data = i0.queryEquijoinCondition(i1, context);
      resultData.addAll(data);
    }
    data = resultData;
    return data;
  }
}
/**
 * 
 * @author asif This is a helper class which provides information on how an
 *         index data be used so as to make it compatible with the query.
 *  
 */

class IndexConditioningHelper {

  /**
   * boolean if true implies that the index results need to be iterated so as to
   * make it compatible with from clause. Shuffling may be needed for any of the
   * following reasons: 1) Match level not zero ( implying index result
   * expansion or cutdown) 2) Match level zero , but the relative positions of
   * iterators in the List of iterators for the group not matching the positions
   * in the index result StructBag 3) Iter operand is not null. *
   *  
   */
  //Asif :If shuffling is not needed , then it can be bcoz of two reasons
  //1) The Index results is a ResultSet & match level is zero ( in that case we
  // don't have to do anything)
  //2) The Index results is a StructBag with match level as zero & inddex
  // fields matching
  //   the order of RuntimeIterators. In that case we just have to change the
  // StructType of the StructBag
  boolean shufflingNeeded = true;
  /**
   * An arary of RuntimeIterators whose size is equal to the number of fields in
   * the Index results. It identifies the RuntimeIterator for the field in the
   * Index Results. Thus the Runtime Iterator at position 0 will be that for
   * field 0 in the index result & so on. For those index fields which do not
   * have a Runtime Iterator assosciated , the value is null (This is the case
   * if index results require cut down)
   *  
   */
  RuntimeIterator[] indexFieldToItrsMapping = null;
  /**
   * The List containing RuntimeIterators to which the index results need to be
   * expanded This will usually be Final List of RuntimeIterators -
   * RuntimeIteratosr already accounted for in the index results
   */
  //Asif : The default is initialized as empty List rather than null to avoid
  // Null Pointer Exception in the function
  // getconditionedRelationshipIndexResults
  List expansionList = Collections.EMPTY_LIST;
  /**
   * The List containing RuntimeIterators which define the final SelectResults
   * after the relevant expansion/cutdown of index results
   */
  //Asif : Though in case of single index usage , if no shuffling is needed (
  // exact match) we
  // do not need finalList , but it is used in relation ship index , even if
  // match level is zero.
  // So we should never leave it as null
  List finalList = null;
  /**
   * This is the List of RuntimeIterators which gets created only if the index
   * resulst require a cutdown. In such cases , it identifies those Runtime
   * Iterators of Index Results which will be selected to form the result tuple.
   * The RuntimeIterators in this List will have corresponding fields in the
   * resultset obtained from Index usage. This List will be populated only if
   * there exists fields in index resultset which will not be selected.If all
   * the fields of index resultset will be used , then this List should be null
   * or empty. It is used in preventing unnecessary expansion of same type, when
   * a similar expansion has already occured. as for eg
   * 
   * consider a index result containing 3 fields field1 field2 & field3 . Assume
   * that field3 is for cutdown. Since the expansion iterators can either be
   * independent of all the fields in the index result or at the max be
   * dependent on field1 & field2, we should expand for a given combination of
   * field1 & field2 , only once ( as we have resulst as Set, we can only have
   * unique entries) ie. suppose a index result tuple has values ( 1,2 , 3 ) & (
   * 1,2,4) , we should expand only once ( as field with value 3 & 4 are to be
   * discarded).
   * 
   * 
   *  
   */
  /*
   * Asif: Below Can be null or empty collections if the match level is exact &
   * no shuffling needed
   */
  List checkList = null;
  /**
   * Asif : This field is meaninful iff the match level is zero, no shuffling
   * needed & there exists a StructBag (& not a ResultBag)
   */
  StructType structType = null;
  /**
   * Independent Iterator for the Group to which the Path expression belongs to
   */
  RuntimeIterator indpndntItr = null;
  /**
   * Indexnfo object for the path expression
   */
  IndexInfo indxInfo = null;

  //TODO:Asif :Test this function out
  public IndexConditioningHelper(IndexInfo indexInfo, ExecutionContext context,
      int indexFieldsSize, boolean completeExpansion,
      CompiledValue iterOperands, RuntimeIterator grpIndpndntItr) {
    /*
     * Asif : First obtain the match level of index resultset. If the match
     * level happens to be zero , this implies that we just have to change the
     * StructType ( again if only the Index resultset is a StructBag). If the
     * match level is zero & expand to to top level flag is true & iff the total
     * no. of iterators in current scope is greater than the no. of fields in
     * StructBag , then only we need to do any expansion. The grpIndpndtItr
     * passed can be null if the where clause comprises of just this condition.
     * However if it is invoked from GroupJunction , it will be not null
     *  
     */
    this.indxInfo = indexInfo;
    List grpItrs = null;
    int size = indexInfo.mapping.length;
    this.indpndntItr = grpIndpndntItr;
    this.indexFieldToItrsMapping = new RuntimeIterator[indexFieldsSize];
    //  Asif Obtain the grpIndpndt iterator if it is passed as null
    if (this.indpndntItr == null) {
      Set set1 = new HashSet();
      context.computeUtlimateDependencies(indexInfo._path, set1);
      Support
          .Assert(
              set1.size() == 1,
              " Since we are in Indexed Evaluate that means there has to be exactly one independent iterator for this compiled comparison");
      // The ultimate independent RuntimeIterator
      this.indpndntItr = (RuntimeIterator) set1.iterator().next();
      Support
          .Assert(
              this.indpndntItr.getScopeID() == context.currentScope().getScopeID()/*context.getScopeCount()*/,
              " Since we are in Indexed Evaluate that means the current scope count & indpenedent iterator's scope count should match");
    }
    if (indexInfo._matchLevel == 0
        && (!completeExpansion || context.getCurrentIterators().size() == size)) {
      //Asif: Don't do anything , just change the StructType if the set is
      // structset.
      if (size > 1) {
        // The Index resultset is a structType.
        Support
            .Assert(
                indexInfo._index.getResultSetType() instanceof StructType,
                " If the match level is zero & the size of mapping array is 1 then Index is surely ResultBag else StructBag");
        //Asif : The independent iterator is added as the first element
        grpItrs = context
            .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(this.indpndntItr);
        //Asif : Check if reshuffling is needed or just changing the struct
        // type will suffice
        boolean isReshufflingNeeded = false;
        int pos = -1;
        for (int i = 0; i < size; ++i) {
          pos = indexInfo.mapping[i];
          isReshufflingNeeded = isReshufflingNeeded || (pos != (i + 1));
          this.indexFieldToItrsMapping[pos - 1] = (RuntimeIterator) grpItrs
              .get(i);
        }
        this.finalList = grpItrs;
        //Asif : Even if Reshuffle is not need but if the iter conditions are
        // present we need to do evaluation
        // We can avoid iterating over the set iff reshuffling is not needed &
        // there is no iter eval condition
        if (isReshufflingNeeded || iterOperands != null) {
          //this.expansionList = Collections.EMPTY_LIST;
          this.checkList = null;
          //indexReults = QueryUtils.cutDownAndExpandIndexResults(indexReults,
          //  indexFieldToItrsMapping, Collections.EMPTY_LIST, grpItrs,
          //context, Collections.EMPTY_LIST, iterOperands);
        }
        else {
          this.structType = QueryUtils
              .createStructTypeForRuntimeIterators(grpItrs);
          //indexReults.setElementType(structType);
          //Asif : Shuffling is not needed. Index results is a StructBag
          // with match level zero & no expansion needed & index fields map
          // with the RuntimeIterators. But we need to change the StructType
          //of the StructBag
          this.shufflingNeeded = false;
        }
      }
      else {
        //Asif : The finalList should not be left uninitialized, & if the match
        // level is zero
        // & the Index Results is a ResultBag ( & not an StructBag ) implying
        // indexFieldsSize of
        //1 , then the final List should contain only the independent iterator
        this.finalList = new ArrayList();
        this.finalList.add(this.indpndntItr);
        Support
            .Assert(
                this.indexFieldToItrsMapping.length == 1,
                "In this else block , it should be guaranteed that there exists only one iterator in query as well as index from clause & that should be nothing but the independent RuntimeIterator of the group  ");
        this.indexFieldToItrsMapping[0] = this.indpndntItr;
        //Asif :Shuffling is needed if iter operand is not null even if index results is a ResultSet
        // with match level zero & no expansion needed       
        this.shufflingNeeded = (iterOperands != null)  ;        
      }
    }
    else {
      //Asif : There is some expansion or truncation needed on the data
      // obtained from index.Identify a the iterators belonging to this group
      // The independent iterator is added as the first element
      grpItrs = context
          .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(this.indpndntItr);
      //Asif Create an array of RuntimeIterators which map to the fields of the
      // Index set.
      // For those fields which do not have corresponding RuntimeIterator , keep
      // it as null;
      int pos = -1;
      this.finalList = completeExpansion ? context.getCurrentIterators()
          : grpItrs;
      // This is the List of runtimeIterators which have corresponding fields
      // in the resultset obtained from Index usage. This List will be populated
      // only if there exists fields in index resultset which will not be
      // selected
      // If all the fields of index resultset will be used , then this List
      // should
      // be null or empty
      this.checkList = new ArrayList();
      //This List contains the RuntimeIterators which are missing from
      // index resultset but are present in the final iterators
      this.expansionList = new LinkedList(finalList);
      RuntimeIterator tempItr = null;
      //boolean cutDownNeeded = false;
      int unMappedFields = indexFieldsSize;
      for (int i = 0; i < size; ++i) {
        pos = indexInfo.mapping[i];
        if (pos > 0) {
          tempItr = (RuntimeIterator) grpItrs.get(i);
          this.indexFieldToItrsMapping[pos - 1] = tempItr;
          this.expansionList.remove(tempItr);
          this.checkList.add(tempItr);
          --unMappedFields;
        }
      }
      boolean cutDownNeeded = unMappedFields > 0;
      if (!cutDownNeeded) this.checkList = null;
      /*
       * indexReults = QueryUtils.cutDownAndExpandIndexResults(indexReults,
       * indexFieldToItrsMapping, expansionList, finalList, context, checkList,
       * iterOperands);
       */
    }
  }
}
/**
 * 
 * @author asif This is a helper class which contains informaion on how to
 *         expand / cutdown index results for making it compatible with the
 *         query.
 *  
 */

class IndexCutDownExpansionHelper {

  /**
   * booelan which identifies if a cutdown of index results is needed or not.
   */
  boolean cutDownNeeded = false;
  /**
   * A SelectResults ( ResultBag or StructBag) object used to prevent
   * unnecessary expansion of index results as described in
   * IndexConditionalHelper class.
   */
  SelectResults checkSet = null;
  /**
   * ObjectType for the checkSet object ( An ObjectType for a ResultBag &
   * StructType for a StructBag)
   */
  ObjectType checkType = null;
  int checkSize = -1;

  IndexCutDownExpansionHelper(List checkList, ExecutionContext context) {
    cutDownNeeded = checkList != null && (checkSize = checkList.size()) > 0;
    if (cutDownNeeded) {
      Boolean orderByClause = (Boolean)context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
      boolean useLinkedDataStructure = false;
      boolean nullValuesAtStart = true;
      if(orderByClause != null && orderByClause.booleanValue()) {
        List orderByAttrs = (List)context.cacheGet(CompiledValue.ORDERBY_ATTRIB);        
        useLinkedDataStructure =orderByAttrs.size()==1;
        nullValuesAtStart = !((CompiledSortCriterion)orderByAttrs.get(0)).getCriterion();
      }
      if (checkSize > 1) {
        
        checkType = QueryUtils.createStructTypeForRuntimeIterators(checkList);
        if (useLinkedDataStructure) {
          checkSet = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) checkType) 
          : new SortedResultsBag<Struct>((StructTypeImpl)checkType, nullValuesAtStart);
        } else {
          checkSet = QueryUtils.createStructCollection(context, (StructTypeImpl)checkType) ;
        }
      }
      else {
        checkType = ((RuntimeIterator) checkList.get(0)).getElementType();
        if (useLinkedDataStructure) {
          checkSet = context.isDistinct() ? new LinkedResultSet(checkType) :
            new SortedResultsBag(checkType, nullValuesAtStart); 
        } else {
          checkSet = QueryUtils.createResultCollection(context, checkType) ;
        }
      }
    }
  }
  
}
