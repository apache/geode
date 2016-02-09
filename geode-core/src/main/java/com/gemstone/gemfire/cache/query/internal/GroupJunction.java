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
 * Created on Oct 25, 2005
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.ArrayList;
import java.util.List;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * This structure contains the filter evaluable and iter evaluable
 * conditions which are dependent on a group of iterators derived from a single
 * independent iterator ( an iterator on the region) . The iter evaluatable
 * conditions belonging to other group of iterators can be a part of
 * GroupJunction only if the complete expansion flag is true.
 * 
 * @author EricZ
 * @author asif
 * @author kdeshpan
 * 
 *  
 */
public class GroupJunction extends AbstractGroupOrRangeJunction {
  private List unevaluatedFilterOperands = null; 

  GroupJunction(int operator, RuntimeIterator[] indpndntItr,
      boolean isCompleteExpansion, CompiledValue[] operands) {
    super(operator, indpndntItr,isCompleteExpansion,  operands);
    
  }

  void addUnevaluatedFilterOperands(List unevaluatedFilterOps) {
	this.unevaluatedFilterOperands = unevaluatedFilterOps;  
  }
  
  List getUnevaluatedFilterOperands() {
	return this.unevaluatedFilterOperands;  
  }
  
  private GroupJunction(AbstractGroupOrRangeJunction oldGJ, boolean completeExpansion,
      RuntimeIterator indpnds[], CompiledValue iterOp) {
    super(oldGJ, completeExpansion,indpnds, iterOp);    
  }
  
  @Override
  AbstractGroupOrRangeJunction recreateFromOld( boolean completeExpansion, RuntimeIterator indpnds[], CompiledValue iterOp) {
    return new GroupJunction(this,completeExpansion,indpnds,iterOp);      
  }
  
  @Override
  AbstractGroupOrRangeJunction createNewOfSameType(int operator,
      RuntimeIterator[] indpndntItr, boolean isCompleteExpansion,
      CompiledValue[] operands) {
    return new GroupJunction(operator, indpndntItr, isCompleteExpansion,
        operands);
  }
  
  @Override
  OrganizedOperands organizeOperands(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // get the list of operands to evaluate,
    // and evaluate operands that can use indexes first
    
    List evalOperands = new ArrayList(_operands.length);
    int indexCount = 0;
    boolean foundPreferredCondition = false; 
    if (this.getOperator() == LITERAL_and) {
      if (context instanceof QueryExecutionContext && ((QueryExecutionContext)context).hasHints() && ((QueryExecutionContext)context).hasMultiHints()) {
        //Hint was provided, so allow multi index usage
        for (int i = 0; i < _operands.length; i++) {
          if (_operands[i].getPlanInfo(context).evalAsFilter) { 
                            indexCount++; 
                        evalOperands.add(0, _operands[i]); 
          }
          else {
            evalOperands.add(_operands[i]);
          }
        }
      }
      else {
        //Hint was not provided so continue with our single index solution
        /*
        The idea is to use only one index in case of AND Junction .
        Ideally the best index to be used should have been decided during the initial phase itself
        when the compiled junction tree is being probed for indexable operands. But there are issues in
        it as we need to tackle comlex cases like detection of those conditions which can actually form
        a closed range or those which belong to different independent runtime iterators ( in case of multi
        region queries). So going for the  quick fix of sorting here. The filter operands present here 
        could be Comaprisn, IN or Range. The priority of sorting will be equality/IN/Range/Inequality    
        */    
        
        Filter currentBestFilter = null;
        int currentBestFilterSize =-1;
        indexCount = 1;
      
        for (int i = 0; i < _operands.length; i++) {
          // Asif : If we are inside this function this iteslf indicates
          // that there exists atleast on operand which can be evalauted
          // as an auxFilterEvaluate. If any operand even if its flag of
          // evalAsFilter happens to be false, but if it is independently
          // evaluatable, we should attach it with the auxFilterEvaluatable
          // operands irrespective of its value ( either true or false.)
          // This is because if it is true or false, it can always be paired
          // up with other filter operands for AND junction. But for
          // OR junction it can get paired with the filterOperands only
          // if it is false. But we do not have to worry about whether
          // the junction is AND or OR because, if the independent operand's
          // value is true & was under an OR junction , it would not have
          // been filterEvaluated. Instead it would have gone for
          // auxIterEvaluate.
          // We are here itself implies, that any independent operand can be
          // either tru or false for an AND junction but always false for an
          // OR Junction.
          PlanInfo pi = _operands[i].getPlanInfo(context);
          //we check for size == 1 now because of the join optimization can 
          //leave an operand with two indexes, but the key element is not set
          //this will throw an npe
          if (pi.evalAsFilter && pi.indexes.size() == 1) {       
            if(pi.isPreferred) {
              if(currentBestFilter != null) {
                evalOperands.add(currentBestFilter);
              }
              // new best
              currentBestFilter = (Filter)_operands[i];
              currentBestFilterSize = ((Filter)_operands[i]).getSizeEstimate(context);
              foundPreferredCondition = true;
              continue;
            }
            if (currentBestFilter == null) {
              currentBestFilter = (Filter)_operands[i];            
              currentBestFilterSize = ((Filter)_operands[i]).getSizeEstimate(context);
            }
            else if ( foundPreferredCondition ||currentBestFilter.isBetterFilter((Filter)_operands[i], context, currentBestFilterSize)) {
              evalOperands.add(_operands[i]);
            }
            else {
              evalOperands.add(currentBestFilter);
              currentBestFilter = (Filter)_operands[i];
              // TODO:Asif: Avoid this call. Let the function which is doing the
              // comparison return some how the size of comparedTo operand. 
              currentBestFilterSize = ((Filter)_operands[i]).getSizeEstimate(context);
  
            }
          }
          else if (!_operands[i].isDependentOnCurrentScope(context)) {
            // TODO: Asif :Remove this Assert & else if condition after successful
            // testing of the build
            Support
                .assertionFailed("An independentoperand should not ever be present as operand inside a GroupJunction as it should always be present only in CompiledJunction");
          }
          else {
            evalOperands.add(_operands[i]);
          }
        }
        evalOperands.add(0, currentBestFilter);
      }
    }else {
      indexCount =_operands.length ;
      for (int i = 0; i < indexCount; i++) {
        evalOperands.add(_operands[i]);
      }
    }
    
    if(getIterOperands() != null) {
      evalOperands.add(getIterOperands());
    }
    return createOrganizedOperandsObject(indexCount,evalOperands);    
  }
  
  public int getSizeEstimate(ExecutionContext context)throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException  {
	return 1;
  }
}
