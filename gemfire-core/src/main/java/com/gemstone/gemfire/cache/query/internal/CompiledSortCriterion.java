/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * This class represents a compiled form of sort criterian present in order by clause
 * @author Yogesh Mahajan
 */
public class CompiledSortCriterion extends AbstractCompiledValue  {
	//Asif: criterion true indicates descending order
	boolean criterion = false;	
	CompiledValue expr = null;
  
  @Override
  public List getChildren() {
    return Collections.singletonList(this.expr);
  }
  
	/**
	 * @return int
	 */
	public int getType()
    {
        return SORT_CRITERION;
    }
	/** evaluates sort criteria in order by clause 
	 * @param context
	 * @return Object
	 * @throws FunctionDomainException
	 * @throws TypeMismatchException
	 * @throws NameResolutionException
	 * @throws QueryInvocationTargetException
	 */
	public Object evaluate(ExecutionContext context)throws FunctionDomainException, TypeMismatchException,
    NameResolutionException, QueryInvocationTargetException 
	{
		return this.expr.evaluate(context);  
		 
	}
	/**
	 * concstructor
	 * @param criterion
	 * @param cv
	 */
	CompiledSortCriterion(boolean criterion, CompiledValue cv) {
	  this.expr = cv;
	  this.criterion = criterion;
	}
	
  public boolean getCriterion() {
    return criterion;
  }

  public CompiledValue getExpr() {
    return expr;
  }
	
}
