/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: CompiledNegation.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.internal.Assert;


/**
 * Class Description
 *
 * @version     $Revision: 1.1 $
 * @author      ericz
 */


public class CompiledConstruction extends AbstractCompiledValue {
  private Class objectType;
  private List args;
  
  public CompiledConstruction(Class objectType, List args) {
    this.objectType = objectType;
    this.args = args;
  }
  
  @Override
  public List getChildren() {
    return args;
  }
  
  
  public int getType() {
    return CONSTRUCTION;
  }
  
  public Object evaluate(ExecutionContext context)
  throws FunctionDomainException, TypeMismatchException, NameResolutionException,
          QueryInvocationTargetException {
    // we only support ResultsSet now
    Assert.assertTrue(this.objectType == ResultsSet.class);
    ResultsSet newSet = new ResultsSet(this.args.size());
    for (Iterator itr = this.args.iterator(); itr.hasNext(); ) {
      CompiledValue cv = (CompiledValue)itr.next();
      Object eval = cv.evaluate(context);
      if (eval == QueryService.UNDEFINED) {
        return QueryService.UNDEFINED;
      }
      newSet.add(eval);
    }
    return newSet;
  }
  
  @Override
  public Set computeDependencies(ExecutionContext context)
  throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    for (Iterator itr = this.args.iterator(); itr.hasNext(); ) {
      CompiledValue cv = (CompiledValue)itr.next();
      context.addDependencies(this, cv.computeDependencies(context));
    }
    return context.getDependencySet(this, true);
  }
}
