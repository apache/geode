/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: CompiledFunction.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.internal.PdxString;



/**
 * Predefined functions
 *
 * @version     $Revision: 1.1 $
 * @author      ericz
 */


public class CompiledFunction extends AbstractCompiledValue {
  private CompiledValue [] _args;
  private int _function;
  
  public CompiledFunction(CompiledValue [] args, int function) {
    _args = args;
    _function = function;
  }
  
  @Override
  public List getChildren() {
    return Arrays.asList(this._args);
  }
  
  
  public int getType() {
    return FUNCTION;
  }
  public int getFunction() {
    return this._function;
  }
  
  public Object evaluate(ExecutionContext context)
  throws FunctionDomainException, TypeMismatchException, NameResolutionException,
          QueryInvocationTargetException {
    if (this._function == LITERAL_element) {
      Object arg = _args[0].evaluate(context);
      return call(arg, context);
    } else if (this._function == LITERAL_nvl) {
      return Functions.nvl(_args[0],_args[1], context);
    } else if (this._function == LITERAL_to_date){
      return Functions.to_date(_args[0],_args[1], context);
    } else {  
      throw new QueryInvalidException(LocalizedStrings.CompiledFunction_UNSUPPORTED_FUNCTION_WAS_USED_IN_THE_QUERY.toLocalizedString());
    }
  }
  
  @Override
  public Set computeDependencies(ExecutionContext context)
  throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    int len =  this._args.length;
    for(int i = 0; i < len; i++) {
      context.addDependencies(this,this._args[i].computeDependencies(context));  
    }
    return context.getDependencySet(this, true);
  }
  
  private Object call(Object arg, ExecutionContext context)
  throws FunctionDomainException, TypeMismatchException {
    Support.Assert(_function == LITERAL_element);
    return Functions.element(arg, context);
  }
  
  public CompiledValue [] getArguments() {
    return this._args;
  }
  
  @Override
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer, ExecutionContext context)
  throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    clauseBuffer.insert(0, ')');
    int len = this._args.length;
    for(int i = len - 1; i > 0; i--) {
      _args[i].generateCanonicalizedExpression(clauseBuffer, context);
       clauseBuffer.insert(0, ',');
    }
    _args[0].generateCanonicalizedExpression(clauseBuffer, context);
    switch(this._function) {
      case LITERAL_nvl : 
        clauseBuffer.insert(0, "NVL(");
        break;
      case LITERAL_element : 
        clauseBuffer.insert(0, "ELEMENT(");
        break;
      case LITERAL_to_date : 
        clauseBuffer.insert(0, "TO_DATE(");
        break;    
       default :
        super.generateCanonicalizedExpression(clauseBuffer, context);
    }          
  }
}

