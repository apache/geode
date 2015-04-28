/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: CompiledLiteral.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

//import java.util.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.pdx.internal.PdxString;
//import com.gemstone.gemfire.internal.Assert;

/**
 * Class Description
 *
 * @version     $Revision: 1.1 $
 * @author      ericz
 */


public class CompiledLiteral extends AbstractCompiledValue {
  Object _obj;
  PdxString _pdxString;
  
  public CompiledLiteral(Object obj) {
    _obj = obj;
  }
  
  
  public int getType() {
    return LITERAL;
  }
  
  
  public Object evaluate(ExecutionContext context)
  throws FunctionDomainException, TypeMismatchException {
    return _obj;
  }
  
  /**
   * creates new PdxString from String and caches it
   */
  public PdxString getSavedPdxString(){
    if(_pdxString == null){
      _pdxString = new PdxString((String)_obj);
    }
    return _pdxString;
  }
  
  @Override
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer, ExecutionContext context)
  throws AmbiguousNameException, TypeMismatchException {
    if (_obj == null) {
      clauseBuffer.insert(0, "null");
    }
    else if (_obj instanceof String) {
      clauseBuffer.insert(0, '\'').insert(0, _obj.toString()).insert(0, '\'');
    }
    else {      
      clauseBuffer.insert(0, _obj.toString());
    }
  } 
  
  public int getSizeEstimate(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException
  {
    //The literal could be true or false only in case of Filter 
    // Evaluation. Either way it should be evaluated first, Right?
    return 0;
  }

}
