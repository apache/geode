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

//import java.util.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.pdx.internal.PdxString;
//import com.gemstone.gemfire.internal.Assert;

/**
 * Class Description
 *
 * @version     $Revision: 1.1 $
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
