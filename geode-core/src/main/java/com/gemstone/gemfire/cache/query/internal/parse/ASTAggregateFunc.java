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
package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.Token;

import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 * 
 *
 */
public class ASTAggregateFunc extends GemFireAST {
  private static final long serialVersionUID = 8713004765228379685L;
  private int  aggFunctionType;
  private boolean  distinctOnly = false;
  
  
  public ASTAggregateFunc() { 
    
  }
  
  
  public ASTAggregateFunc(Token t) {
    super(t);    
  }
  
  public void setAggregateFunctionType(int type) {
    this.aggFunctionType = type;
  }
  
  public void setDistinctOnly(boolean distinctOnly) {
    this.distinctOnly = distinctOnly;
  }
  
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    Object expr = compiler.pop();
    if(expr instanceof String) {
      if(((String)expr).equals("*")) {
        expr = null;
      }else {
        throw new QueryInvalidException("invalid parameter to aggregate function");
      }
    }
    compiler.aggregateFunction((CompiledValue)expr, this.aggFunctionType, this.distinctOnly);
  }
}
