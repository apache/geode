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

import antlr.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;


public class ASTProjection extends GemFireAST {  
  private static final long serialVersionUID = -1464858491766486290L;
  
  public ASTProjection() { }
  
  public ASTProjection(Token t) {
    super(t);
  }
  
    
  @Override
  public void compile(QCompiler compiler) {
    // has either one or two children
    // if has two children, first one is the expr, otherwise just the expr
    // push null on the stack if there is no label id
    super.compile(compiler);
    if (getNumberOfChildren() == 1) {
      compiler.pushNull();
    }
    compiler.projection();
  }
  
  
}
