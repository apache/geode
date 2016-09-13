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
//import antlr.collections.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 *
 */
public class ASTIteratorDef extends GemFireAST {
  private static final long serialVersionUID = -736956634497535951L;
  
  public ASTIteratorDef() { }
  
  
  public ASTIteratorDef(Token t) {
    super(t);
  }
  
  @Override
  public void compile(QCompiler compiler) {
    // children are colln expr, id, and type.
    // the id and/or type may be missing.
    
    GemFireAST child = (GemFireAST)getFirstChild();
    child.compile(compiler); // the colln expr
    
    GemFireAST nextChild = (GemFireAST)child.getNextSibling();
    if (nextChild == null) {
      // push two nulls for id and type
      compiler.pushNull();
      compiler.pushNull();
    }
    else {
      if (nextChild instanceof ASTType) {
        // push a null for the id
        compiler.pushNull();  // id
        nextChild.compile(compiler); // the type
        nextChild = (GemFireAST)nextChild.getNextSibling();
        Assert.assertTrue(nextChild == null);
      }
      else {
        nextChild.compile(compiler); // the id
        nextChild = (GemFireAST)nextChild.getNextSibling();
        if (nextChild == null) { // no type
          compiler.pushNull(); // type
        }
        else {
          Assert.assertTrue(nextChild instanceof ASTType);
          nextChild.compile(compiler); // must be the type
          Assert.assertTrue(nextChild.getNextSibling() == null);
        }
      }
    }
    
    compiler.iteratorDef();
  }
  
  
}
