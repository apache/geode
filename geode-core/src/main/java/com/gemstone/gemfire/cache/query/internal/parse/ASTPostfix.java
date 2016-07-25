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
import antlr.collections.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 * This handles paths, method invocations, and index operators that
 * post-fix to a receiver expression.
 */
public class ASTPostfix extends GemFireAST {
  private static final long serialVersionUID = 1100525641797867500L;
  
  public ASTPostfix() { }
  
  public ASTPostfix(Token t) {
    super(t);
  }
  
  @Override
  public void compile(QCompiler compiler) {
    AST child = getFirstChild();
    // push receiver, which is any CompiledValue
    ((GemFireAST)child).compile(compiler);
    child = child.getNextSibling();
    while (child != null) {
      switch (child.getType()) {
        case OQLLexerTokenTypes.METHOD_INV:
          // defer to method inv
          ((GemFireAST)child).compile(compiler);
          break;
        case OQLLexerTokenTypes.Identifier:
          compiler.appendPathComponent(child.getText());
          break;
        case OQLLexerTokenTypes.TOK_LBRACK:
          // push the indexExpr List on the stack
          //If the sibling is TOKEN_STAR, push null
          if(child.getFirstChild().getType() == OQLLexerTokenTypes.TOK_STAR) {
            compiler.pushNull();  
          }else {
            ((GemFireAST)child).compile(compiler);
          }
          compiler.indexOp();
          break;
        default:
          throw new Error("unexpected node type:"  + child.getType());
      }
      child = child.getNextSibling();
    }
  }
  
}
