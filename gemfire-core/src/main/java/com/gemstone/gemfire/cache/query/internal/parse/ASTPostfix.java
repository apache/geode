/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
