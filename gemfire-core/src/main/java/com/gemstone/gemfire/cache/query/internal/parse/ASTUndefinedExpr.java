/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */


package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;


public class ASTUndefinedExpr extends GemFireAST {
  private static final long serialVersionUID = -4629351288894274041L;
  
  public ASTUndefinedExpr(Token t) {
    super(t);
  }
  
  public ASTUndefinedExpr() { }
  
  
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    compiler.undefinedExpr(
            getType() == OQLLexerTokenTypes.LITERAL_is_defined);
  }
  
}
