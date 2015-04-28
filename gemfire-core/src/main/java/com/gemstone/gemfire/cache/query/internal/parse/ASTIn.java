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

public class ASTIn extends GemFireAST {  
  private static final long serialVersionUID = -7688132343283983119L;
  
  public ASTIn() { }
  
  
  public ASTIn(Token t) {
    super(t);
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
    // two children, c1 IN c2
    super.compile(compiler);
    // puts c1 then c2 on stack
    compiler.inExpr();
  }
  
}
