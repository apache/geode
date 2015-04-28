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
