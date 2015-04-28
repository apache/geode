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

public class ASTOr extends GemFireAST {
  private static final long serialVersionUID = 5770135811089855867L;
  public ASTOr(Token t) {
    super(t);
  }
  
  public ASTOr() { }
  
  
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    compiler.or(getNumberOfChildren());
  }
}
