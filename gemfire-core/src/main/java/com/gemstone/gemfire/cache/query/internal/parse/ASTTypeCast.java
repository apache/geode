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


public class ASTTypeCast extends GemFireAST {
  private static final long serialVersionUID = -6368577668325776355L;
  
  public ASTTypeCast() { }
  
  public ASTTypeCast(Token t) {
    super(t);
  }
  
  
  
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    // there's a type on the stack now
    compiler.typecast();
  }
  
  
}
