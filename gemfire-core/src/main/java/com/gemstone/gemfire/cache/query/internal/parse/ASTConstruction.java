/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */


package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
import com.gemstone.gemfire.cache.query.internal.*;
import com.gemstone.gemfire.internal.Assert;

public class ASTConstruction extends GemFireAST {  
  private static final long serialVersionUID = 6647545354866647845L;
  
  public ASTConstruction() { }
  
  
  public ASTConstruction(Token t) {
    super(t);
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    // we only support "SET" for now
    Assert.assertTrue("set".equalsIgnoreCase(getText()));
    // left argList on stack
    compiler.constructObject(ResultsSet.class);
  }
  
}
