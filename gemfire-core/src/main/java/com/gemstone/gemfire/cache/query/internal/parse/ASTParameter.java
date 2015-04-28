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


public class ASTParameter extends GemFireAST {
  private static final long serialVersionUID = 2964948528198383319L;
//  private int _index;
  
  public ASTParameter(Token t) {
    super(t);
  }
  
  public ASTParameter() { }
  
  @Override
  public void compile(QCompiler compiler)  {
    compiler.pushBindArgument(Integer.parseInt(getFirstChild().getText()));
  }
  
}
