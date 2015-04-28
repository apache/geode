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

/**
 *
 * @author Eric Zoerner
 */
public class ASTCombination extends GemFireAST {
  private static final long serialVersionUID = -5390937242819850292L;
  
  public ASTCombination() { }

  /** Creates a new instance of ASTCombination */
  public ASTCombination(Token t) {
    super(t);
  }
    
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    compiler.combine(getNumberOfChildren());
  }

  
  
}
