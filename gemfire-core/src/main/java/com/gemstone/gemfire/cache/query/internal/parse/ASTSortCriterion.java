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
 * @author Yogesh Mahajan
 */
public class ASTSortCriterion extends GemFireAST {
  private static final long serialVersionUID = -3654854374157753771L;
  public ASTSortCriterion() { }

  /** Creates a new instance of ASTSortCriterion */
  public ASTSortCriterion(Token t) {
    super(t);
  }
    
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    compiler.compileSortCriteria(this.getText());
  }

  
  
}
