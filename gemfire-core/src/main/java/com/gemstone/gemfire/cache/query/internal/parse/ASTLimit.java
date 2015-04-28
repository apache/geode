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
 * @author Asif Shahid
 */
public class ASTLimit extends GemFireAST {

  public ASTLimit() {
  }

  /** Creates a new instance of ASTSortCriterion */
  public ASTLimit(Token t) {
    super(t);
  }

  public void compile(QCompiler compiler) {
    super.compile(compiler);
    compiler.compileLimit(this.getText());
  }

}