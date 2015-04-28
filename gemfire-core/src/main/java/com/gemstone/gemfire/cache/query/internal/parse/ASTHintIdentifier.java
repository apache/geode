/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.Token;

import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 * @author jhuynh
 * 
 * @since 8.1
 *
 */
public class ASTHintIdentifier extends GemFireAST {
  
  public ASTHintIdentifier() {
  
  }
  
  public ASTHintIdentifier(Token token) {
    super(token);
  }
  
  @Override
  public String getText() {
    return super.getText();
  }

  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    compiler.setHintIdentifier(this.getText());
  }
}
