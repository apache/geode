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

import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 * @author shobhit
 * 
 * @since 6.6.2
 *
 */
public class ASTTrace extends GemFireAST {

  @Override
  public String getText() {
    return super.getText();
  }

  @Override
  public void compile(QCompiler compiler) {
    compiler.traceRequest();
  }
}
