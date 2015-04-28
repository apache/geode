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

import antlr.Token;

/**
 * @author shobhit
 * @since 6.6
 */
public class ASTCount extends GemFireAST {

  /**
   * 
   */
  public ASTCount() {
  }

  /**
   * @param tok
   */
  public ASTCount(Token tok) {
    super(tok);
  }

  @Override
  public void compile(QCompiler compiler) {
    GemFireAST child = (GemFireAST)getFirstChild();
    int tokenType = child.getType();
    if (tokenType == OQLLexerTokenTypes.TOK_STAR) {
      compiler.push("COUNT");
    }
  }

}
