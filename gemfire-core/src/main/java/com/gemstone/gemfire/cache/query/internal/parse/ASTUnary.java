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
import com.gemstone.gemfire.cache.query.internal.Support;
/**
 * 
 * @author Yogesh Mahajan
 *
 */
public class ASTUnary extends GemFireAST {	 
    private static final long serialVersionUID = -3906821390970046083L;

    /**
     * 
     * @param t
     */
	public ASTUnary(Token t) {
    super(t);
  }
  /**
   * 
   *
   */
  public ASTUnary() { }

  @Override
  public void compile(QCompiler compiler) {
    if (this.getType() == OQLLexerTokenTypes.TOK_MINUS) {
      GemFireAST child = (GemFireAST)getFirstChild();
      int tokenType = child.getType();
      if (tokenType == OQLLexerTokenTypes.NUM_INT
          || tokenType == OQLLexerTokenTypes.NUM_LONG
          || tokenType == OQLLexerTokenTypes.NUM_FLOAT
          || tokenType == OQLLexerTokenTypes.NUM_DOUBLE) {
        Support.Assert(child.getNextSibling() == null);
        child.setText('-' + child.getText());
        child.compile(compiler);
      } else {
        super.compile(compiler);
        compiler.unaryMinus();
      }
    } else {
      super.compile(compiler);
      compiler.not();
    }
  }
}
