package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.Token;

import com.gemstone.gemfire.cache.query.internal.QCompiler;

public class ASTDummy extends GemFireAST {
private static final long serialVersionUID = -5390937473819850292L;
  
  public ASTDummy() { }

  /** Creates a new instance of ASTCombination */
  public ASTDummy(Token t) {
    super(t);
  }
    
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    compiler.push(this.getText());
  }

}
