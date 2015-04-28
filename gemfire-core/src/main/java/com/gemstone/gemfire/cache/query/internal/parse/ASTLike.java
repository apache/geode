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

import antlr.*;

import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 * @author ashahid
 *
 */
public class ASTLike extends GemFireAST{
private static final long serialVersionUID = 1171234838254852463L;
public ASTLike() { }
  
  
  public ASTLike(Token t) {
    super(t);
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
    // two children c1 and c2 (c2 being a CompiledLiteral)
    super.compile(compiler);
    // puts c1 then c2 on stack
    compiler.like();
  }
}
