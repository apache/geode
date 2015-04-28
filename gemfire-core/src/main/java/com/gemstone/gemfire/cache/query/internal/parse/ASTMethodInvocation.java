/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */


package com.gemstone.gemfire.cache.query.internal.parse;

import org.apache.logging.log4j.Logger;

import antlr.*;

import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.internal.logging.LogService;

public class ASTMethodInvocation extends GemFireAST {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = -3158542132262327470L;
  private boolean implicitReceiver;
    
  
  public ASTMethodInvocation() { }    
  
  public ASTMethodInvocation(Token t) {
    super(t);
  }
  
  public void setImplicitReceiver(boolean implicitReceiver) {
    this.implicitReceiver = implicitReceiver;
  }
  
  @Override
  public int getType() {
    return OQLLexerTokenTypes.METHOD_INV;
  }
  
  
  
  @Override
  public void compile(QCompiler compiler) {
    if (logger.isTraceEnabled()) {
      logger.trace("ASTMethodInvocation.compile: implicitReceiver={}", implicitReceiver);
    } 
    if (this.implicitReceiver) {
      compiler.pushNull(); // placeholder for receiver
    }
    
    // push the methodName and argList on the stack
    super.compile(compiler);
    
    compiler.methodInvocation();
  }
  
  
}
