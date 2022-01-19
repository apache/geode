/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */


package org.apache.geode.cache.query.internal.parse;

import antlr.Token;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ASTMethodInvocation extends GemFireAST {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = -3158542132262327470L;
  private boolean implicitReceiver;


  public ASTMethodInvocation() {}

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
    if (implicitReceiver) {
      compiler.pushNull(); // placeholder for receiver
    }

    // push the methodName and argList on the stack
    super.compile(compiler);

    compiler.methodInvocation();
  }


}
