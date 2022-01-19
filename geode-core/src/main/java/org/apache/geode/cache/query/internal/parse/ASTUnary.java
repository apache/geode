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

import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.cache.query.internal.Support;

public class ASTUnary extends GemFireAST {
  private static final long serialVersionUID = -3906821390970046083L;

  public ASTUnary(Token t) {
    super(t);
  }

  public ASTUnary() {}

  @Override
  public void compile(QCompiler compiler) {
    if (getType() == OQLLexerTokenTypes.TOK_MINUS) {
      GemFireAST child = (GemFireAST) getFirstChild();
      int tokenType = child.getType();
      if (tokenType == OQLLexerTokenTypes.NUM_INT || tokenType == OQLLexerTokenTypes.NUM_LONG
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
