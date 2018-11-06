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

/**
 * AST class used for an AST node that cannot be compiled directly because it is either data for
 * another operation or is a feature that is not yet supported by GemFire
 *
 */
public class ASTUnsupported extends GemFireAST {
  private static final long serialVersionUID = -1192307218047393827L;

  public ASTUnsupported() {}

  public ASTUnsupported(Token t) {
    super(t);
  }

  @Override
  public void compile(QCompiler compiler) {
    throw new UnsupportedOperationException(
        String.format("Unsupported feature: %s", getText()));
  }
}
