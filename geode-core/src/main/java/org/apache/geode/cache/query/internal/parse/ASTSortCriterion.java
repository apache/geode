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

public class ASTSortCriterion extends GemFireAST {
  private static final long serialVersionUID = -3654854374157753771L;

  public ASTSortCriterion() {}

  /** Creates a new instance of ASTSortCriterion */
  public ASTSortCriterion(Token t) {
    super(t);
  }

  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    compiler.compileSortCriteria(getText());
  }



}
