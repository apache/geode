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

import java.util.HashMap;
import java.util.Map;

import antlr.Token;
import antlr.collections.AST;

import org.apache.geode.cache.query.internal.QCompiler;

public class ASTSelect extends GemFireAST {
  private static final long serialVersionUID = 1389351692304773456L;


  public ASTSelect() {}


  public ASTSelect(Token t) {
    super(t);
  }


  @Override
  public void compile(QCompiler compiler) {
    AST child = getFirstChild();
    Map<Integer, Object> queryComponents = new HashMap<>();
    // The query components are :
    // 1) Distinct | All
    // 2) Hint
    // 3) count
    // 4) Projection Attrib
    // 5) From Clause
    // 6) where clause
    // 7) group by clause
    // 8) order by clause
    // 9) limit
    while (child != null) {
      int clauseType = child.getType();
      ((GemFireAST) child).compile(compiler);
      Object compiledObject = compiler.pop();
      queryComponents.put(clauseType, compiledObject);
      child = child.getNextSibling();
    }

    compiler.select(queryComponents);
  }


}
