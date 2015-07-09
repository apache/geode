/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */


package com.gemstone.gemfire.cache.query.internal.parse;

import java.util.HashMap;
import java.util.Map;

import antlr.*;
import antlr.collections.*;

import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;

public class ASTSelect extends GemFireAST {
  private static final long serialVersionUID = 1389351692304773456L;
  
  
  public ASTSelect() { }
  
  
  public ASTSelect(Token t) {
    super(t);
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
    AST child = getFirstChild();
    Map<Integer, Object> queryComponents = new HashMap<Integer, Object>();
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
