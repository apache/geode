/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
//import antlr.collections.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 *
 * @author Eric Zoerner
 */
public class ASTIteratorDef extends GemFireAST {
  private static final long serialVersionUID = -736956634497535951L;
  
  public ASTIteratorDef() { }
  
  
  public ASTIteratorDef(Token t) {
    super(t);
  }
  
  @Override
  public void compile(QCompiler compiler) {
    // children are colln expr, id, and type.
    // the id and/or type may be missing.
    
    GemFireAST child = (GemFireAST)getFirstChild();
    child.compile(compiler); // the colln expr
    
    GemFireAST nextChild = (GemFireAST)child.getNextSibling();
    if (nextChild == null) {
      // push two nulls for id and type
      compiler.pushNull();
      compiler.pushNull();
    }
    else {
      if (nextChild instanceof ASTType) {
        // push a null for the id
        compiler.pushNull();  // id
        nextChild.compile(compiler); // the type
        nextChild = (GemFireAST)nextChild.getNextSibling();
        Assert.assertTrue(nextChild == null);
      }
      else {
        nextChild.compile(compiler); // the id
        nextChild = (GemFireAST)nextChild.getNextSibling();
        if (nextChild == null) { // no type
          compiler.pushNull(); // type
        }
        else {
          Assert.assertTrue(nextChild instanceof ASTType);
          nextChild.compile(compiler); // must be the type
          Assert.assertTrue(nextChild.getNextSibling() == null);
        }
      }
    }
    
    compiler.iteratorDef();
  }
  
  
}
