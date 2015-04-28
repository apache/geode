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
//import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 *
 * @author Eric Zoerner
 */
public class GemFireAST extends CommonAST {
  private static final long serialVersionUID = 779964802274305208L;
  
  public GemFireAST() {
    super();
  }
  
  public GemFireAST(Token tok) {
    super(tok);
  }
  
  @Override
  public String getText() {
    String txt = super.getText();
    if (txt == null) {
      return "[no text]";
    }
    return txt;
  }
  
  public void compile(QCompiler compiler)  {
    childrenCompile(compiler);
  }
  
  public void childrenCompile(QCompiler compiler) {
    GemFireAST child = (GemFireAST)getFirstChild();
    while (child != null) {
      child.compile(compiler);
      child = (GemFireAST)child.getNextSibling();
    }
  }
  
}
