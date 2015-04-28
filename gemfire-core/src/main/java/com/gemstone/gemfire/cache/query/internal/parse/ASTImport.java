/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */


package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
import antlr.collections.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

public class ASTImport extends GemFireAST {  
  private static final long serialVersionUID = 6002078657881181949L;
  
  public ASTImport() { }
  
  
  public ASTImport(Token t) {
    super(t);
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
    StringBuffer nameBuf = new StringBuffer();
    String asName = null;
    AST child = getFirstChild();
    while (child != null) {
      if (child.getType() == OQLLexerTokenTypes.LITERAL_as) {
        child = child.getNextSibling();
        asName = child.getText();
        break;
      }
      else {
        nameBuf.append(child.getText());
        child = child.getNextSibling();
        if (child != null && child.getType() != OQLLexerTokenTypes.LITERAL_as) {
          nameBuf.append('.');
        }
      }
    }
    compiler.importName(nameBuf.toString(), asName);
  }
}
