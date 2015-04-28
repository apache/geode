/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * AST class used for an AST node that cannot be compiled directly
 * because it is either data for another operation or is
 * a feature that is not yet supported by GemFire
 *
 * @author Eric Zoerner
 */
public class ASTUnsupported extends GemFireAST {
  private static final long serialVersionUID = -1192307218047393827L;
  
  public ASTUnsupported() {  
  }
  
  public ASTUnsupported(Token t) {
    super(t);
  }
  
  @Override
  public void compile(QCompiler compiler) {
    throw new UnsupportedOperationException(LocalizedStrings.ASTUnsupported_UNSUPPORTED_FEATURE_0.toLocalizedString(getText()));
  }
}
