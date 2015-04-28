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
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.internal.Assert;


public class ASTType extends GemFireAST {
  private static final long serialVersionUID = 6155481284905422722L;
  private ObjectType javaType = null;
  private String typeName = null; // to be resolved
  
  public ASTType() { }
  
  public ASTType(Token t) {
    super(t);
  }
  
  public void setJavaType(ObjectType javaType) {
    this.javaType = javaType;
  }
  
  public void setTypeName(String typeName) {
    // type name to be resolved
    this.typeName = typeName;
  }
  
  public ObjectType getJavaType() {
    return this.javaType;
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
    // resolve children type nodes if any, pushing on stack first
    // collections are pushed as CollectionTypes, but elementTypes are not yet resolved (set to OBJECT_TYPE)
    super.compile(compiler);
    
    Assert.assertTrue(this.javaType != null ^ this.typeName != null);
    if (this.typeName != null) {
      this.javaType = compiler.resolveType(this.typeName);
    }    
    
    compiler.push(this.javaType);
  }
  
  
}
