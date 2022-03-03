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
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.Assert;


public class ASTType extends GemFireAST {
  private static final long serialVersionUID = 6155481284905422722L;
  private ObjectType javaType = null;
  private String typeName = null; // to be resolved

  public ASTType() {}

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
    return javaType;
  }


  @Override
  public void compile(QCompiler compiler) {
    // resolve children type nodes if any, pushing on stack first
    // collections are pushed as CollectionTypes, but elementTypes are not yet resolved (set to
    // OBJECT_TYPE)
    super.compile(compiler);

    Assert.assertTrue(javaType != null ^ typeName != null);
    if (typeName != null) {
      javaType = compiler.resolveType(typeName);
    }

    compiler.push(javaType);
  }


}
