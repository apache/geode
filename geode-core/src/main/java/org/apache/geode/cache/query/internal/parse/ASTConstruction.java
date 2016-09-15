/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.geode.cache.query.internal.parse;

import antlr.*;
import org.apache.geode.cache.query.internal.*;
import org.apache.geode.internal.Assert;

public class ASTConstruction extends GemFireAST {  
  private static final long serialVersionUID = 6647545354866647845L;
  
  public ASTConstruction() { }
  
  
  public ASTConstruction(Token t) {
    super(t);
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    // we only support "SET" for now
    Assert.assertTrue("set".equalsIgnoreCase(getText()));
    // left argList on stack
    compiler.constructObject(ResultsSet.class);
  }
  
}
