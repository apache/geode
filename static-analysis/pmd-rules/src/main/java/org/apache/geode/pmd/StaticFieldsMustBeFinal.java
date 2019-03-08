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
package org.apache.geode.pmd;

import net.sourceforge.pmd.lang.java.ast.ASTFieldDeclaration;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;

public class StaticFieldsMustBeFinal extends AbstractJavaRule {
  @Override
  public Object visit(ASTFieldDeclaration node, Object data) {
    if (isMutable(node)) {
      addViolation(data, node);
    }
    return super.visit(node, data);
  }

  private boolean isMutable(ASTFieldDeclaration node) {
    return node.isStatic() && !node.isFinal() && !Annotations.hasDocumentedAnnotation(node);
  }
}
