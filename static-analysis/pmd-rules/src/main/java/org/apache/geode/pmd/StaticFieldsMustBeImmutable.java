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

import static org.apache.geode.pmd.Annotations.hasDocumentedAnnotation;

import java.io.File;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import net.sourceforge.pmd.lang.java.ast.ASTFieldDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTVariableDeclaratorId;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;
import net.sourceforge.pmd.lang.java.symboltable.VariableNameDeclaration;

public class StaticFieldsMustBeImmutable extends AbstractJavaRule {
  private static final Set<String> immutableTypes =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          String.class.getSimpleName(),
          ThreadLocal.class.getSimpleName(),
          Object.class.getSimpleName(),
          AtomicLongFieldUpdater.class.getSimpleName(),
          AtomicIntegerFieldUpdater.class.getSimpleName(),
          AtomicReferenceFieldUpdater.class.getSimpleName(),
          BigInteger.class.getSimpleName(),
          Number.class.getSimpleName(),
          Integer.class.getSimpleName(),
          Double.class.getSimpleName(),
          File.class.getSimpleName(),
          InetAddress.class.getSimpleName(),
          Pattern.class.getSimpleName(),
          "Logger")));

  @Override
  public Object visit(ASTFieldDeclaration node, Object data) {
    if (isMutable(node)) {
      addViolation(data, node);
    }
    return super.visit(node, data);
  }

  private boolean isMutable(ASTFieldDeclaration node) {
    return node.isStatic() && !hasDocumentedAnnotation(node) && hasMutableVariable(node);
  }

  private boolean hasMutableVariable(ASTFieldDeclaration node) {
    return StreamSupport.stream(node.spliterator(), false)
        .anyMatch(this::isMutable);
  }

  private boolean isMutable(ASTVariableDeclaratorId variable) {
    VariableNameDeclaration nameDeclaration = variable.getNameDeclaration();
    return !(nameDeclaration.isPrimitiveType()
        || immutableTypes.contains(nameDeclaration.getTypeImage()));
  }
}
