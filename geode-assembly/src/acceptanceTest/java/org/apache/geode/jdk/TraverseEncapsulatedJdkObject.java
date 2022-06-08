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
 *
 */

package org.apache.geode.jdk;

import java.math.BigDecimal;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.size.ObjectTraverser;
import org.apache.geode.internal.size.ObjectTraverser.Visitor;

public class TraverseEncapsulatedJdkObject implements Function<Void> {
  private static final Visitor TRAVERSE_ENTIRE_OBJECT_GRAPH = (parent, object) -> true;
  private final ObjectTraverser traverser = new ObjectTraverser();

  // OBJECT must have a JDK type with inaccessible fields, defined in a package that Gfsh does
  // not open by default.
  static final BigDecimal OBJECT = BigDecimal.ONE;
  // MODULE must be the module that defines OBJECT's type.
  static final String MODULE = "java.base";
  static final String ID = "traverse-big-decimal";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext<Void> context) {
    try {
      traverser.breadthFirstSearch(OBJECT, TRAVERSE_ENTIRE_OBJECT_GRAPH, false);
    } catch (IllegalAccessException e) {
      context.getResultSender().sendException(e);
      return;
    }
    context.getResultSender().lastResult("OK");
  }
}
