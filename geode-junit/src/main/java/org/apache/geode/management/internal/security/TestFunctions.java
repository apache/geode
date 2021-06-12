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

package org.apache.geode.management.internal.security;

import static org.apache.geode.security.ResourcePermission.Operation.READ;
import static org.apache.geode.security.ResourcePermission.Resource.DATA;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.security.ResourcePermission;

public final class TestFunctions implements Serializable {
  public static class WriteFunction implements Function<Object> {
    public static final String SUCCESS_OUTPUT = "writeDataFunctionSucceeded";

    @Override
    public void execute(FunctionContext<Object> context) {
      verifyPrincipal(context);
      context.getResultSender().lastResult(SUCCESS_OUTPUT);
    }

    @Override
    public String getId() {
      return "writeData";
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }
  }

  public static class ReadFunction implements Function<Object> {
    public static final String SUCCESS_OUTPUT = "readDataFunctionSucceeded";

    @Override
    public void execute(FunctionContext<Object> context) {
      verifyPrincipal(context);
      context.getResultSender().lastResult(SUCCESS_OUTPUT);
    }

    @Override
    public Collection<ResourcePermission> getRequiredPermissions(String onRegion) {
      return Collections.singletonList(new ResourcePermission(DATA, READ, onRegion));
    }

    @Override
    public String getId() {
      return "readData";
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }
  }

  private static void verifyPrincipal(FunctionContext<Object> context) {
    String principal = (String) context.getPrincipal();
    assertThat(principal).as("Principal cannot be null").isNotNull();
    assertThat(principal).startsWith("data");

  }
}
