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

package org.apache.geode.management.internal.cli.functions;


import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.commands.IndexDefinition;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class ManageIndexDefinitionFunction extends CliFunction<RegionConfig.Index> {
  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.ManageIndexDefinitionFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<RegionConfig.Index> context)
      throws Exception {
    RegionConfig.Index definedIndex = context.getArguments();
    // this is called by the ClearDefinedIndexCommand
    if (definedIndex == null) {
      IndexDefinition.indexDefinitions.clear();
    }
    // this is call by the DefineIndexCommand
    else {
      IndexDefinition.indexDefinitions.add(definedIndex);
    }
    return new CliFunctionResult(context.getMemberName(), true, "");
  }
}
