/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;

public class CreateDataSourceInterceptor extends UsernamePasswordInterceptor {

  static final String POOL_PROPERTIES_ONLY_VALID_ON_POOLED_DATA_SOURCE =
      "The --pool-properties option is only valid on --pooled data sources.";
  static final String POOLED_DATA_SOURCE_FACTORY_CLASS_ONLY_VALID_ON_POOLED_DATA_SOURCE =
      "The --pooled-data-source-factory-class option is only valid on --pooled data sources.";

  public CreateDataSourceInterceptor() {
    super();
  }

  // constructor for unit test
  CreateDataSourceInterceptor(Gfsh gfsh) {
    super(gfsh);
  }

  @Override
  public ResultModel preExecution(GfshParseResult parseResult) {
    String pooled = parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED);
    if (pooled != null && pooled.equalsIgnoreCase("false")) {
      String poolProperties =
          parseResult.getParamValueAsString(CreateDataSourceCommand.POOL_PROPERTIES);
      if (poolProperties != null && poolProperties.length() > 0) {
        return ResultModel.createError(POOL_PROPERTIES_ONLY_VALID_ON_POOLED_DATA_SOURCE);
      }
      String pooledDataSourceFactoryClass = parseResult
          .getParamValueAsString(CreateDataSourceCommand.POOLED_DATA_SOURCE_FACTORY_CLASS);
      if (pooledDataSourceFactoryClass != null && pooledDataSourceFactoryClass.length() > 0) {
        return ResultModel.createError(
            POOLED_DATA_SOURCE_FACTORY_CLASS_ONLY_VALID_ON_POOLED_DATA_SOURCE);
      }
    }

    return super.preExecution(parseResult);
  }

}
