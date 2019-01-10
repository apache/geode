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

package org.apache.geode.management.internal.cli.functions;

import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.configuration.realizers.ConfigurationRealizer;
import org.apache.geode.management.internal.configuration.realizers.ConfigurationRealizerFactory;

public class UpdateCacheFunction extends CliFunction<List> {
  public enum CacheElementOperation {
    ADD, DELETE, UPDATE
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<List> context) throws Exception {
    CacheElement cacheElement = (CacheElement) context.getArguments().get(0);
    CacheElementOperation operation = (CacheElementOperation) context.getArguments().get(1);
    Cache cache = context.getCache();

    ConfigurationRealizer realizer =
        (new ConfigurationRealizerFactory(cache)).generate(cacheElement);
    switch (operation) {
      case ADD:
        realizer.create();
        break;
      case DELETE:
        realizer.delete();
        break;
      case UPDATE:
        realizer.update();
        break;
    }
    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        "success");
  }
}
