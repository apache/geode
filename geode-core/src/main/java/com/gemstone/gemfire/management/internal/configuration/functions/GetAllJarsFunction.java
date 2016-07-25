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
package com.gemstone.gemfire.management.internal.configuration.functions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.configuration.domain.Configuration;

public class GetAllJarsFunction extends FunctionAdapter implements
    InternalEntity {
  
  private static final long serialVersionUID = 1L;

  public GetAllJarsFunction() {
  }

  @Override
  public void execute(FunctionContext context)  {
    InternalLocator locator = (InternalLocator) Locator.getLocator();
    
    if (locator != null) {
      SharedConfiguration sharedConfig = locator.getSharedConfiguration();
      if (sharedConfig != null) {
        try {
          Map<String, Configuration> entireConfig  = sharedConfig.getEntireConfiguration();
          Set<String> configNames = entireConfig.keySet();
          context.getResultSender().lastResult(sharedConfig.getAllJars(configNames));
        } catch (IOException e) {
          context.getResultSender().sendException(e);
        } catch (Exception e) {
          context.getResultSender().sendException(e);
        }
      }
    }
    
    context.getResultSender().lastResult(null);
  }

  @Override
  public String getId() {
    return GetAllJarsFunction.class.getName();
  }

}
