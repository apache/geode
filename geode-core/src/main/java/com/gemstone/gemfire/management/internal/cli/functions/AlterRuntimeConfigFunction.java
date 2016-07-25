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
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/****
 * 
 *
 */
public class AlterRuntimeConfigFunction extends FunctionAdapter implements
InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    String memberId = "";

    try {
      Object arg = context.getArguments();
      Cache trueCache = null;
      GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
      DistributionConfig config = cache.getSystem().getConfig();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();

      Map<String, String> runtimeAttributes = (Map<String, String>)arg;
      Set<Entry<String,String>> entries = runtimeAttributes.entrySet();
      
      for (Entry<String, String> entry : entries) {
        String attributeName = entry.getKey();
        String attributeValue = entry.getValue();
        
        if (attributeName.equals(CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ)) {
          cache.setCopyOnRead(Boolean.parseBoolean(attributeValue));
        } else if (attributeName.equals(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE)) {
          cache.setLockLease(Integer.parseInt(attributeValue));
        } else if (attributeName.equals(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT)) {
          int lockTimeout = Integer.parseInt(attributeValue);
          cache.setLockTimeout(lockTimeout);
        } else if (attributeName.equals(CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT)) {
          cache.setSearchTimeout(Integer.parseInt(attributeValue));
        } else if (attributeName.equals(CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL)) {
          cache.setMessageSyncInterval(Integer.parseInt(attributeValue));
        } else {
          config.setAttribute(attributeName, attributeValue, ConfigSource.runtime());
        }
      }
      
      CliFunctionResult cliFuncResult = new CliFunctionResult(memberId, true, null);
      context.getResultSender().lastResult(cliFuncResult);
      
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);
      
    } catch (Exception e){
      CliFunctionResult cliFuncResult = new CliFunctionResult(memberId, e, CliUtil.stackTraceAsString(e));
      context.getResultSender().lastResult(cliFuncResult);    
    }
  }

  @Override
  public String getId() {
    return AlterRuntimeConfigFunction.class.getName();
  }
}
