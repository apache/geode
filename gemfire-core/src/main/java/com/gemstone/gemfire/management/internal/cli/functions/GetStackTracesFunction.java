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
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.management.internal.cli.domain.StackTracesPerMember;

public class GetStackTracesFunction extends FunctionAdapter implements
    InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      String memberNameOrId = cache.getDistributedSystem().getDistributedMember().getName();
      
      if (memberNameOrId == null) {
        memberNameOrId = cache.getDistributedSystem().getDistributedMember().getId();
      }
      StackTracesPerMember stackTracePerMember = new StackTracesPerMember(memberNameOrId, OSProcess.zipStacks());
      context.getResultSender().lastResult(stackTracePerMember);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    } 
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return GetStackTracesFunction.class.getName();
  }
}
