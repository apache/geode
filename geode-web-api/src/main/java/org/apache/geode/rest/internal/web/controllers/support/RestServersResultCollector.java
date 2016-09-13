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
package com.gemstone.gemfire.rest.internal.web.controllers.support;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.springframework.util.StringUtils;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;

public class RestServersResultCollector<String, Object> implements ResultCollector<String, Object> {

  private ArrayList resultList = new ArrayList();
  
  public void addResult(DistributedMember memberID,
      String result) {
    if(!StringUtils.isEmpty(result)){
      this.resultList.add(result);
    }
  }
  
  public void endResults() {
  }

  public Object getResult() throws FunctionException {
    return (Object)resultList;
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException {
    return (Object)resultList;
  }
  
  public void clearResults() {
    resultList.clear();
  }

}
