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
package com.gemstone.gemfire.modules.hibernate;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

public class GemFireCacheListener extends CacheListenerAdapter implements
    Declarable {

  @Override
  public void afterCreate(EntryEvent event) {
    System.out.println("Create : " + event.getKey() + " / "
        + event.getNewValue());
  }

  @Override
  public void afterDestroy(EntryEvent event) {
    System.out.println("Destroy : " + event.getKey());
  }

  @Override
  public void afterInvalidate(EntryEvent event) {
    System.out.println("Invalidate : " + event.getKey());
  }

  @Override
  public void afterUpdate(EntryEvent event) {
    System.out.println("Update : " + event.getKey() + " / "
        + event.getNewValue());
  }

  public void init(Properties props) {

  }

}
