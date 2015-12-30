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
package com.gemstone.gemfire.modules.session.catalina.callback;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;

import javax.servlet.http.HttpSession;
import java.util.Properties;

public class LocalSessionCacheWriter extends CacheWriterAdapter<String, HttpSession> implements Declarable {

  private final Region<String, HttpSession> backingRegion;

  public LocalSessionCacheWriter(Region<String, HttpSession> backingRegion) {
    this.backingRegion = backingRegion;
  }

  public void beforeCreate(EntryEvent<String, HttpSession> event) throws CacheWriterException {
    this.backingRegion.put(event.getKey(), event.getNewValue(), event.getCallbackArgument());
  }

  public void beforeUpdate(EntryEvent<String, HttpSession> event) throws CacheWriterException {
    this.backingRegion.put(event.getKey(), event.getNewValue(), event.getCallbackArgument());
  }

  public void beforeDestroy(EntryEvent<String, HttpSession> event) throws CacheWriterException {
    try {
      this.backingRegion.destroy(event.getKey(), event.getCallbackArgument());
    } catch (EntryNotFoundException e) {
      // I think it is safe to ignore this exception. The entry could have
      // expired already in the backing region.
    }
  }

  public void close() {
  }

  public void init(Properties p) {
  }
}
