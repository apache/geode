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
package com.gemstone.gemfire.modules.util;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

import java.util.Properties;

@SuppressWarnings("unchecked")
public class DebugCacheListener extends CacheListenerAdapter implements Declarable {

  public void afterCreate(EntryEvent event) {
    log(event);
  }

  public void afterUpdate(EntryEvent event) {
    log(event);
  }

  public void afterInvalidate(EntryEvent event) {
    log(event);
  }

  public void afterDestroy(EntryEvent event) {
    log(event);
  }

  private void log(EntryEvent event) {
    StringBuilder builder = new StringBuilder();
    builder.append("DebugCacheListener: Received ")
        .append(event.getOperation())
        .append(" for key=")
        .append(event.getKey());
    if (event.getNewValue() != null) {
      builder.append("; value=").append(event.getNewValue());
    }
    event.getRegion().getCache().getLogger().info(builder.toString());
  }

  public void init(Properties p) {
  }

  public boolean equals(Object obj) {
    // This method is only implemented so that RegionCreator.validateRegion works properly.
    // The CacheListener comparison fails because two of these instances are not equal.
    if (this == obj) {
      return true;
    }

    if (obj == null || !(obj instanceof DebugCacheListener)) {
      return false;
    }

    return true;
  }
}
