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
package com.company.app;

import com.gemstone.gemfire.cache.*;

/**
 * com.company.app.OrdersCacheListener. Cache listener impl for CacheXmlxxTest
 *
 * @since GemFire 5.0
 */
public class OrdersCacheListener implements CacheListener, Declarable {

  public OrdersCacheListener() {}

  public void afterCreate(EntryEvent event) {}

  public void afterUpdate(EntryEvent event) {}

  public void afterInvalidate(EntryEvent event) {}

  public void afterDestroy(EntryEvent event) {}

  public void afterRegionInvalidate(RegionEvent event) {}

  public void afterRegionDestroy(RegionEvent event) {}

  public void afterRegionClear(RegionEvent event) {}
  
  public void afterRegionCreate(RegionEvent event) {}
  
  public void afterRegionLive(RegionEvent event) {}
  
  public void close() {}
  
  public void init(java.util.Properties props) {}
}
