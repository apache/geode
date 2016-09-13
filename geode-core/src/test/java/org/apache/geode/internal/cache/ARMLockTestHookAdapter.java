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
package org.apache.geode.internal.cache;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import org.apache.geode.cache.CacheEvent;
import org.apache.geode.test.dunit.VM;

public class ARMLockTestHookAdapter implements AbstractRegionMap.ARMLockTestHook, Serializable {

  public void beforeBulkLock(LocalRegion region) {};
  public void afterBulkLock(LocalRegion region) {};
  public void beforeBulkRelease(LocalRegion region) {};
  public void afterBulkRelease(LocalRegion region) {};

  public void beforeLock(LocalRegion region, CacheEvent event) {};
  public void afterLock(LocalRegion region, CacheEvent event) {};
  public void beforeRelease(LocalRegion region, CacheEvent event) {};
  public void afterRelease(LocalRegion region, CacheEvent event) {};

  public void beforeStateFlushWait() {}
}
