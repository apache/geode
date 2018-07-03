/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.snapshot;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;

public class ClientExporterTest {

  @Test
  public void proxyExportFunctionGetsRequestedRegion() {
    Cache cache = spy(new CacheFactory().set("locators", "").set("mcast-port", "0").create());
    cache.createRegionFactory(RegionShortcut.PARTITION).create("testRegion");
    ResultSender resultSender = mock(ResultSender.class);
    ClientExporter.ClientArgs<String, String> args = mock(ClientExporter.ClientArgs.class);
    when(args.isPRSingleHop()).thenReturn(true);
    when(args.getRegion()).thenReturn("testRegion");
    FunctionContext context = new FunctionContextImpl(cache, "id", args, resultSender);

    new ClientExporter.ProxyExportFunction<String, String>().execute(context);
    verify(cache).getRegion(any());
    cache.close();
  }
}
