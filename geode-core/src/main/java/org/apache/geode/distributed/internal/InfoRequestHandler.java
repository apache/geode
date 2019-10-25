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

package org.apache.geode.distributed.internal;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.tcpserver.InfoResponse;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.GemFireVersion;

public class InfoRequestHandler implements RestartableTcpHandler {
  public InfoRequestHandler() {}

  @Override
  public void restarting(final DistributedSystem ds, final GemFireCache cache,
      final InternalConfigurationPersistenceService sharedConfig) {

  }

  @Override
  public Object processRequest(final Object request) throws IOException {
    String[] info = new String[2];
    info[0] = System.getProperty("user.dir");

    URL url = GemFireVersion.getJarURL();
    if (url == null) {
      String s = "Could not find gemfire jar";
      throw new IllegalStateException(s);
    }

    File gemfireJar = new File(url.getPath());
    File lib = gemfireJar.getParentFile();
    File product = lib.getParentFile();
    info[1] = product.getAbsolutePath();

    return new InfoResponse(info);
  }

  @Override
  public void endRequest(final Object request, final long startTime) {

  }

  @Override
  public void endResponse(final Object request, final long startTime) {

  }

  @Override
  public void shutDown() {

  }

  @Override
  public void init(final TcpServer tcpServer) {

  }
}
