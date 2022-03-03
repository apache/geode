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

package org.apache.geode.management.internal;

import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import com.healthmarketscience.rmiio.RemoteStreamServer;
import com.healthmarketscience.rmiio.exporter.RemoteStreamExporter;

/**
 * This class allows for exporting RMI objects when specific RMIClientSocketFactory and
 * RMIClientSocketFactory implementations are being used.
 */
public class GeodeRemoteStreamExporter extends RemoteStreamExporter {

  private final int port;
  private final RMIClientSocketFactory clientSocketFactory;
  private final RMIServerSocketFactory serverSocketFactory;

  public GeodeRemoteStreamExporter(int port, RMIClientSocketFactory csf,
      RMIServerSocketFactory ssf) {
    this.port = port;
    clientSocketFactory = csf;
    serverSocketFactory = ssf;
  }

  @Override
  protected Object exportImpl(RemoteStreamServer<?, ?> server) throws RemoteException {
    return UnicastRemoteObject.exportObject(server, port, clientSocketFactory, serverSocketFactory);
  }

  @Override
  protected void unexportImpl(RemoteStreamServer<?, ?> server) throws Exception {
    UnicastRemoteObject.unexportObject(server, true);
  }
}
