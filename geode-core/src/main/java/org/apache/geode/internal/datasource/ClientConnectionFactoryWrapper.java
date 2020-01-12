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
package org.apache.geode.internal.datasource;

/**
 * This class wraps the client connection factory and the corresponding connection manager Object.
 *
 */
public class ClientConnectionFactoryWrapper implements AutoCloseable {

  private Object clientConnFac;
  private Object manager;

  /**
   * Constructor.
   */
  public ClientConnectionFactoryWrapper(Object connFac, Object man) {
    this.clientConnFac = connFac;
    this.manager = man;
  }

  @Override
  public void close() {
    if (manager instanceof JCAConnectionManagerImpl) {
      ((JCAConnectionManagerImpl) this.manager).clearUp();
    } else if (manager instanceof FacetsJCAConnectionManagerImpl) {
      ((FacetsJCAConnectionManagerImpl) this.manager).clearUp();
    }
  }

  public Object getClientConnFactory() {
    return clientConnFac;
  }

  public Object getConnectionManager() {
    return this.manager;
  }
}
