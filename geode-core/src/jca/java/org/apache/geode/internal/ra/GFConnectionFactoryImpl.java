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
package org.apache.geode.internal.ra;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ManagedConnectionFactory;

import org.apache.geode.ra.GFConnectionFactory;

public class GFConnectionFactoryImpl implements GFConnectionFactory {
  private final ConnectionManager cm;

  private final ManagedConnectionFactory mcf;

  private Reference ref;

  public GFConnectionFactoryImpl(ManagedConnectionFactory mcf) {
    cm = null;
    this.mcf = mcf;
  }

  public GFConnectionFactoryImpl(ConnectionManager cm, ManagedConnectionFactory mcf) {
    this.cm = cm;
    this.mcf = mcf;
  }

  @Override
  public GFConnectionImpl getConnection() throws ResourceException {
    return (GFConnectionImpl) cm.allocateConnection(mcf, null);
  }

  @Override
  public void setReference(Reference ref) {
    this.ref = ref;

  }

  @Override
  public Reference getReference() throws NamingException {
    return ref;
  }

}
