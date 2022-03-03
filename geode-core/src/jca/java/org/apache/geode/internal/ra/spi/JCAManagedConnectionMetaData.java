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
package org.apache.geode.internal.ra.spi;

import javax.resource.ResourceException;
import javax.resource.spi.ManagedConnectionMetaData;

public class JCAManagedConnectionMetaData implements ManagedConnectionMetaData {
  private final String prodName;

  private final String version;

  private final String user;

  public JCAManagedConnectionMetaData(String prodName, String version, String user) {
    this.prodName = prodName;
    this.version = version;
    this.user = user;
  }


  @Override
  public String getEISProductName() throws ResourceException {
    return prodName;
  }


  @Override
  public String getEISProductVersion() throws ResourceException {

    return version;
  }


  @Override
  public int getMaxConnections() throws ResourceException {

    return 0;
  }


  @Override
  public String getUserName() throws ResourceException {
    return user;
  }

}
