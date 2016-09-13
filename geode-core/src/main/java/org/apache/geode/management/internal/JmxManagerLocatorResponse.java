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
package org.apache.geode.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

/**
 * Sent to a locator to request it to find (and possibly start)
 * a jmx manager for us. It returns a JmxManagerLocatorResponse.
 * 
 * @since GemFire 7.0
 *
 */
public class JmxManagerLocatorResponse implements DataSerializableFixedID {
  private String host;
  private int port;
  private boolean ssl;
  private Throwable ex;

  public JmxManagerLocatorResponse(String host, int port, boolean ssl, Throwable ex) {
    this.host = host;
    this.port = port;
    this.ssl = ssl;
    this.ex = ex;
  }
  
  public JmxManagerLocatorResponse() {
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.host = DataSerializer.readString(in);
    this.port = DataSerializer.readPrimitiveInt(in);
    this.ssl = DataSerializer.readPrimitiveBoolean(in);
    this.ex = DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.host, out);
    DataSerializer.writePrimitiveInt(this.port, out);
    DataSerializer.writePrimitiveBoolean(this.ssl, out);
    DataSerializer.writeObject(this.ex, out);
  }

  public int getDSFID() {
    return DataSerializableFixedID.JMX_MANAGER_LOCATOR_RESPONSE;
  }

  @Override
  public String toString() {
    return "JmxManagerLocatorResponse [host=" + host + ", port=" + port
        + ", ssl=" + ssl + ", ex=" + ex + "]";
  }

  public String getHost() {
    try {
      // try to convert it to a symbolic name known by this machine
      return InetAddress.getByName(this.host).getHostName();
    } catch (UnknownHostException e) {
      // Just return the numeric ip address.
      return this.host;
    }
  }

  public int getPort() {
    return this.port;
  }
  
  public Throwable getException() {
    return this.ex;
  }

  public boolean isJmxManagerSslEnabled() {
    return ssl;
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
