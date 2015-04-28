/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * Sent to a locator to request it to find (and possibly start)
 * a jmx manager for us. It returns a JmxManagerLocatorResponse.
 * 
 * @author darrel
 * @since 7.0
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
