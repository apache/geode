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
package org.apache.geode.distributed.internal.tcpserver;

import static org.apache.geode.internal.serialization.DataSerializableFixedID.HOST_AND_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class HostAndPortTest {

  /**
   * Test that getSocketInentAddress returns resolved InetSocketAddress
   */
  @Test
  public void getSocketInetAddress_returns_resolved_SocketAddress() {
    HostAndPort locator1 = new HostAndPort("localhost", 8080);

    InetSocketAddress actual = locator1.getSocketInetAddress();

    assertThat(actual.isUnresolved()).isFalse();
  }

  /**
   * Test that getSocketInentAddress returns unresolved InetSocketAddress
   */
  @Test
  public void getSocketInentAddress_returns_unresolved_SocketAddress() {
    HostAndPort locator1 = new HostAndPort("fakelocalhost", 8090);

    InetSocketAddress actual = locator1.getSocketInetAddress();

    assertThat(actual.isUnresolved())
        .as("Hostname resolved unexpectedly. Check for DNS hijacking in addition to code errors.")
        .isTrue();
  }

  /**
   * Test whether HostAndPort are equal, when created from resolved and unresolved
   * InetSocketAddress
   */
  @Test
  public void equals_LocatorAddress_from_resolved_and_unresolved_SocketAddress() {
    HostAndPort locator1 = new HostAndPort("localhost", 8080);

    InetSocketAddress host2address = locator1.getSocketInetAddress();
    HostAndPort locator2 = new HostAndPort("localhost", host2address.getPort());

    assertThat(host2address.isUnresolved()).isFalse();
    assertThat(locator1.equals(locator2)).isTrue();
  }

  @Test
  public void getPort_returns_port() {
    HostAndPort locator1 = new HostAndPort("localhost", 8090);
    assertThat(locator1.getPort()).isEqualTo(8090);
  }

  @Test
  public void getHostName_returns_hostname() {
    HostAndPort locator1 = new HostAndPort("fakelocalhost", 8091);
    assertThat(locator1.getHostName()).isEqualTo("fakelocalhost");
  }

  @Test
  public void hashCode_of_SocketAddress() {
    InetSocketAddress host1address = InetSocketAddress.createUnresolved("fakelocalhost", 8091);
    HostAndPort locator1 = new HostAndPort("fakelocalhost", 8091);
    assertThat(locator1.hashCode()).isEqualTo(host1address.hashCode());
  }

  @Test
  public void constructorWithNoHostName() {
    HostAndPort hostAndPort = new HostAndPort(null, 8091);
    assertThat(hostAndPort.getAddress()).isNotNull();
    assertThat(hostAndPort.getHostName()).isEqualTo("0.0.0.0");
    assertThat(hostAndPort.getPort()).isEqualTo(8091);
    assertThat(hostAndPort.getSocketInetAddress()).isNotNull();
  }

  @Test
  public void testEquality() {
    HostAndPort hostAndPort1 = new HostAndPort("127.0.0.1", 8091);
    HostAndPort hostAndPort2 = new HostAndPort("127.0.0.1", 8091);
    HostAndPort hostAndPort3 = new HostAndPort("127.0.0.1", 8092);
    assertThat(hostAndPort1.getSocketInetAddress()).isSameAs(hostAndPort1.getSocketInetAddress());
    assertThat(hostAndPort1).isEqualTo(hostAndPort1);
    assertThat(hostAndPort1).isEqualTo(hostAndPort2);
    assertThat(hostAndPort1).isNotEqualTo(hostAndPort3);
    assertThat(hostAndPort1.equals(null)).isFalse();
  }

  @Test
  public void testSerializationWithNumericAddress() throws IOException, ClassNotFoundException {
    DSFIDSerializer dsfidSerializer = new DSFIDSerializerFactory().create();
    dsfidSerializer.registerDSFID(HOST_AND_PORT, HostAndPort.class);
    HostAndPort hostAndPort1 = new HostAndPort("127.0.0.1", 8091);
    BufferDataOutputStream out = new BufferDataOutputStream(100, Version.CURRENT);
    dsfidSerializer.getObjectSerializer().writeObject(hostAndPort1, out);
    HostAndPort hostAndPort2 = dsfidSerializer.getObjectDeserializer()
        .readObject(new ByteArrayDataInput(out.toByteArray()));
    assertThat(hostAndPort1).isEqualTo(hostAndPort2);
    assertThat(hostAndPort2).isEqualTo(hostAndPort1);
  }

  @Test
  public void testSerializationWithUnresolvableHostName()
      throws IOException, ClassNotFoundException {
    DSFIDSerializer dsfidSerializer = new DSFIDSerializerFactory().create();
    dsfidSerializer.registerDSFID(HOST_AND_PORT, HostAndPort.class);
    HostAndPort hostAndPort1 = new HostAndPort("unresolvable host name", 8091);
    BufferDataOutputStream out = new BufferDataOutputStream(100, Version.CURRENT);
    dsfidSerializer.getObjectSerializer().writeObject(hostAndPort1, out);
    HostAndPort hostAndPort2 = dsfidSerializer.getObjectDeserializer()
        .readObject(new ByteArrayDataInput(out.toByteArray()));
    assertThat(hostAndPort1).isEqualTo(hostAndPort2);
    assertThat(hostAndPort2).isEqualTo(hostAndPort1);
    assertThat(hostAndPort1.getAddress())
        .as("Hostname resolved unexpectedly. Check for DNS hijacking in addition to code errors.")
        .isNull();
    assertThat(hostAndPort2.getAddress()).isNull();
    assertThat(hostAndPort2.getSocketInetAddress()).isNotNull();
    assertThat(hostAndPort1.getSocketInetAddress().isUnresolved()).isTrue();
    assertThat(hostAndPort2.getSocketInetAddress().isUnresolved()).isTrue();
  }

  @Test
  public void testSerializationWithNoHostName() throws IOException, ClassNotFoundException {
    DSFIDSerializer dsfidSerializer = new DSFIDSerializerFactory().create();
    dsfidSerializer.registerDSFID(HOST_AND_PORT, HostAndPort.class);
    HostAndPort hostAndPort1 = new HostAndPort(null, 8091);
    BufferDataOutputStream out = new BufferDataOutputStream(100, Version.CURRENT);
    dsfidSerializer.getObjectSerializer().writeObject(hostAndPort1, out);
    HostAndPort hostAndPort2 = dsfidSerializer.getObjectDeserializer()
        .readObject(new ByteArrayDataInput(out.toByteArray()));
    assertThat(hostAndPort1).isEqualTo(hostAndPort2);
    assertThat(hostAndPort2).isEqualTo(hostAndPort1);
    assertThat(hostAndPort2.getHostName()).isEqualTo("0.0.0.0");
    assertThat(hostAndPort2.getSocketInetAddress()).isNotNull();
    assertThat(hostAndPort1.getSocketInetAddress().isUnresolved()).isFalse();
    assertThat(hostAndPort2.getSocketInetAddress().isUnresolved()).isFalse();
  }
}
