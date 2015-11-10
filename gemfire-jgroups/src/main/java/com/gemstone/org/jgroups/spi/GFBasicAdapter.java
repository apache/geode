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
package com.gemstone.org.jgroups.spi;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.SockCreator;

public interface GFBasicAdapter {
    void invokeToData(Object obj, DataOutput out) throws IOException;

    void writeObject(Object obj, DataOutput out) throws IOException;

    void invokeFromData(Object obj, DataInput in) throws IOException, ClassNotFoundException;

    <T> T readObject(DataInput in) throws IOException, ClassNotFoundException;

    short getMulticastVersionOrdinal();

    short getSerializationVersionOrdinal(short version);

    short getCurrentVersionOrdinal();

    byte[] serializeWithVersion(Object obj, int destVersionOrdinal);

    void serializeJGMessage(Message message, DataOutputStream out) throws IOException;

    void deserializeJGMessage(Message message, DataInputStream in) throws IOException, IllegalAccessException, InstantiationException;

    ObjectOutput getObjectOutput(DataOutputStream out) throws IOException;

    ObjectInput getObjectInput(DataInputStream in) throws IOException;

    void writeString(String str, DataOutput out) throws IOException ;

    String readString(DataInput in) throws IOException;

    void writeStringArray(String[] strings, DataOutput out) throws IOException;

    String[] readStringArray(DataInput in) throws IOException;

    DataOutputStream getVersionedDataOutputStream(DataOutputStream dos, short version) throws IOException;

    DataInputStream getVersionedDataInputStream(DataInputStream instream,
        short version) throws IOException;

    byte[] readByteArray(DataInput in) throws IOException;

    void writeByteArray(byte[] array, DataOutput out) throws IOException;

    void writeProperties(Properties props, DataOutput oos) throws IOException;

    Properties readProperties(DataInput in) throws IOException, ClassNotFoundException;

    int getGossipVersionForOrdinal(short serverOrdinal);

    boolean isVersionForStreamAtLeast(DataOutput stream, short version);

    boolean isVersionForStreamAtLeast(DataInput stream, short version);

    String getHostName(InetAddress ip_addr);

    RuntimeException getAuthenticationFailedException(String failReason);

    SockCreator getSockCreator();


    RuntimeException getSystemConnectException(String localizedString);

    Object getForcedDisconnectException(String localizedString);

    RuntimeException getDisconnectException(String localizedString);

    RuntimeException getGemFireConfigException(String string);

    void setDefaultGemFireAttributes(IpAddress local_addr);

    void setGemFireAttributes(IpAddress addr, Object attr);

    InetAddress getLocalHost() throws UnknownHostException;

    void checkDisableDNS();

    String getVmKindString(int vmKind);


}
