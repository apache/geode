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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.util.internal.GeodeGlossary;

public class SocketMessageWriter {
  private static final int CLIENT_PING_TASK_PERIOD =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "serverToClientPingPeriod", 60000);

  public void writeHandshakeMessage(DataOutputStream dos, byte type, String p_msg,
      @Nullable KnownVersion clientVersion, byte endpointType, int queueSize)
      throws IOException {
    String msg = p_msg;

    // write the message type
    dos.writeByte(type);

    dos.writeByte(endpointType);
    dos.writeInt(queueSize);

    if (msg == null) {
      msg = "";
    }
    dos.writeUTF(msg);

    if (clientVersion != null) {
      // get all the instantiators.
      Instantiator[] instantiators = InternalInstantiator.getInstantiators();
      Map<Integer, List<String>> instantiatorMap = new HashMap<>();
      if (instantiators != null && instantiators.length > 0) {
        for (Instantiator instantiator : instantiators) {
          List<String> instantiatorAttributes = new ArrayList<>();
          instantiatorAttributes.add(instantiator.getClass().toString().substring(6));
          instantiatorAttributes.add(instantiator.getInstantiatedClass().toString().substring(6));
          instantiatorMap.put(instantiator.getId(), instantiatorAttributes);
        }
      }
      DataSerializer.writeHashMap(instantiatorMap, dos);

      // get all the dataserializers.
      DataSerializer[] dataSerializers = InternalDataSerializer.getSerializers();
      HashMap<Integer, ArrayList<String>> dsToSupportedClasses = new HashMap<>();
      HashMap<Integer, String> dataSerializersMap = new HashMap<>();
      if (dataSerializers != null && dataSerializers.length > 0) {
        for (DataSerializer dataSerializer : dataSerializers) {
          dataSerializersMap.put(dataSerializer.getId(),
              dataSerializer.getClass().toString().substring(6));
          ArrayList<String> supportedClassNames = new ArrayList<>();
          for (Class<?> clazz : dataSerializer.getSupportedClasses()) {
            supportedClassNames.add(clazz.getName());
          }
          dsToSupportedClasses.put(dataSerializer.getId(), supportedClassNames);
        }
      }
      DataSerializer.writeHashMap(dataSerializersMap, dos);
      DataSerializer.writeHashMap(dsToSupportedClasses, dos);
      if (clientVersion.isNotOlderThan(KnownVersion.GEODE_1_5_0)) {
        dos.writeInt(CLIENT_PING_TASK_PERIOD);
      }
    }
    dos.flush();
  }

  /**
   * Writes an exception message to the socket
   *
   * @param dos the <code>DataOutputStream</code> to use for writing the message
   * @param type a byte representing the exception type
   * @param ex the exception to be written; should not be null
   */
  public void writeException(DataOutputStream dos, byte type, Exception ex,
      KnownVersion clientVersion)
      throws IOException {
    writeHandshakeMessage(dos, type, ex.toString(), clientVersion, (byte) 0x00, 0);
  }
}
