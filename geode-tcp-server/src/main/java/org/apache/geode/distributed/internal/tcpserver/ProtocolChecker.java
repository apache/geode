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

import java.io.DataInputStream;
import java.net.Socket;

/**
 * ProtocolChecker checks the given byte to determine whether it is a valid communication
 * mode. A ProtocolChecker may optionally handle all communication on the given socket
 * and return a true value. Otherwise if the given byte is a valid communication mode the
 * checker should return false.
 */
public interface ProtocolChecker {
  boolean checkProtocol(Socket socket, DataInputStream input,
      int firstByte) throws Exception;
}
