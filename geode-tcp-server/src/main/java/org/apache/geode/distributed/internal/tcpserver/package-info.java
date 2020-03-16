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
/**
 * The tcpserver package implements a request/response framework that is used by Geode's
 * Locator service to process location requests. A TcpServer listens on a TCP/IP
 * server socket and passes requests to the TcpHandler installed in the server. A TcpClient
 * can be used to communicate with a TcpServer.
 * <p>
 * The tcpserver package also provides TcpSocketCreator and its dependent interfaces
 * ClientSocketCreator, ClusterSocketCreator and AdvancedSocketCreator. Geode has versions
 * of these in a higher-level package that support TLS as configured via Geode's "ssl"
 * properties.
 * <p>
 * You can create a TcpSocketCreator with the implementation TcpSocketCreatorImpl.
 */
package org.apache.geode.distributed.internal.tcpserver;
