/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.launcher;


import java.net.InetAddress;

public interface ServerLauncherConfig {

  boolean isAssignBuckets();

  boolean isDebugging();

  boolean isDisableDefaultServer();

  boolean isForcing();

  boolean isRebalancing();

  boolean isRedirectingOutput();

  String getHostNameForClients();

  String getMemberName();

  InetAddress getServerBindAddress();

  Integer getServerPort();

  String getSpringXmlLocation();

  Float getCriticalHeapPercentage();

  Float getEvictionHeapPercentage();

  Float getCriticalOffHeapPercentage();

  Float getEvictionOffHeapPercentage();

  Integer getMaxConnections();

  Integer getMaxMessageCount();

  Integer getMaxThreads();

  Integer getMessageTimeToLive();

  Integer getSocketBufferSize();

  boolean isSpringXmlLocationSpecified();

  String getWorkingDirectory();

  ServiceInfo status();

  interface ServiceInfo {

    static boolean isStartingNotRespondingOrNull(ServiceInfo serverState) {
      return serverState == null || serverState.isStartingOrNotResponding();
    }

    boolean isStartingOrNotResponding();

    String getStatusMessage();

    String toJson();
  }

  interface Builder {

    ServerLauncherConfig build();

    Boolean getAssignBuckets();

    Boolean getDisableDefaultServer();

    Boolean getForce();

    Integer getPid();

    Boolean getRebalance();

    Boolean getRedirectOutput();

    InetAddress getServerBindAddress();

    Integer getServerPort();

    String getSpringXmlLocation();

    String getWorkingDirectory();

    Float getCriticalHeapPercentage();

    Float getEvictionHeapPercentage();

    Float getCriticalOffHeapPercentage();

    Float getEvictionOffHeapPercentage();

    Integer getMaxConnections();

    Integer getMaxMessageCount();

    Integer getMaxThreads();

    Integer getMessageTimeToLive();

    Integer getSocketBufferSize();

    String getHostNameForClients();

    String getMemberName();

    Builder setAssignBuckets(Boolean assignBuckets);

    Builder setDisableDefaultServer(Boolean disableDefaultServer);

    Builder setForce(Boolean force);

    Builder setPid(Integer pid);

    Builder setRebalance(Boolean rebalance);

    Builder setRedirectOutput(Boolean redirectOutput);

    Builder setServerBindAddress(String serverBindAddress);

    Builder setServerPort(Integer serverPort);

    Builder setSpringXmlLocation(String springXmlLocation);

    Builder setWorkingDirectory(String workingDirectory);

    Builder setCriticalHeapPercentage(Float criticalHeapPercentage);

    Builder setEvictionHeapPercentage(Float evictionHeapPercentage);

    Builder setCriticalOffHeapPercentage(Float criticalOffHeapPercentage);

    Builder setEvictionOffHeapPercentage(Float evictionOffHeapPercentage);

    Builder setMaxConnections(Integer maxConnections);

    Builder setMaxMessageCount(Integer maxMessageCount);

    Builder setMaxThreads(Integer maxThreads);

    Builder setMessageTimeToLive(Integer messageTimeToLive);

    Builder setSocketBufferSize(Integer socketBufferSize);

    Builder setHostNameForClients(String hostNameForClients);

    Builder setMemberName(String memberName);

    Boolean getDebug();

    Builder setDebug(Boolean debug);

    Boolean getDeletePidFileOnStop();

    Builder setDeletePidFileOnStop(Boolean deletePidFileOnStop);
  }
}
