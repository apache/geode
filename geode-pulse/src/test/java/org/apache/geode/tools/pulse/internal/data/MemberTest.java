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
package org.apache.geode.tools.pulse.internal.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class MemberTest {
  private Cluster.Member member;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    member = new Cluster.Member();
  }

  @Test
  public void updateMemberClientsHMapShouldAddClientInformationIfItDoeNotExistAlready() {
    Cluster.Client newClient = new Cluster.Client();
    newClient.setId(testName.getMethodName());
    newClient.setConnected(true);
    newClient.setGets(1);
    newClient.setPuts(2);
    newClient.setCpus(3);
    newClient.setQueueSize(4);
    newClient.setStatus("Status");
    newClient.setThreads(5);
    newClient.setClientCQCount(6);
    newClient.setSubscriptionEnabled(true);
    newClient.setUptime(7);
    newClient.setProcessCpuTime(8);
    HashMap<String, Cluster.Client> updatedClientsMap = new HashMap<>();
    updatedClientsMap.put(newClient.getId(), newClient);
    member.updateMemberClientsHMap(updatedClientsMap);

    Cluster.Client resultingClient = member.getMemberClientsHMap().get(testName.getMethodName());
    assertThat(resultingClient).isNotNull();
    assertThat(resultingClient.isConnected()).isTrue();
    assertThat(resultingClient.getGets()).isEqualTo(1);
    assertThat(resultingClient.getPuts()).isEqualTo(2);
    assertThat(resultingClient.getCpus()).isEqualTo(3);
    assertThat(resultingClient.getQueueSize()).isEqualTo(4);
    assertThat(resultingClient.getStatus()).isEqualTo("Status");
    assertThat(resultingClient.getThreads()).isEqualTo(5);
    assertThat(resultingClient.getClientCQCount()).isEqualTo(6);
    assertThat(resultingClient.isSubscriptionEnabled()).isTrue();
    assertThat(resultingClient.getUptime()).isEqualTo(7);
    assertThat(resultingClient.getProcessCpuTime()).isEqualTo(8);
  }

  @Test
  public void updateMemberClientsHMapShouldUpdateExistingClientInformation() {
    Cluster.Client existingClient = new Cluster.Client();
    existingClient.setId(testName.getMethodName());
    existingClient.setUptime(1);
    existingClient.setProcessCpuTime(1000000000);
    HashMap<String, Cluster.Client> existingClientsMap = new HashMap<>();
    existingClientsMap.put(existingClient.getId(), existingClient);
    member.setMemberClientsHMap(existingClientsMap);

    Cluster.Client updatedClient = new Cluster.Client();
    updatedClient.setId(testName.getMethodName());
    updatedClient.setConnected(true);
    updatedClient.setGets(30);
    updatedClient.setPuts(60);
    updatedClient.setCpus(5);
    updatedClient.setQueueSize(5);
    updatedClient.setStatus("Status");
    updatedClient.setThreads(100);
    updatedClient.setClientCQCount(0);
    updatedClient.setSubscriptionEnabled(true);
    updatedClient.setUptime(2);
    updatedClient.setProcessCpuTime(2000000000);
    HashMap<String, Cluster.Client> updatedClientsMap = new HashMap<>();
    updatedClientsMap.put(updatedClient.getId(), updatedClient);

    member.updateMemberClientsHMap(updatedClientsMap);
    Cluster.Client resultingClient = member.getMemberClientsHMap().get(testName.getMethodName());
    assertThat(resultingClient).isNotNull();
    assertThat(resultingClient.isConnected()).isTrue();
    assertThat(resultingClient.getGets()).isEqualTo(30);
    assertThat(resultingClient.getPuts()).isEqualTo(60);
    assertThat(resultingClient.getCpus()).isEqualTo(5);
    assertThat(resultingClient.getQueueSize()).isEqualTo(5);
    assertThat(resultingClient.getStatus()).isEqualTo("Status");
    assertThat(resultingClient.getThreads()).isEqualTo(100);
    assertThat(resultingClient.getClientCQCount()).isEqualTo(0);
    assertThat(resultingClient.isSubscriptionEnabled()).isTrue();
    assertThat(resultingClient.getCpuUsage()).isEqualTo(0.2);
    assertThat(resultingClient.getUptime()).isEqualTo(2);
    assertThat(resultingClient.getProcessCpuTime()).isEqualTo(2000000000);
  }

  @Test
  public void updateMemberClientsHMapShouldDeleteNonSeenClients() {
    HashMap<String, Cluster.Client> existingClientsMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      Cluster.Client existingClient = new Cluster.Client();
      existingClient.setId(testName.getMethodName() + "_" + i);
      existingClient.setUptime(1);
      existingClient.setProcessCpuTime(1000000000);
      existingClientsMap.put(existingClient.getId(), existingClient);
    }
    member.setMemberClientsHMap(existingClientsMap);

    String remainingClientId = testName.getMethodName() + "_0";
    Cluster.Client updatedClient = new Cluster.Client();
    updatedClient.setId(remainingClientId);
    updatedClient.setConnected(true);
    updatedClient.setGets(30);
    updatedClient.setPuts(60);
    updatedClient.setCpus(5);
    updatedClient.setQueueSize(5);
    updatedClient.setStatus("Status");
    updatedClient.setThreads(100);
    updatedClient.setClientCQCount(0);
    updatedClient.setSubscriptionEnabled(true);
    updatedClient.setUptime(2);
    updatedClient.setProcessCpuTime(2000000000);
    HashMap<String, Cluster.Client> updatedClientsMap = new HashMap<>();
    updatedClientsMap.put(updatedClient.getId(), updatedClient);

    member.updateMemberClientsHMap(updatedClientsMap);
    Map<String, Cluster.Client> clients = member.getMemberClientsHMap();
    assertThat(clients.size()).isEqualTo(1);
    Cluster.Client resultingClient = clients.get(remainingClientId);
    assertThat(resultingClient).isNotNull();
    assertThat(resultingClient.isConnected()).isTrue();
    assertThat(resultingClient.getGets()).isEqualTo(30);
    assertThat(resultingClient.getPuts()).isEqualTo(60);
    assertThat(resultingClient.getCpus()).isEqualTo(5);
    assertThat(resultingClient.getQueueSize()).isEqualTo(5);
    assertThat(resultingClient.getStatus()).isEqualTo("Status");
    assertThat(resultingClient.getThreads()).isEqualTo(100);
    assertThat(resultingClient.getClientCQCount()).isEqualTo(0);
    assertThat(resultingClient.isSubscriptionEnabled()).isTrue();
    assertThat(resultingClient.getCpuUsage()).isEqualTo(0.2);
    assertThat(resultingClient.getUptime()).isEqualTo(2);
    assertThat(resultingClient.getProcessCpuTime()).isEqualTo(2000000000);
  }

  @Test
  public void updateMemberClientsHMapShouldNotDivideByZero() {
    Cluster.Client oldClient = new Cluster.Client();
    oldClient.setId(testName.getMethodName());
    oldClient.setCpus(0);
    oldClient.setUptime(100);
    HashMap<String, Cluster.Client> oldClientsMap = new HashMap<>();
    oldClientsMap.put(oldClient.getId(), oldClient);
    member.setMemberClientsHMap(oldClientsMap);

    Cluster.Client newClient = new Cluster.Client();
    newClient.setId(testName.getMethodName());
    newClient.setCpus(0);
    newClient.setUptime(100);
    HashMap<String, Cluster.Client> newClientsMap = new HashMap<>();
    newClientsMap.put(oldClient.getId(), oldClient);

    assertThatCode(() -> member.updateMemberClientsHMap(newClientsMap)).doesNotThrowAnyException();
    Cluster.Client resultingClient = member.getMemberClientsHMap().get(testName.getMethodName());
    assertThat(resultingClient).isNotNull();
    assertThat(resultingClient.getCpuUsage()).isEqualTo(0);
    assertThat(resultingClient.getProcessCpuTime()).isEqualTo(0);
  }
}
