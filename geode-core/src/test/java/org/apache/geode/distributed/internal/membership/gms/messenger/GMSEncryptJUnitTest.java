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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.crypto.KeyAgreement;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.DHParameterSpec;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.ServiceConfig;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category({IntegrationTest.class, MembershipTest.class})
public class GMSEncryptJUnitTest {

  private static final int THREAD_COUNT = 20;

  Services services;

  InternalDistributedMember mockMembers[];

  NetView netView;

  @Rule
  public ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.builder().threadCount(THREAD_COUNT).build();

  private void initMocks() throws Exception {
    initMocks("AES:128");
  }

  private void initMocks(String algo) throws Exception {
    Properties nonDefault = new Properties();
    nonDefault.put(ConfigurationProperties.SECURITY_UDP_DHALGO, algo);
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig tconfig =
        new RemoteTransportConfig(config, ClusterDistributionManager.NORMAL_DM_TYPE);

    ServiceConfig serviceConfig = new ServiceConfig(tconfig, config);

    services = mock(Services.class);
    when(services.getConfig()).thenReturn(serviceConfig);

    mockMembers = new InternalDistributedMember[4];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = new InternalDistributedMember("localhost", 8888 + i);
    }
    int viewId = 1;
    List<InternalDistributedMember> mbrs = new LinkedList<>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);

    // prepare the view
    netView = new NetView(mockMembers[0], viewId, mbrs);

  }

  String[] algos = new String[] {"AES:128", "Blowfish"};

  @Test
  public void testOneMemberCanDecryptAnothersMessage() throws Exception {
    for (String algo : algos) {
      initMocks(algo);

      GMSEncrypt sender = new GMSEncrypt(services, mockMembers[1]);
      GMSEncrypt receiver = new GMSEncrypt(services, mockMembers[2]);

      // establish the public keys for the sender and receiver
      netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
      netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

      sender.installView(netView, mockMembers[1]);
      receiver.installView(netView, mockMembers[2]);

      // sender encrypts a message, so use receiver's public key
      String ch = "Hello world";
      byte[] challenge = ch.getBytes();
      byte[] encryptedChallenge = sender.encryptData(challenge, mockMembers[2]);

      // receiver decrypts the message using the sender's public key
      byte[] decryptBytes = receiver.decryptData(encryptedChallenge, mockMembers[1]);

      // now send a response
      String response = "Hello yourself!";
      byte[] responseBytes = response.getBytes();
      byte[] encryptedResponse = receiver.encryptData(responseBytes, mockMembers[1]);

      // receiver decodes the response
      byte[] decryptedResponse = sender.decryptData(encryptedResponse, mockMembers[2]);

      Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

      Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

      Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

      Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

    }
  }

  @Test
  public void testOneMemberCanDecryptAnothersMessageMultithreaded() throws Exception {
    initMocks();
    final int runs = 100000;
    final GMSEncrypt sender = new GMSEncrypt(services, mockMembers[1]);
    final GMSEncrypt receiver = new GMSEncrypt(services, mockMembers[2]);

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

    sender.installView(netView, mockMembers[1]);
    receiver.installView(netView, mockMembers[2]);
    final CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

    for (int j = 0; j < THREAD_COUNT; j++)
      executorServiceRule.execute(new Runnable() {
        public void run() {
          // sender encrypts a message, so use receiver's public key
          try {
            int count = 0;
            for (int i = 0; i < runs; i++) {
              // System.out.println("another run " + i + " threadid " +
              // Thread.currentThread().getId());
              String ch = "Hello world";
              byte[] challenge = ch.getBytes();
              byte[] encryptedChallenge = sender.encryptData(challenge, mockMembers[2]);

              // receiver decrypts the message using the sender's public key
              byte[] decryptBytes = receiver.decryptData(encryptedChallenge, mockMembers[1]);

              // now send a response
              String response = "Hello yourself!";
              byte[] responseBytes = response.getBytes();
              byte[] encryptedResponse = receiver.encryptData(responseBytes, mockMembers[1]);

              // receiver decodes the response
              byte[] decryptedResponse = sender.decryptData(encryptedResponse, mockMembers[2]);

              Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

              Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

              Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

              Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));
              count++;
            }
            Assert.assertEquals(runs, count);
            countDownLatch.countDown();
          } catch (Exception e) {
            e.printStackTrace();

          }

        }
      });

    countDownLatch.await();
  }

  @Test
  public void testPublicKeyPrivateKeyFromSameMember() throws Exception {
    initMocks();

    GMSEncrypt sender = new GMSEncrypt(services, mockMembers[1]);
    GMSEncrypt receiver = new GMSEncrypt(services, mockMembers[2]);

    sender = sender.clone();
    receiver = receiver.clone();

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

    sender.installView(netView, mockMembers[1]);
    receiver.installView(netView, mockMembers[2]);

    // sender encrypts a message, so use receiver's public key
    String ch = "Hello world";
    byte[] challenge = ch.getBytes();
    byte[] encryptedChallenge = sender.encryptData(challenge, mockMembers[2]);

    // receiver decrypts the message using the sender's public key
    byte[] decryptBytes = receiver.decryptData(encryptedChallenge, mockMembers[1]);

    // now send a response
    String response = "Hello yourself!";
    byte[] responseBytes = response.getBytes();
    byte[] encryptedResponse = receiver.encryptData(responseBytes, mockMembers[1]);

    // receiver decodes the response
    byte[] decryptedResponse = sender.decryptData(encryptedResponse, mockMembers[2]);

    Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

    Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

    Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

    Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

  }

  @Test
  public void testForClusterSecretKey() throws Exception {
    initMocks();

    GMSEncrypt sender = new GMSEncrypt(services, mockMembers[1]);
    sender.initClusterSecretKey();
    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());

    sender.installView(netView, mockMembers[1]);

    // sender encrypts a message, so use receiver's public key
    String ch = "Hello world";
    byte[] challenge = ch.getBytes();
    byte[] encryptedChallenge = sender.encryptData(challenge);

    // receiver decrypts the message using the sender's public key
    byte[] decryptBytes = sender.decryptData(encryptedChallenge);

    Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

    Assert.assertTrue(Arrays.equals(challenge, decryptBytes));
  }

  @Test
  public void testForClusterSecretKeyFromOtherMember() throws Exception {
    for (String algo : algos) {
      initMocks(algo);

      final GMSEncrypt sender = new GMSEncrypt(services, mockMembers[1]);
      sender.initClusterSecretKey();
      final GMSEncrypt receiver = new GMSEncrypt(services, mockMembers[2]);

      // establish the public keys for the sender and receiver
      netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
      netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

      sender.installView(netView, mockMembers[1]);

      byte[] secretBytes = sender.getClusterSecretKey();
      receiver.addClusterKey(secretBytes);

      receiver.installView(netView, mockMembers[1]);

      // sender encrypts a message, so use receiver's public key
      String ch = "Hello world";
      byte[] challenge = ch.getBytes();
      byte[] encryptedChallenge = sender.encryptData(challenge);

      // receiver decrypts the message using the sender's public key
      byte[] decryptBytes = receiver.decryptData(encryptedChallenge);

      // now send a response
      String response = "Hello yourself!";
      byte[] responseBytes = response.getBytes();
      byte[] encryptedResponse = receiver.encryptData(responseBytes);

      // receiver decodes the response
      byte[] decryptedResponse = sender.decryptData(encryptedResponse);

      Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

      Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

      Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

      Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));
    }
  }

  @Test
  public void testForClusterSecretKeyFromOtherMemberMultipleThreads() throws Exception {
    initMocks();

    final GMSEncrypt sender = new GMSEncrypt(services, mockMembers[1]);
    Thread.currentThread().sleep(100);
    sender.initClusterSecretKey();
    final GMSEncrypt receiver = new GMSEncrypt(services, mockMembers[2]);

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], sender.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], receiver.getPublicKeyBytes());

    sender.installView(netView, mockMembers[1]);

    byte[] secretBytes = sender.getClusterSecretKey();
    receiver.addClusterKey(secretBytes);

    receiver.installView(netView, mockMembers[1]);

    final int runs = 100000;
    final CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

    for (int j = 0; j < THREAD_COUNT; j++)
      executorServiceRule.execute(new Runnable() {
        public void run() {
          // sender encrypts a message, so use receiver's public key
          try {
            int count = 0;
            for (int i = 0; i < runs; i++) {
              String ch = "Hello world";
              byte[] challenge = ch.getBytes();
              byte[] encryptedChallenge = sender.encryptData(challenge);

              // receiver decrypts the message using the sender's public key
              byte[] decryptBytes = receiver.decryptData(encryptedChallenge);

              // now send a response
              String response = "Hello yourself!";
              byte[] responseBytes = response.getBytes();
              byte[] encryptedResponse = receiver.encryptData(responseBytes);

              // receiver decodes the response
              byte[] decryptedResponse = sender.decryptData(encryptedResponse);

              Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

              Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

              Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

              Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

              count++;
            }

            Assert.assertEquals(runs, count);

            countDownLatch.countDown();
          } catch (Exception e) {
            e.printStackTrace();
          }

        }
      });

    countDownLatch.await();
  }
}
