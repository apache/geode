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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.Cipher;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;

public class GMSEncryptionCipherPool<ID extends MemberIdentifier> {
  private static final int MAX_CIPHERS_PER_POOL = Integer.getInteger("GMSEncrypt.MAX_ENCRYPTORS",
      Math.max(Runtime.getRuntime().availableProcessors() * 4, 16));
  private static final int MAX_CIPHER_WAIT_IN_SEC = 10;
  private static final Logger logger = Services.getLogger();

  private final GMSEncrypt<ID> gmsEncrypt;
  private final byte[] secretBytes;
  private final BlockingQueue<Cipher> encryptCipherQueue = new LinkedBlockingQueue<>();
  private final AtomicInteger encryptCipherCount = new AtomicInteger(0);
  private final BlockingQueue<Cipher> decryptCipherQueue = new LinkedBlockingQueue<>();
  private final AtomicInteger decryptCipherCount = new AtomicInteger(0);

  GMSEncryptionCipherPool(GMSEncrypt<ID> gmsEncrypt, byte[] secretBytes) {
    this.gmsEncrypt = gmsEncrypt;
    this.secretBytes = secretBytes;
  }

  byte[] getSecretBytes() {
    return secretBytes;
  }

  interface ThrowingFunction<T, R> {
    R apply(T in) throws Exception;
  }

  byte[] encryptBytes(byte[] data) throws Exception {
    Cipher encrypt =
        getOrCreateCipher(encryptCipherQueue, encryptCipherCount, gmsEncrypt::getEncryptCipher);
    try {
      return encrypt.doFinal(data);
    } finally {
      encryptCipherQueue.offer(encrypt);
    }
  }

  byte[] decryptBytes(byte[] data) throws Exception {
    Cipher decrypt =
        getOrCreateCipher(decryptCipherQueue, decryptCipherCount, gmsEncrypt::getDecryptCipher);
    try {
      return decrypt.doFinal(data);
    } finally {
      decryptCipherQueue.offer(decrypt);
    }
  }

  private Cipher getOrCreateCipher(BlockingQueue<Cipher> cipherQueue, AtomicInteger cipherCount,
      ThrowingFunction<byte[], Cipher> maker) throws Exception {
    Cipher cipher = cipherQueue.poll();
    if (cipher == null) {
      if (cipherCount.getAndIncrement() < MAX_CIPHERS_PER_POOL) {
        cipher = maker.apply(secretBytes);
      } else {
        cipherCount.decrementAndGet();
        cipher = cipherQueue.poll(MAX_CIPHER_WAIT_IN_SEC, TimeUnit.SECONDS);
      }
    }
    if (cipher == null) {
      logger.error("No encryption cipher available, exceeding max cipher threshold");
      cipherCount.incrementAndGet();
      cipher = maker.apply(secretBytes);
    }
    return cipher;
  }
}
