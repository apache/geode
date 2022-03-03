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

import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.spec.DHParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.locator.GMSLocator;
import org.apache.geode.internal.lang.utils.JavaWorkarounds;

public final class GMSEncrypt<ID extends MemberIdentifier> {
  // Parameters for the Diffie-Hellman key exchange
  private static final BigInteger dhP =
      new BigInteger("13528702063991073999718992897071702177131142188276542919088770094024269"
          + "73079899070080419278066109785292538223079165925365098181867673946"
          + "34756714063947534092593553024224277712367371302394452615862654308"
          + "11180902979719649450105660478776364198726078338308557022096810447"
          + "3500348898008043285865193451061481841186553");

  private static final BigInteger dhG =
      new BigInteger("13058345680719715096166513407513969537624553636623932169016704425008150"
          + "56576152779768716554354314319087014857769741104157332735258102835"
          + "93126577393912282416840649805564834470583437473176415335737232689"
          + "81480201869671811010996732593655666464627559582258861254878896534"
          + "1273697569202082715873518528062345259949959");

  private static final int dhL = 1023;

  private final PrivateKey dhPrivateKey;

  private final PublicKey dhPublicKey;

  private final String dhSKAlgo;

  private final Services<ID> services;

  private GMSMembershipView<ID> view;

  /**
   * it keeps PK for peers
   */
  private final Map<GMSMemberWrapper, byte[]> memberToPublicKey =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<ID, GMSEncryptionCipherPool<ID>> peerEncryptors =
      new ConcurrentHashMap<>();

  private GMSEncryptionCipherPool<ID> clusterEncryptor;

  protected void installView(GMSMembershipView<ID> view) {
    this.view = view;
    this.view.setPublicKey(services.getJoinLeave().getMemberID(), getPublicKeyBytes());
  }

  void overrideInstallViewForTest(GMSMembershipView<ID> view) {
    this.view = view;
  }

  protected byte[] getClusterSecretKey() {
    if (clusterEncryptor != null) {
      return clusterEncryptor.getSecretBytes();
    } else {
      return null;
    }
  }

  protected synchronized void initClusterSecretKey() throws Exception {
    if (clusterEncryptor == null) {
      clusterEncryptor = new GMSEncryptionCipherPool<>(this, generateSecret(dhPublicKey));
    }
  }

  protected synchronized void setClusterKey(byte[] secretBytes) {
    clusterEncryptor = new GMSEncryptionCipherPool<>(this, secretBytes);
  }

  private byte[] getPublicKeyIfIAmLocator(ID mbr) {
    GMSLocator<ID> locator = (GMSLocator<ID>) services.getLocator();
    if (locator != null) {
      return locator.getPublicKey(mbr);
    }
    return null;
  }

  GMSEncrypt(Services<ID> services, String dhSKAlgo) throws Exception {
    this.services = services;

    this.dhSKAlgo = dhSKAlgo;

    // Initialize the keys when either the host is a peer that has
    // non-blank setting for DH symmetric algo, or this is a server
    // that has authenticator defined.
    if ((this.dhSKAlgo != null && this.dhSKAlgo.length() > 0)) {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("DH");
      DHParameterSpec dhSpec = new DHParameterSpec(dhP, dhG, dhL);
      keyGen.initialize(dhSpec);
      KeyPair keypair = keyGen.generateKeyPair();

      // Get the generated public and private keys
      dhPrivateKey = keypair.getPrivate();
      dhPublicKey = keypair.getPublic();
    } else {
      dhPrivateKey = null;
      dhPublicKey = null;
    }
  }

  byte[] decryptData(byte[] data, ID member) throws Exception {
    return getPeerEncryptor(member).decryptBytes(data);
  }

  byte[] encryptData(byte[] data, ID member) throws Exception {
    return getPeerEncryptor(member).encryptBytes(data);
  }


  byte[] decryptData(byte[] data) throws Exception {
    return clusterEncryptor.decryptBytes(data);
  }

  byte[] decryptData(byte[] data, byte[] pkBytes) throws Exception {
    GMSEncryptionCipherPool<ID> encryptor =
        new GMSEncryptionCipherPool<>(this, generateSecret(pkBytes));
    return encryptor.decryptBytes(data);
  }

  byte[] encryptData(byte[] data) throws Exception {
    return clusterEncryptor.encryptBytes(data);
  }

  byte[] getPublicKeyBytes() {
    return dhPublicKey.getEncoded();
  }

  private byte[] lookupKeyByMember(ID member) {
    byte[] pk = memberToPublicKey.get(new GMSMemberWrapper(member));
    if (pk == null) {
      pk = getPublicKeyIfIAmLocator(member);
    }
    if (pk == null) {
      pk = (byte[]) view.getPublicKey(member);
    }
    if (pk == null) {
      throw new IllegalStateException("unable to find public key for " + member);
    }
    return pk;
  }

  protected byte[] getPublicKey(ID member) {
    ID localMbr = services.getMessenger().getMemberID();
    try {
      if (localMbr != null && localMbr.compareTo(member, false, false) == 0) {
        return dhPublicKey.getEncoded();// local one
      }
      return lookupKeyByMember(member);
    } catch (Exception e) {
      throw new RuntimeException(
          "Not found public key for member " + member + "; my address is " + localMbr, e);
    }
  }

  protected void setPublicKey(byte[] publickey, ID mbr) {
    try {
      memberToPublicKey.put(new GMSMemberWrapper(mbr), publickey);
      peerEncryptors.replace(mbr, new GMSEncryptionCipherPool<>(this, generateSecret(publickey)));
    } catch (Exception e) {
      throw new RuntimeException("Unable to create peer encryptor " + mbr, e);
    }
  }

  private GMSEncryptionCipherPool<ID> getPeerEncryptor(ID member)
      throws Exception {
    return JavaWorkarounds.computeIfAbsent(peerEncryptors, member, (mbr) -> {
      try {
        return new GMSEncryptionCipherPool<>(this, generateSecret(lookupKeyByMember(member)));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  private static int getKeySize(String skAlgo) {
    // skAlgo contain both algo and key size info
    int colIdx = skAlgo.indexOf(':');
    String algoStr;
    int algoKeySize = 0;
    if (colIdx >= 0) {
      algoStr = skAlgo.substring(0, colIdx);
      algoKeySize = Integer.parseInt(skAlgo.substring(colIdx + 1));
    } else {
      algoStr = skAlgo;
    }
    int keysize = -1;
    if (algoStr.equalsIgnoreCase("DESede")) {
      keysize = 24;
    } else if (algoStr.equalsIgnoreCase("Blowfish")) {
      keysize = algoKeySize > 128 ? algoKeySize / 8 : 16;
    } else if (algoStr.equalsIgnoreCase("AES")) {
      keysize = (algoKeySize != 192 && algoKeySize != 256) ? 16 : algoKeySize / 8;
    }
    return keysize;
  }

  private static String getDhAlgoStr(String skAlgo) {
    int colIdx = skAlgo.indexOf(':');
    String algoStr;
    if (colIdx >= 0) {
      algoStr = skAlgo.substring(0, colIdx);
    } else {
      algoStr = skAlgo;
    }
    return algoStr;
  }

  private static int getBlockSize(String skAlgo) {
    int blocksize = -1;
    String algoStr = getDhAlgoStr(skAlgo);
    if (algoStr.equalsIgnoreCase("DESede")) {
      blocksize = 8;
    } else if (algoStr.equalsIgnoreCase("Blowfish")) {
      blocksize = 8;
    } else if (algoStr.equalsIgnoreCase("AES")) {
      blocksize = 16;
    }
    return blocksize;
  }

  Cipher getEncryptCipher(byte[] secretBytes) throws Exception {
    Cipher encrypt = null;

    int keysize = getKeySize(dhSKAlgo);
    int blocksize = getBlockSize(dhSKAlgo);

    if (keysize == -1 || blocksize == -1) {
      SecretKeySpec sks = new SecretKeySpec(secretBytes, dhSKAlgo);
      encrypt = Cipher.getInstance(dhSKAlgo);
      encrypt.init(Cipher.ENCRYPT_MODE, sks);
    } else {
      String dhAlgoStr = getDhAlgoStr(dhSKAlgo);

      SecretKeySpec sks = new SecretKeySpec(secretBytes, 0, keysize, dhAlgoStr);
      IvParameterSpec ivps = new IvParameterSpec(secretBytes, keysize, blocksize);

      encrypt = Cipher.getInstance(dhAlgoStr + "/CBC/PKCS5Padding");
      encrypt.init(Cipher.ENCRYPT_MODE, sks, ivps);
    }

    return encrypt;
  }

  Cipher getDecryptCipher(byte[] secretBytes) throws Exception {
    Cipher decrypt;

    int keysize = getKeySize(dhSKAlgo);
    int blocksize = getBlockSize(dhSKAlgo);

    if (keysize == -1 || blocksize == -1) {
      SecretKeySpec sks = new SecretKeySpec(secretBytes, dhSKAlgo);
      decrypt = Cipher.getInstance(dhSKAlgo);
      decrypt.init(Cipher.DECRYPT_MODE, sks);
    } else {
      String algoStr = getDhAlgoStr(dhSKAlgo);

      SecretKeySpec sks = new SecretKeySpec(secretBytes, 0, keysize, algoStr);
      IvParameterSpec ivps = new IvParameterSpec(secretBytes, keysize, blocksize);

      decrypt = Cipher.getInstance(algoStr + "/CBC/PKCS5Padding");
      decrypt.init(Cipher.DECRYPT_MODE, sks, ivps);
    }
    return decrypt;
  }

  private byte[] generateSecret(byte[] peerKeyBytes) throws Exception {
    X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(peerKeyBytes);
    KeyFactory keyFact = KeyFactory.getInstance("DH");
    PublicKey peerKey = keyFact.generatePublic(x509KeySpec);
    return generateSecret(dhSKAlgo, dhPrivateKey, peerKey);
  }

  private byte[] generateSecret(PublicKey peerKey) throws Exception {
    return generateSecret(dhSKAlgo, dhPrivateKey, peerKey);
  }

  private static byte[] generateSecret(String dhSKAlgo, PrivateKey privateKey,
      PublicKey otherPublicKey) throws Exception {
    KeyAgreement ka = KeyAgreement.getInstance("DH");
    ka.init(privateKey);
    ka.doPhase(otherPublicKey, true);

    int keysize = getKeySize(dhSKAlgo);
    int blocksize = getBlockSize(dhSKAlgo);

    if (keysize == -1 || blocksize == -1) {
      SecretKey sKey = ka.generateSecret(dhSKAlgo);
      return sKey.getEncoded();
    } else {
      return ka.generateSecret();
    }
  }
}
