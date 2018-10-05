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

import static org.apache.geode.internal.cache.tier.sockets.Handshake.CREDENTIALS_DHENCRYPT;
import static org.apache.geode.internal.cache.tier.sockets.Handshake.PRIVATE_KEY_ALIAS_PROP;
import static org.apache.geode.internal.cache.tier.sockets.Handshake.PRIVATE_KEY_FILE_PROP;
import static org.apache.geode.internal.cache.tier.sockets.Handshake.PRIVATE_KEY_PASSWD_PROP;
import static org.apache.geode.internal.cache.tier.sockets.Handshake.PUBLIC_KEY_FILE_PROP;
import static org.apache.geode.internal.cache.tier.sockets.Handshake.PUBLIC_KEY_PASSWD_PROP;
import static org.apache.geode.internal.cache.tier.sockets.Handshake.REPLY_AUTH_NOT_REQUIRED;
import static org.apache.geode.internal.cache.tier.sockets.Handshake.REPLY_OK;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyAgreement;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.DHParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.geode.DataSerializer;
import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.GemFireSecurityException;

public class EncryptorImpl implements Encryptor {
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

  private Cipher _encrypt;
  private Cipher _decrypt;

  private PublicKey clientPublicKey;

  private String clientSKAlgo;

  private static PrivateKey dhPrivateKey;

  private static PublicKey dhPublicKey;

  private static String dhSKAlgo;

  // Members for server authentication using digital signature

  private static String certificateFilePath;

  private static HashMap certificateMap;

  private static String privateKeyAlias;

  private static String privateKeySubject;

  private static PrivateKey privateKeyEncrypt;

  private static String privateKeySignAlgo;

  private static SecureRandom random;

  private byte appSecureMode = (byte) 0;

  private LogWriter logWriter;


  EncryptorImpl(EncryptorImpl encryptor) {
    this.appSecureMode = encryptor.appSecureMode;
    this.logWriter = encryptor.logWriter;
  }

  public EncryptorImpl(LogWriter logWriter) {
    this.logWriter = logWriter;
  }


  void setAppSecureMode(byte appSecureMode) {
    this.appSecureMode = appSecureMode;
  }

  public static byte[] decryptBytes(byte[] data, Cipher decrypt) throws Exception {
    return decrypt.doFinal(data);
  }

  protected Cipher getDecryptCipher(String dhSKAlgo, PublicKey publicKey) throws Exception {
    if (_decrypt == null) {
      KeyAgreement ka = KeyAgreement.getInstance("DH");
      ka.init(dhPrivateKey);
      ka.doPhase(publicKey, true);

      Cipher decrypt;

      int keysize = getKeySize(dhSKAlgo);
      int blocksize = getBlockSize(dhSKAlgo);

      if (keysize == -1 || blocksize == -1) {
        SecretKey sKey = ka.generateSecret(dhSKAlgo);
        decrypt = Cipher.getInstance(dhSKAlgo);
        decrypt.init(Cipher.DECRYPT_MODE, sKey);
      } else {
        String algoStr = getDhAlgoStr(dhSKAlgo);

        byte[] sKeyBytes = ka.generateSecret();
        SecretKeySpec sks = new SecretKeySpec(sKeyBytes, 0, keysize, algoStr);
        IvParameterSpec ivps = new IvParameterSpec(sKeyBytes, keysize, blocksize);

        decrypt = Cipher.getInstance(algoStr + "/CBC/PKCS5Padding");
        decrypt.init(Cipher.DECRYPT_MODE, sks, ivps);
      }

      _decrypt = decrypt;
    }
    return _decrypt;
  }

  /**
   * Populate the available server public keys into a local static HashMap. This method is not
   * thread safe.
   */
  public static void initCertsMap(Properties props) throws Exception {

    certificateMap = new HashMap();
    certificateFilePath = props.getProperty(PUBLIC_KEY_FILE_PROP);
    if (certificateFilePath != null && certificateFilePath.length() > 0) {
      KeyStore ks = KeyStore.getInstance("JKS");
      String keyStorePass = props.getProperty(PUBLIC_KEY_PASSWD_PROP);
      char[] passPhrase = (keyStorePass != null ? keyStorePass.toCharArray() : null);
      FileInputStream keystorefile = new FileInputStream(certificateFilePath);
      try {
        ks.load(keystorefile, passPhrase);
      } finally {
        keystorefile.close();
      }
      Enumeration aliases = ks.aliases();
      while (aliases.hasMoreElements()) {
        String alias = (String) aliases.nextElement();
        Certificate cert = ks.getCertificate(alias);
        if (cert instanceof X509Certificate) {
          String subject = ((X509Certificate) cert).getSubjectDN().getName();
          certificateMap.put(subject, cert);
        }
      }
    }
  }

  /**
   * Load the private key of the server. This method is not thread safe.
   */
  public static void initPrivateKey(Properties props) throws Exception {

    String privateKeyFilePath = props.getProperty(PRIVATE_KEY_FILE_PROP);
    privateKeyAlias = "";
    privateKeyEncrypt = null;
    if (privateKeyFilePath != null && privateKeyFilePath.length() > 0) {
      KeyStore ks = KeyStore.getInstance("PKCS12");
      privateKeyAlias = props.getProperty(PRIVATE_KEY_ALIAS_PROP);
      if (privateKeyAlias == null) {
        privateKeyAlias = "";
      }
      String keyStorePass = props.getProperty(PRIVATE_KEY_PASSWD_PROP);
      char[] passPhrase = (keyStorePass != null ? keyStorePass.toCharArray() : null);
      FileInputStream privateKeyFile = new FileInputStream(privateKeyFilePath);
      try {
        ks.load(privateKeyFile, passPhrase);
      } finally {
        privateKeyFile.close();
      }
      Key key = ks.getKey(privateKeyAlias, passPhrase);
      Certificate keyCert = ks.getCertificate(privateKeyAlias);
      if (key instanceof PrivateKey && keyCert instanceof X509Certificate) {
        privateKeyEncrypt = (PrivateKey) key;
        privateKeySignAlgo = ((X509Certificate) keyCert).getSigAlgName();
        privateKeySubject = ((X509Certificate) keyCert).getSubjectDN().getName();
      }
    }
  }

  /**
   * Initialize the Diffie-Hellman keys. This method is not thread safe
   */
  public static void initDHKeys(DistributionConfig config) throws Exception {

    dhSKAlgo = config.getSecurityClientDHAlgo();
    dhPrivateKey = null;
    dhPublicKey = null;
    // Initialize the keys when either the host is a client that has
    // non-blank setting for DH symmetric algo, or this is a server
    // that has authenticator defined.
    if ((dhSKAlgo != null
        && dhSKAlgo.length() > 0) /* || securityService.isClientSecurityRequired() */) {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("DH");
      DHParameterSpec dhSpec = new DHParameterSpec(dhP, dhG, dhL);
      keyGen.initialize(dhSpec);
      KeyPair keypair = keyGen.generateKeyPair();

      // Get the generated public and private keys
      dhPrivateKey = keypair.getPrivate();
      dhPublicKey = keypair.getPublic();

      random = new SecureRandom();
      // Force the random generator to seed itself.
      byte[] someBytes = new byte[48];
      random.nextBytes(someBytes);
    }
  }

  public byte[] decryptBytes(byte[] data) throws Exception {
    if (this.appSecureMode == CREDENTIALS_DHENCRYPT) {
      String algo = null;
      if (this.clientSKAlgo != null) {
        algo = this.clientSKAlgo;
      } else {
        algo = dhSKAlgo;
      }
      Cipher c = getDecryptCipher(algo, this.clientPublicKey);
      return decryptBytes(data, c);
    } else {
      return data;
    }
  }



  public byte[] encryptBytes(byte[] data) throws Exception {
    if (this.appSecureMode == CREDENTIALS_DHENCRYPT) {
      String algo = null;
      if (this.clientSKAlgo != null) {
        algo = this.clientSKAlgo;
      } else {
        algo = dhSKAlgo;
      }
      return encryptBytes(data, getEncryptCipher(algo, this.clientPublicKey));
    } else {
      return data;
    }
  }

  public static byte[] encryptBytes(byte[] data, Cipher encrypt) throws Exception {
    return encrypt.doFinal(data);
  }

  protected Cipher getEncryptCipher(String dhSKAlgo, PublicKey publicKey) throws Exception {
    if (_encrypt == null) {
      KeyAgreement ka = KeyAgreement.getInstance("DH");
      ka.init(dhPrivateKey);
      ka.doPhase(publicKey, true);

      Cipher encrypt;

      int keysize = getKeySize(dhSKAlgo);
      int blocksize = getBlockSize(dhSKAlgo);

      if (keysize == -1 || blocksize == -1) {
        SecretKey sKey = ka.generateSecret(dhSKAlgo);
        encrypt = Cipher.getInstance(dhSKAlgo);
        encrypt.init(Cipher.ENCRYPT_MODE, sKey);
      } else {
        String dhAlgoStr = getDhAlgoStr(dhSKAlgo);

        byte[] sKeyBytes = ka.generateSecret();
        SecretKeySpec sks = new SecretKeySpec(sKeyBytes, 0, keysize, dhAlgoStr);
        IvParameterSpec ivps = new IvParameterSpec(sKeyBytes, keysize, blocksize);

        encrypt = Cipher.getInstance(dhAlgoStr + "/CBC/PKCS5Padding");
        encrypt.init(Cipher.ENCRYPT_MODE, sks, ivps);
      }
      _encrypt = encrypt;
    }
    return _encrypt;
  }

  boolean isEnabled() {
    return dhSKAlgo != null && dhSKAlgo.length() > 0;
  }

  byte writeEncryptedCredential(DataOutputStream dos, DataInputStream dis,
      HeapDataOutputStream heapdos) throws IOException {
    byte acceptanceCode;
    try {
      logWriter.fine("HandShake: using Diffie-Hellman key exchange with algo " + dhSKAlgo);
      boolean requireAuthentication =
          (certificateFilePath != null && certificateFilePath.length() > 0);
      if (requireAuthentication) {
        logWriter.fine("HandShake: server authentication using digital " + "signature required");
      }
      // Credentials with encryption indicator
      heapdos.writeByte(CREDENTIALS_DHENCRYPT);
      this.appSecureMode = CREDENTIALS_DHENCRYPT;
      heapdos.writeBoolean(requireAuthentication);
      // Send the symmetric encryption algorithm name
      DataSerializer.writeString(dhSKAlgo, heapdos);
      // Send the DH public key
      byte[] keyBytes = dhPublicKey.getEncoded();
      DataSerializer.writeByteArray(keyBytes, heapdos);
      byte[] clientChallenge = null;
      if (requireAuthentication) {
        // Authentication of server should be with the client supplied
        // challenge
        clientChallenge = new byte[64];
        random.nextBytes(clientChallenge);
        DataSerializer.writeByteArray(clientChallenge, heapdos);
      }
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();

      // Expect the alias and signature in the reply
      acceptanceCode = dis.readByte();
      if (acceptanceCode == REPLY_OK) {
        // Get the public key of the other side
        keyBytes = DataSerializer.readByteArray(dis);
        if (requireAuthentication) {
          String subject = DataSerializer.readString(dis);
          byte[] signatureBytes = DataSerializer.readByteArray(dis);
          if (!certificateMap.containsKey(subject)) {
            throw new AuthenticationFailedException(
                String.format("HandShake failed to find public key for server with subject %s",
                    subject));
          }

          // Check the signature with the public key
          X509Certificate cert = (X509Certificate) certificateMap.get(subject);
          Signature sig = Signature.getInstance(cert.getSigAlgName());
          sig.initVerify(cert);
          sig.update(clientChallenge);
          // Check the challenge string
          if (!sig.verify(signatureBytes)) {
            throw new AuthenticationFailedException(
                "Mismatch in client " + "challenge bytes. Malicious server?");
          }
          logWriter.fine("HandShake: Successfully verified the " + "digital signature from server");
        }

        // Read server challenge bytes
        byte[] serverChallenge = DataSerializer.readByteArray(dis);
        X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFact = KeyFactory.getInstance("DH");
        // PublicKey pubKey = keyFact.generatePublic(x509KeySpec);
        this.clientPublicKey = keyFact.generatePublic(x509KeySpec);

        try (HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT)) {
          // Add the challenge string
          DataSerializer.writeByteArray(serverChallenge, hdos);
          // byte[] encBytes = encrypt.doFinal(hdos.toByteArray());
          byte[] encBytes =
              encryptBytes(hdos.toByteArray(), getEncryptCipher(dhSKAlgo, this.clientPublicKey));
          DataSerializer.writeByteArray(encBytes, dos);
        }
      }
    } catch (IOException ex) {
      throw ex;
    } catch (GemFireSecurityException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AuthenticationFailedException("HandShake failed in Diffie-Hellman key exchange",
          ex);
    }
    return acceptanceCode;
  }

  byte writeEncryptedCredentials(DataOutputStream dos, DataInputStream dis,
      Properties p_credentials, HeapDataOutputStream heapdos) throws IOException {
    byte acceptanceCode;
    try {
      logWriter.fine("HandShake: using Diffie-Hellman key exchange with algo " + dhSKAlgo);
      boolean requireAuthentication =
          (certificateFilePath != null && certificateFilePath.length() > 0);
      if (requireAuthentication) {
        logWriter.fine("HandShake: server authentication using digital signature required");
      }
      // Credentials with encryption indicator
      heapdos.writeByte(CREDENTIALS_DHENCRYPT);
      heapdos.writeBoolean(requireAuthentication);
      // Send the symmetric encryption algorithm name
      DataSerializer.writeString(dhSKAlgo, heapdos);
      // Send the DH public key
      byte[] keyBytes = dhPublicKey.getEncoded();
      DataSerializer.writeByteArray(keyBytes, heapdos);
      byte[] clientChallenge = null;
      if (requireAuthentication) {
        // Authentication of server should be with the client supplied
        // challenge
        clientChallenge = new byte[64];
        random.nextBytes(clientChallenge);
        DataSerializer.writeByteArray(clientChallenge, heapdos);
      }
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();

      // Expect the alias and signature in the reply
      acceptanceCode = dis.readByte();
      if (acceptanceCode == REPLY_OK) {
        // Get the public key of the other side
        keyBytes = DataSerializer.readByteArray(dis);
        if (requireAuthentication) {
          String subject = DataSerializer.readString(dis);
          byte[] signatureBytes = DataSerializer.readByteArray(dis);
          if (!certificateMap.containsKey(subject)) {
            throw new AuthenticationFailedException(
                String.format("HandShake failed to find public key for server with subject %s",
                    subject));
          }

          // Check the signature with the public key
          X509Certificate cert = (X509Certificate) certificateMap.get(subject);
          Signature sig = Signature.getInstance(cert.getSigAlgName());
          sig.initVerify(cert);
          sig.update(clientChallenge);
          // Check the challenge string
          if (!sig.verify(signatureBytes)) {
            throw new AuthenticationFailedException(
                "Mismatch in client " + "challenge bytes. Malicious server?");
          }
          logWriter.fine("HandShake: Successfully verified the " + "digital signature from server");
        }

        byte[] challenge = DataSerializer.readByteArray(dis);
        X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFact = KeyFactory.getInstance("DH");
        // PublicKey pubKey = keyFact.generatePublic(x509KeySpec);
        this.clientPublicKey = keyFact.generatePublic(x509KeySpec);



        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
        try {
          DataSerializer.writeProperties(p_credentials, hdos);
          // Also add the challenge string
          DataSerializer.writeByteArray(challenge, hdos);

          // byte[] encBytes = encrypt.doFinal(hdos.toByteArray());
          byte[] encBytes =
              encryptBytes(hdos.toByteArray(), getEncryptCipher(dhSKAlgo, this.clientPublicKey));
          DataSerializer.writeByteArray(encBytes, dos);
        } finally {
          hdos.close();
        }
      }
    } catch (IOException ex) {
      throw ex;
    } catch (GemFireSecurityException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AuthenticationFailedException("HandShake failed in Diffie-Hellman key exchange",
          ex);
    }
    return acceptanceCode;
  }

  void readEncryptedCredentials(DataInputStream dis, DataOutputStream dos, DistributedSystem system,
      boolean requireAuthentication) throws Exception {
    this.appSecureMode = CREDENTIALS_DHENCRYPT;
    boolean sendAuthentication = dis.readBoolean();
    InternalLogWriter securityLogWriter = (InternalLogWriter) system.getSecurityLogWriter();
    // Get the symmetric encryption algorithm to be used
    this.clientSKAlgo = DataSerializer.readString(dis);
    // Get the public key of the other side
    byte[] keyBytes = DataSerializer.readByteArray(dis);
    byte[] challenge = null;
    // PublicKey pubKey = null;
    if (requireAuthentication) {
      // Generate PublicKey from encoded form
      X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
      KeyFactory keyFact = KeyFactory.getInstance("DH");
      this.clientPublicKey = keyFact.generatePublic(x509KeySpec);

      // Send the public key to other side
      keyBytes = dhPublicKey.getEncoded();
      challenge = new byte[64];
      random.nextBytes(challenge);

      // If the server has to also authenticate itself then
      // sign the challenge from client.
      if (sendAuthentication) {
        // Get the challenge string from client
        byte[] clientChallenge = DataSerializer.readByteArray(dis);
        if (privateKeyEncrypt == null) {
          throw new AuthenticationFailedException(
              "Server private key not available for creating signature.");
        }
        // Sign the challenge from client and send it to the client
        Signature sig = Signature.getInstance(privateKeySignAlgo);
        sig.initSign(privateKeyEncrypt);
        sig.update(clientChallenge);
        byte[] signedBytes = sig.sign();
        dos.writeByte(REPLY_OK);
        DataSerializer.writeByteArray(keyBytes, dos);
        // DataSerializer.writeString(privateKeyAlias, dos);
        DataSerializer.writeString(privateKeySubject, dos);
        DataSerializer.writeByteArray(signedBytes, dos);
        securityLogWriter.fine("HandShake: sent the signed client challenge");
      } else {
        // These two lines should not be moved before the if{} statement in
        // a common block for both if...then...else parts. This is to handle
        // the case when an AuthenticationFailedException is thrown by the
        // if...then part when sending the signature.
        dos.writeByte(REPLY_OK);
        DataSerializer.writeByteArray(keyBytes, dos);
      }
      // Now send the server challenge
      DataSerializer.writeByteArray(challenge, dos);
      securityLogWriter.fine("HandShake: sent the public key and challenge");
      dos.flush();

      // Read and decrypt the credentials
      byte[] encBytes = DataSerializer.readByteArray(dis);
      Cipher c = getDecryptCipher(this.clientSKAlgo, this.clientPublicKey);
      byte[] credentialBytes = decryptBytes(encBytes, c);
      ByteArrayInputStream bis = new ByteArrayInputStream(credentialBytes);
      DataInputStream dinp = new DataInputStream(bis);
      // credentials = DataSerializer.readProperties(dinp);//Hitesh: we don't send in handshake
      // now
      byte[] challengeRes = DataSerializer.readByteArray(dinp);
      // Check the challenge string
      if (!Arrays.equals(challenge, challengeRes)) {
        throw new AuthenticationFailedException(
            "Mismatch in challenge bytes. Malicious client?");
      }
      dinp.close();
    } else {
      if (sendAuthentication) {
        // Read and ignore the client challenge
        DataSerializer.readByteArray(dis);
      }
      dos.writeByte(REPLY_AUTH_NOT_REQUIRED);
      dos.flush();
    }
  }

  static Properties getDecryptedCredentials(DataInputStream dis, DataOutputStream dos,
      DistributedSystem system, boolean requireAuthentication, Properties credentials)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException,
      SignatureException, NoSuchPaddingException, InvalidAlgorithmParameterException,
      IllegalBlockSizeException, BadPaddingException, ClassNotFoundException {
    boolean sendAuthentication = dis.readBoolean();
    InternalLogWriter securityLogWriter = (InternalLogWriter) system.getSecurityLogWriter();
    // Get the symmetric encryption algorithm to be used
    String skAlgo = DataSerializer.readString(dis);
    // Get the public key of the other side
    byte[] keyBytes = DataSerializer.readByteArray(dis);
    byte[] challenge = null;
    PublicKey pubKey = null;
    if (requireAuthentication) {
      // Generate PublicKey from encoded form
      X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
      KeyFactory keyFact = KeyFactory.getInstance("DH");
      pubKey = keyFact.generatePublic(x509KeySpec);

      // Send the public key to other side
      keyBytes = dhPublicKey.getEncoded();
      challenge = new byte[64];
      random.nextBytes(challenge);

      // If the server has to also authenticate itself then
      // sign the challenge from client.
      if (sendAuthentication) {
        // Get the challenge string from client
        byte[] clientChallenge = DataSerializer.readByteArray(dis);
        if (privateKeyEncrypt == null) {
          throw new AuthenticationFailedException(
              "Server private key not available for creating signature.");
        }
        // Sign the challenge from client and send it to the client
        Signature sig = Signature.getInstance(privateKeySignAlgo);
        sig.initSign(privateKeyEncrypt);
        sig.update(clientChallenge);
        byte[] signedBytes = sig.sign();
        dos.writeByte(REPLY_OK);
        DataSerializer.writeByteArray(keyBytes, dos);
        // DataSerializer.writeString(privateKeyAlias, dos);
        DataSerializer.writeString(privateKeySubject, dos);
        DataSerializer.writeByteArray(signedBytes, dos);
        securityLogWriter.fine("HandShake: sent the signed client challenge");
      } else {
        // These two lines should not be moved before the if{} statement in
        // a common block for both if...then...else parts. This is to handle
        // the case when an AuthenticationFailedException is thrown by the
        // if...then part when sending the signature.
        dos.writeByte(REPLY_OK);
        DataSerializer.writeByteArray(keyBytes, dos);
      }
      // Now send the server challenge
      DataSerializer.writeByteArray(challenge, dos);
      securityLogWriter.fine("HandShake: sent the public key and challenge");
      dos.flush();

      // Read and decrypt the credentials
      byte[] encBytes = DataSerializer.readByteArray(dis);
      KeyAgreement ka = KeyAgreement.getInstance("DH");
      ka.init(dhPrivateKey);
      ka.doPhase(pubKey, true);

      Cipher decrypt;

      int keysize = getKeySize(skAlgo);
      int blocksize = getBlockSize(skAlgo);

      if (keysize == -1 || blocksize == -1) {
        SecretKey sKey = ka.generateSecret(skAlgo);
        decrypt = Cipher.getInstance(skAlgo);
        decrypt.init(Cipher.DECRYPT_MODE, sKey);
      } else {
        String algoStr = getDhAlgoStr(skAlgo);

        byte[] sKeyBytes = ka.generateSecret();
        SecretKeySpec sks = new SecretKeySpec(sKeyBytes, 0, keysize, algoStr);
        IvParameterSpec ivps = new IvParameterSpec(sKeyBytes, keysize, blocksize);

        decrypt = Cipher.getInstance(algoStr + "/CBC/PKCS5Padding");
        decrypt.init(Cipher.DECRYPT_MODE, sks, ivps);
      }

      byte[] credentialBytes = decrypt.doFinal(encBytes);
      ByteArrayInputStream bis = new ByteArrayInputStream(credentialBytes);
      DataInputStream dinp = new DataInputStream(bis);
      credentials = DataSerializer.readProperties(dinp);
      byte[] challengeRes = DataSerializer.readByteArray(dinp);
      // Check the challenge string
      if (!Arrays.equals(challenge, challengeRes)) {
        throw new AuthenticationFailedException(
            "Mismatch in challenge bytes. Malicious client?");
      }
      dinp.close();
    } else {
      if (sendAuthentication) {
        // Read and ignore the client challenge
        DataSerializer.readByteArray(dis);
      }
      dos.writeByte(REPLY_AUTH_NOT_REQUIRED);
      dos.flush();
    }
    return credentials;
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


}
