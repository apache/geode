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

package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.GatewayConfigurationException;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.ServerRefusedConnectionException;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.cache.tier.Acceptor;
import com.gemstone.gemfire.internal.cache.tier.ClientHandShake;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;
import com.gemstone.gemfire.security.*;
import org.apache.logging.log4j.Logger;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.spec.DHParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLSocket;
import java.io.*;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.net.Socket;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

public class HandShake implements ClientHandShake
{
  private static final Logger logger = LogService.getLogger();
  
  protected static final byte REPLY_OK = (byte)59;

  protected static final byte REPLY_REFUSED = (byte)60;

  protected static final byte REPLY_INVALID = (byte)61;

  protected static final byte REPLY_EXCEPTION_AUTHENTICATION_REQUIRED = (byte)62;

  protected static final byte REPLY_EXCEPTION_AUTHENTICATION_FAILED = (byte)63;
  
  protected static final byte REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT = (byte)64;
  
  protected static final byte REPLY_WAN_CREDENTIALS = (byte)65;
  
  protected static final byte REPLY_AUTH_NOT_REQUIRED = (byte)66;

  private byte code;
  private int clientReadTimeout = PoolFactory.DEFAULT_READ_TIMEOUT;

  private boolean isRead = false;
  protected final DistributedSystem system;

  /** Singleton for client side */
  // Client has no more a singleton handShake instance. Now each connection will
  // have its own handShake instance.
  private static HandShake handshake;

  final protected ClientProxyMembershipID id;

  private Properties credentials;
 
  private Version clientVersion;

  /**
   * Used at client side, indicates whether the 'delta-propagation' property is
   * enabled on the DS this client is connected to. This variable is used to
   * decide whether to send delta bytes or full value to the server for a
   * delta-update operation.
   */
  private static boolean deltaEnabledOnServer = true;

  // Security mode flags

  /** No credentials being sent */
  public static final byte CREDENTIALS_NONE = (byte)0;

  /** Credentials being sent without encryption on the wire */
  public static final byte CREDENTIALS_NORMAL = (byte)1;

  /** Credentials being sent with Diffie-Hellman key encryption */
  public static final byte CREDENTIALS_DHENCRYPT = (byte)2;
  
  public static final byte SECURITY_MULTIUSER_NOTIFICATIONCHANNEL = (byte)3;

  private byte appSecureMode = (byte)0;

  private PublicKey clientPublicKey = null;

  private String clientSKAlgo = null; 

  private boolean multiuserSecureMode = false;

  // Parameters for the Diffie-Hellman key exchange
  private static final BigInteger dhP = new BigInteger(
      "13528702063991073999718992897071702177131142188276542919088770094024269"
       +  "73079899070080419278066109785292538223079165925365098181867673946"
       +  "34756714063947534092593553024224277712367371302394452615862654308"
       +  "11180902979719649450105660478776364198726078338308557022096810447"
       +  "3500348898008043285865193451061481841186553");

  private static final BigInteger dhG = new BigInteger(
      "13058345680719715096166513407513969537624553636623932169016704425008150"
      +  "56576152779768716554354314319087014857769741104157332735258102835"
      +  "93126577393912282416840649805564834470583437473176415335737232689"
      +  "81480201869671811010996732593655666464627559582258861254878896534"
      +  "1273697569202082715873518528062345259949959");

  private static final int dhL = 1023;

  private static PrivateKey dhPrivateKey = null;

  private static PublicKey dhPublicKey = null;

  private static String dhSKAlgo = null;

  // Members for server authentication using digital signature

  private static String certificateFilePath = null;

  private static HashMap certificateMap = null;

  private static String privateKeyAlias = null;
  
  private static String privateKeySubject = null;

  private static PrivateKey privateKeyEncrypt = null;

  private static String privateKeySignAlgo = null;

  private static SecureRandom random = null;

  public static final String PUBLIC_KEY_FILE_PROP = "security-client-kspath";

  public static final String PUBLIC_KEY_PASSWD_PROP = "security-client-kspasswd";

  public static final String PRIVATE_KEY_FILE_PROP = "security-server-kspath";

  public static final String PRIVATE_KEY_ALIAS_PROP = "security-server-ksalias";

  public static final String PRIVATE_KEY_PASSWD_PROP = "security-server-kspasswd";

  /** @since GemFire 5.7 */
  public static final byte CONFLATION_DEFAULT = 0;
  /** @since GemFire 5.7 */
  public static final byte CONFLATION_ON = 1;
  /** @since GemFire 5.7 */
  public static final byte CONFLATION_OFF = 2;
  /** @since GemFire 5.7 */
  private byte clientConflation = CONFLATION_DEFAULT;

  /** @since GemFire 6.0.3
   *  List of per client property override bits.
   */
  private byte[] overrides = null;
  
  /**
   * Test hooks for per client conflation
   * @since GemFire 5.7
   */
  public static byte clientConflationForTesting = 0;
  public static boolean setClientConflationForTesting = false;

  /**
   * Test hook for client version support
   * @since GemFire 5.7
   */
  private static Version currentClientVersion = ConnectionProxy.VERSION;
  /**
   * Another test hook, holding a version ordinal that is higher than CURRENT
   */
  private static short overrideClientVersion = -1;
  
  /** Constructor used by server side connection */
  public HandShake(Socket sock, int timeout, DistributedSystem sys,
      Version clientVersion, byte communicationMode) throws IOException,
      AuthenticationRequiredException {
    this.clientVersion = clientVersion;
    this.system = sys;
    //SocketChannel sc = sock.getChannel();
    /*if (sc != null) {
      } else*/ {
      int soTimeout = -1;
      try {
        soTimeout = sock.getSoTimeout();
        sock.setSoTimeout(timeout);
        InputStream is = sock.getInputStream();
        int valRead =  is.read();
        //this.code =  (byte)is.read();
        if (valRead == -1) {
          throw new EOFException(LocalizedStrings.HandShake_HANDSHAKE_EOF_REACHED_BEFORE_CLIENT_CODE_COULD_BE_READ.toLocalizedString()); 
        }
        this.code =  (byte)valRead;
        if (this.code != REPLY_OK) {
          throw new IOException(LocalizedStrings.HandShake_HANDSHAKE_REPLY_CODE_IS_NOT_OK.toLocalizedString());
        }
        try {
          DataInputStream dis = new DataInputStream(is);
          DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
          this.clientReadTimeout = dis.readInt();
          if (clientVersion.compareTo(Version.CURRENT) < 0) {
            // versioned streams allow object serialization code to deal with older clients
            dis = new VersionedDataInputStream(dis, clientVersion);
            dos = new VersionedDataOutputStream(dos, clientVersion);
          }
          this.id = ClientProxyMembershipID.readCanonicalized(dis);
          // Note: credentials should always be the last piece in handshake for
          // Diffie-Hellman key exchange to work
          String authenticator = this.system.getProperties().getProperty(
              SECURITY_CLIENT_AUTHENTICATOR);
          if (clientVersion.compareTo(Version.GFE_603) >= 0) {
            setOverrides(new byte[] { dis.readByte() });
          } else {
            setClientConflation(dis.readByte());
          }
          //Hitesh
          if (this.clientVersion.compareTo(Version.GFE_65) < 0
              || communicationMode == Acceptor.GATEWAY_TO_GATEWAY) {
            this.credentials = readCredentials(dis, dos, authenticator, sys);
          } else {
            this.credentials = this
                .readCredential(dis, dos, authenticator, sys);
          }
        } catch(IOException ioe) {
          this.code = -2;
          throw  ioe;
        } catch(ClassNotFoundException cnfe) {
          this.code = -3;
          throw new IOException(LocalizedStrings.HandShake_CLIENTPROXYMEMBERSHIPID_CLASS_COULD_NOT_BE_FOUND_WHILE_DESERIALIZING_THE_OBJECT.toLocalizedString());
        }
      } finally {
        if (soTimeout != -1) {
          try {
            sock.setSoTimeout(soTimeout);
          }
          catch (IOException ignore) {
          }
        }
      }
    }
  }

  public final Version getClientVersion() {
    return this.clientVersion;
  }

  //private void initSingleton(DistributedSystem sys) {
  //  id = ClientProxyMembershipID.getNewProxyMembership();    
  //  this.system = sys;
  //  this.code = REPLY_OK;
  //}

   public HandShake(ClientProxyMembershipID id, DistributedSystem sys) {
     this.id = id;
     this.code = REPLY_OK;
     this.system = sys;
     setOverrides();
     this.credentials = null;
   }  
   
   public void updateProxyID(InternalDistributedMember idm) {
     this.id.updateID(idm);
   }

   public HandShake(HandShake handShake) {
     this.appSecureMode = handShake.appSecureMode;
     this.clientConflation = handShake.clientConflation;
     this.clientPublicKey = null;
     this.clientReadTimeout = handShake.clientReadTimeout;
     this.clientSKAlgo = null;
     this.clientVersion = handShake.clientVersion;
     this.code = handShake.code;
     this.credentials = handShake.credentials;
     this.isRead = handShake.isRead;
     this.multiuserSecureMode = handShake.multiuserSecureMode;
     this.overrides = handShake.overrides;
     this.system = handShake.system;
     this.id = handShake.id;
     //create new one
     this._decrypt = null;
     this._encrypt = null;
   }  

  /*private void read(DataInputStream dis,byte[] toFill)  throws IOException {
     /* this.code = (byte) in.read();
      if (this.code == -1) {
        throw new IOException(LocalizedStrings.HandShake_HANDSHAKE_READ_AT_END_OF_STREAM.toLocalizedString());
      }
      if (this.code != REPLY_OK) {
        throw new IOException(LocalizedStrings.HandShake_HANDSHAKE_REPLY_CODE_IS_NOT_OK.toLocalizedString());
      }
      try {
        ObjectInput in = new ObjectInputStream(is);
        this.id = ClientProxyMembershipID.readCanonicalized(in);
      } catch(IOException ioe) {
        this.code = -2;
        throw  ioe;
      } catch(ClassNotFoundException cnfe) {
        this.code = -3;
        IOException e = new IOException(LocalizedStrings.HandShake_ERROR_DESERIALIZING_HANDSHAKE.toLocalizedString());
        e.initCause(cnfe);
        throw e;
      }
    }
    finally {
      synchronized (this) {
        this.isRead = true;
      }
    }
  }*/

   //used by the client side
   private byte setClientConflation() {
     byte result = CONFLATION_DEFAULT;
     
     String clientConflationValue = this.system.getProperties().getProperty(CONFLATE_EVENTS);
     if (DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_ON
         .equalsIgnoreCase(clientConflationValue)) {
       result = CONFLATION_ON;
     }
     else 
     if (DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_OFF
         .equalsIgnoreCase(clientConflationValue)) {
       result = CONFLATION_OFF;
     }
     return result;
   }

  // used by the server side
  private void setClientConflation(byte value) {
    this.clientConflation = value;
    switch (this.clientConflation) {
    case CONFLATION_DEFAULT:
    case CONFLATION_OFF:
    case CONFLATION_ON:
      break;
    default:
      throw new IllegalArgumentException("Illegal clientConflation");
    }
  }

  // used by the client side
  private void setOverrides() {
    this.clientConflation = setClientConflation();

    // As of May 2009 ( GFE 6.0 ):
    // Note that this.clientVersion is used by server side for accepting
    // handshakes.
    // Client side handshake code uses this.currentClientVersion which can be
    // set via tests.
    if (HandShake.currentClientVersion.compareTo(Version.GFE_603) >= 0) {
      byte override = 0;
      /*
      override = this.notifyBySubscriptionOverride;
      override = (byte)((override << 2) | this.removeUnresponsiveClientOverride);
      override = (byte)((override << 2) | this.clientConflation);
      */
      override = this.clientConflation;
      this.overrides = new byte[] { override };
    }
  }

  // used by the server side
  private void setOverrides(byte[] values) {
    byte override = values[0];
    setClientConflation(((byte)(override & 0x03)));
    /*
    override = (byte)(override >>> 2);
    setRemoveUnresponsiveClientOverride(((byte)(override & 0x03)));
    override = (byte)(override >>> 2);
    setNotifyBySubscriptionOverride(((byte)(override & 0x03)));
    */
  }

  // used by CacheClientNotifier's handshake reading code
  public static byte[] extractOverrides(byte[] values) {
    byte override = values[0];
    byte[] overrides = new byte[1];
    for (int item = 0; item < overrides.length; item++) {
      overrides[item] = (byte)(override & 0x03);
      override = (byte)(override >>> 2);
    }
    return overrides;
  }

  public static void setVersionForTesting(short ver) {
    if (ver > Version.CURRENT_ORDINAL) {
      overrideClientVersion = ver;
    } else {
      currentClientVersion = Version.fromOrdinalOrCurrent(ver);
      overrideClientVersion = -1;
    }
  }
  
  public byte write(DataOutputStream dos, DataInputStream dis, byte communicationMode, int replyCode,
      int readTimeout, List ports, Properties p_credentials, DistributedMember member, boolean isCallbackConnection)
      throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    byte acceptanceCode = -1;
    try {
      hdos.writeByte(communicationMode);
      if (overrideClientVersion > 0) {
        // for testing
        Version.writeOrdinal(hdos, overrideClientVersion, true);
      } else {
        Version.writeOrdinal(hdos, currentClientVersion.ordinal(), true);
      }
        
      hdos.writeByte(replyCode);
      if (ports != null) {
        hdos.writeInt(ports.size());
        for (int i = 0; i < ports.size(); i++) {
          hdos.writeInt(Integer.parseInt((String)ports.get(i)));
        }
      }
      else {
        hdos.writeInt(readTimeout);
      }
      // we do not know the receiver's version at this point, but the on-wire
      // form of InternalDistributedMember changed in 9.0, so we must serialize
      // it using the previous version
      DataOutput idOut = new VersionedDataOutputStream(hdos, Version.GFE_82);
      DataSerializer.writeObject(this.id, idOut);
  
      if (currentClientVersion.compareTo(Version.GFE_603) >= 0) {
        for (int bytes = 0; bytes < this.overrides.length; bytes++) {
          hdos.writeByte(this.overrides[bytes]);
        }
      } else {
        // write the client conflation setting byte
        if (setClientConflationForTesting) {
          hdos.writeByte(clientConflationForTesting);
        }
        else {
          hdos.writeByte(this.clientConflation);
        }        
      }

      if (isCallbackConnection
          || communicationMode == Acceptor.GATEWAY_TO_GATEWAY) {
        if (isCallbackConnection && this.multiuserSecureMode
            && communicationMode != Acceptor.GATEWAY_TO_GATEWAY) {
          hdos.writeByte(SECURITY_MULTIUSER_NOTIFICATIONCHANNEL);
          hdos.flush();
          dos.write(hdos.toByteArray());
          dos.flush();
        } else {
          writeCredentials(dos, dis, p_credentials, ports != null, member, hdos);
        }
      } else {
        String authInitMethod = this.system.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
        acceptanceCode = writeCredential(dos, dis, authInitMethod,
            ports != null, member, hdos);
      }
    }
    finally {
      hdos.close();
    }
    return acceptanceCode;
  }

  public void writeCredentials(DataOutputStream dos, DataInputStream dis,
      Properties p_credentials, boolean isNotification, DistributedMember member)
      throws IOException, GemFireSecurityException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    try {
      writeCredentials(dos, dis, p_credentials, isNotification, member, hdos);
    }
    finally {
      hdos.close();
    }
  }

  /**
   * 
   * This assumes that authentication is the last piece of info in handshake
   * 
   * @param dos
   * @param dis
   * @param p_credentials
   * @param isNotification
   * @param member
   * @param heapdos stream to append data to.
   * @throws IOException
   * @throws GemFireSecurityException
   */
  public void writeCredentials(DataOutputStream dos, DataInputStream dis,
      Properties p_credentials, boolean isNotification, DistributedMember member, 
      HeapDataOutputStream heapdos)
      throws IOException, GemFireSecurityException {

    if (p_credentials == null) {
      // No credentials indicator
      heapdos.writeByte(CREDENTIALS_NONE);
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();
      return;
    }
    
    if (dhSKAlgo == null || dhSKAlgo.length() == 0) {
      // Normal credentials without encryption indicator
      heapdos.writeByte(CREDENTIALS_NORMAL);
      DataSerializer.writeProperties(p_credentials, heapdos);
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();
      return;
    }
    
    try {
      InternalLogWriter securityLogWriter = (InternalLogWriter)this.system.getSecurityLogWriter();
      securityLogWriter.fine("HandShake: using Diffie-Hellman key exchange with algo " + dhSKAlgo);
      boolean requireAuthentication = (certificateFilePath != null && certificateFilePath
          .length() > 0);
      if (requireAuthentication) {
        securityLogWriter
            .fine("HandShake: server authentication using digital "
                + "signature required");
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
      byte acceptanceCode = dis.readByte();
      if (acceptanceCode != REPLY_OK
          && acceptanceCode != REPLY_AUTH_NOT_REQUIRED) {
        // Ignore the useless data
        dis.readByte();
        dis.readInt();
        if (!isNotification) {
          DataSerializer.readByteArray(dis);
        }
        readMessage(dis, dos, acceptanceCode, member);
      }
      else 
      if (acceptanceCode == REPLY_OK) {
        // Get the public key of the other side
        keyBytes = DataSerializer.readByteArray(dis);
        if (requireAuthentication) {
          String subject = DataSerializer.readString(dis);
          byte[] signatureBytes = DataSerializer.readByteArray(dis);
          if (!certificateMap.containsKey(subject)) {
            throw new AuthenticationFailedException(
              LocalizedStrings.HandShake_HANDSHAKE_FAILED_TO_FIND_PUBLIC_KEY_FOR_SERVER_WITH_SUBJECT_0.toLocalizedString(subject));
          }
          
          // Check the signature with the public key
          X509Certificate cert = (X509Certificate)certificateMap
              .get(subject);
          Signature sig = Signature.getInstance(cert.getSigAlgName());
          sig.initVerify(cert);
          sig.update(clientChallenge);
          // Check the challenge string
          if (!sig.verify(signatureBytes)) {
            throw new AuthenticationFailedException("Mismatch in client "
                + "challenge bytes. Malicious server?");
          }
          securityLogWriter.fine("HandShake: Successfully verified the "
              + "digital signature from server");
        }
        
        byte[] challenge = DataSerializer.readByteArray(dis);
        X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFact = KeyFactory.getInstance("DH");
        //PublicKey pubKey = keyFact.generatePublic(x509KeySpec);
        this.clientPublicKey = keyFact.generatePublic(x509KeySpec);

        

        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
        try {
          DataSerializer.writeProperties(p_credentials, hdos);
          // Also add the challenge string
          DataSerializer.writeByteArray(challenge, hdos);
          
        //  byte[] encBytes = encrypt.doFinal(hdos.toByteArray());
          byte[] encBytes = encryptBytes(hdos.toByteArray(), getEncryptCipher(dhSKAlgo, this.clientPublicKey));
          DataSerializer.writeByteArray(encBytes, dos);
        }
        finally {
          hdos.close();
        }
      }
    }
    catch (IOException ex) {
      throw ex;
    }
    catch (GemFireSecurityException ex) {
      throw ex;
    }
    catch (Exception ex) {
      throw new AuthenticationFailedException(
          "HandShake failed in Diffie-Hellman key exchange", ex);
    }
    dos.flush();
  }

  /**
   * This method writes what readCredential() method expects to read. (Note the
   * use of singular credential). It is similar to writeCredentials(), except
   * that it doesn't write credential-properties.
   * TODO (ashetkar) A better name.
   * TODO (ashetkar) Could be merged with existing writeCredentials().
   * 
   * @param dos
   * @param dis
   * @param authInit
   * @param isNotification
   * @param member
   * @param heapdos
   * @throws IOException
   * @throws GemFireSecurityException
   */
  public byte writeCredential(DataOutputStream dos, DataInputStream dis,
      String authInit, boolean isNotification, DistributedMember member, 
      HeapDataOutputStream heapdos)
      throws IOException, GemFireSecurityException {

    if (!this.multiuserSecureMode && (authInit == null || authInit.length() == 0)) {
      // No credentials indicator
      heapdos.writeByte(CREDENTIALS_NONE);
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();
      return -1;
    }
    
    if (dhSKAlgo == null || dhSKAlgo.length() == 0) {
      // Normal credentials without encryption indicator
      heapdos.writeByte(CREDENTIALS_NORMAL);
      this.appSecureMode = CREDENTIALS_NORMAL;
//      DataSerializer.writeProperties(p_credentials, heapdos);
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();
      return -1;
    }
    byte acceptanceCode = -1;
    try {
      InternalLogWriter securityLogWriter = (InternalLogWriter)this.system.getSecurityLogWriter();
      securityLogWriter.fine("HandShake: using Diffie-Hellman key exchange with algo " + dhSKAlgo);
      boolean requireAuthentication = (certificateFilePath != null && certificateFilePath
          .length() > 0);
      if (requireAuthentication) {
        securityLogWriter
            .fine("HandShake: server authentication using digital "
                + "signature required");
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
      if (acceptanceCode != REPLY_OK
          && acceptanceCode != REPLY_AUTH_NOT_REQUIRED) {
        // Ignore the useless data
        dis.readByte();
        dis.readInt();
        if (!isNotification) {
          DataSerializer.readByteArray(dis);
        }
        readMessage(dis, dos, acceptanceCode, member);
      }
      else 
      if (acceptanceCode == REPLY_OK) {
        // Get the public key of the other side
        keyBytes = DataSerializer.readByteArray(dis);
        if (requireAuthentication) {
          String subject = DataSerializer.readString(dis);
          byte[] signatureBytes = DataSerializer.readByteArray(dis);
          if (!certificateMap.containsKey(subject)) {
            throw new AuthenticationFailedException(
              LocalizedStrings.HandShake_HANDSHAKE_FAILED_TO_FIND_PUBLIC_KEY_FOR_SERVER_WITH_SUBJECT_0.toLocalizedString(subject));
          }
          
          // Check the signature with the public key
          X509Certificate cert = (X509Certificate)certificateMap
              .get(subject);
          Signature sig = Signature.getInstance(cert.getSigAlgName());
          sig.initVerify(cert);
          sig.update(clientChallenge);
          // Check the challenge string
          if (!sig.verify(signatureBytes)) {
            throw new AuthenticationFailedException("Mismatch in client "
                + "challenge bytes. Malicious server?");
          }
          securityLogWriter.fine("HandShake: Successfully verified the "
              + "digital signature from server");
        }
        
        // Read server challenge bytes
        byte[] serverChallenge = DataSerializer.readByteArray(dis);
        X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFact = KeyFactory.getInstance("DH");
//        PublicKey pubKey = keyFact.generatePublic(x509KeySpec);
        this.clientPublicKey = keyFact.generatePublic(x509KeySpec);

        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
        try {
          // Add the challenge string
          DataSerializer.writeByteArray(serverChallenge, hdos);
//          byte[] encBytes = encrypt.doFinal(hdos.toByteArray());
          byte[] encBytes = encryptBytes(hdos.toByteArray(), getEncryptCipher(dhSKAlgo, this.clientPublicKey));
          DataSerializer.writeByteArray(encBytes, dos);
        }
        finally {
          hdos.close();
        }
      }
    }
    catch (IOException ex) {
      throw ex;
    }
    catch (GemFireSecurityException ex) {
      throw ex;
    }
    catch (Exception ex) {
      throw new AuthenticationFailedException(
          "HandShake failed in Diffie-Hellman key exchange", ex);
    }
    dos.flush();
    return acceptanceCode;
  }

  public byte [] encryptBytes(byte[] data) throws Exception
  {
    if( this.appSecureMode == CREDENTIALS_DHENCRYPT) {
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

  static public byte[] encryptBytes(byte[] data, Cipher encrypt)
  throws Exception{
    
    
    try{
        byte[] encBytes = encrypt.doFinal(data);
        return encBytes;
    } catch (Exception ex) {
      throw ex;
    }
  }

  private Cipher _encrypt;

  private Cipher getEncryptCipher(String dhSKAlgo, PublicKey publicKey) 
      throws Exception{
    try {
      if(_encrypt == null) {
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
        }
        else {
          String dhAlgoStr = getDhAlgoStr(dhSKAlgo);
          
          byte[] sKeyBytes = ka.generateSecret();
          SecretKeySpec sks = new SecretKeySpec(sKeyBytes, 0, keysize, dhAlgoStr);
          IvParameterSpec ivps = new IvParameterSpec(sKeyBytes, keysize, blocksize);
          
          encrypt = Cipher.getInstance(dhAlgoStr + "/CBC/PKCS5Padding");
          encrypt.init(Cipher.ENCRYPT_MODE, sks, ivps);
        }
        _encrypt = encrypt;
      }
    }catch(Exception ex) {
      throw ex;
    }
    return _encrypt;
  }
  
//This assumes that authentication is the last piece of info in handshake
  public Properties readCredential(DataInputStream dis,
      DataOutputStream dos, String authenticator, DistributedSystem system)
      throws GemFireSecurityException, IOException {

    Properties credentials = null;
    boolean requireAuthentication = (authenticator != null && authenticator
        .length() > 0);
    try {
      byte secureMode = dis.readByte();
      if (secureMode == CREDENTIALS_NONE) {
        // when server is configured with authenticator, new joiner must
        // pass its credentials.
        if (requireAuthentication) {
          throw new AuthenticationRequiredException(
            LocalizedStrings.HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED
              .toLocalizedString());
        }
      }
      else if (secureMode == CREDENTIALS_NORMAL) {
        this.appSecureMode = CREDENTIALS_NORMAL;
        /*if (requireAuthentication) {
          credentials = DataSerializer.readProperties(dis);
        }
        else {
          DataSerializer.readProperties(dis); // ignore the credentials
        }*/
      }
      else if (secureMode == CREDENTIALS_DHENCRYPT) {
        this.appSecureMode = CREDENTIALS_DHENCRYPT;
        boolean sendAuthentication = dis.readBoolean();
        InternalLogWriter securityLogWriter = (InternalLogWriter)system.getSecurityLogWriter();
        // Get the symmetric encryption algorithm to be used
        //String skAlgo = DataSerializer.readString(dis);
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
                LocalizedStrings.HandShake_SERVER_PRIVATE_KEY_NOT_AVAILABLE_FOR_CREATING_SIGNATURE.toLocalizedString());
            }
            // Sign the challenge from client and send it to the client
            Signature sig = Signature.getInstance(privateKeySignAlgo);
            sig.initSign(privateKeyEncrypt);
            sig.update(clientChallenge);
            byte[] signedBytes = sig.sign();
            dos.writeByte(REPLY_OK);
            DataSerializer.writeByteArray(keyBytes, dos);
            //DataSerializer.writeString(privateKeyAlias, dos);
            DataSerializer.writeString(privateKeySubject, dos);
            DataSerializer.writeByteArray(signedBytes, dos);
            securityLogWriter.fine("HandShake: sent the signed client challenge");
          }
          else {
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
          Cipher c= getDecryptCipher(this.clientSKAlgo, this.clientPublicKey);
          byte[] credentialBytes = decryptBytes(encBytes, c);
          ByteArrayInputStream bis = new ByteArrayInputStream(credentialBytes);
          DataInputStream dinp = new DataInputStream(bis);
          //credentials = DataSerializer.readProperties(dinp);//Hitesh: we don't send in handshake now
          byte[] challengeRes = DataSerializer.readByteArray(dinp);
          // Check the challenge string
          if (!Arrays.equals(challenge, challengeRes)) {
            throw new AuthenticationFailedException(
              LocalizedStrings.HandShake_MISMATCH_IN_CHALLENGE_BYTES_MALICIOUS_CLIENT
                .toLocalizedString());
          }
          dinp.close();
        }
        else {
          if (sendAuthentication) {
            // Read and ignore the client challenge
            DataSerializer.readByteArray(dis);
          }
          dos.writeByte(REPLY_AUTH_NOT_REQUIRED);
          dos.flush();
        }
      }
    }
    catch (IOException ex) {
      throw ex;
    }
    catch (GemFireSecurityException ex) {
      throw ex;
    }
    catch (Exception ex) {
      throw new AuthenticationFailedException(
        LocalizedStrings.HandShake_FAILURE_IN_READING_CREDENTIALS
          .toLocalizedString(),
        ex);
    }
    return credentials;
  }
  
  public byte[] decryptBytes(byte[] data) throws Exception
  {  
    if( this.appSecureMode == CREDENTIALS_DHENCRYPT) {
      String algo = null;
      if (this.clientSKAlgo != null) {
        algo = this.clientSKAlgo;
      } else {
        algo = dhSKAlgo;
      }
      Cipher c= getDecryptCipher(algo, this.clientPublicKey);
      return decryptBytes(data, c);
    } else {
      return data;
    }
  }
  
  
  
  static public byte[] decryptBytes(byte[] data, Cipher decrypt)
  throws Exception{
    try {
      byte[] decrptBytes = decrypt.doFinal(data);
      return decrptBytes;
    }catch(Exception ex) {
      throw ex;
    }    
  }
  
  private Cipher _decrypt = null;
  
  private Cipher getDecryptCipher( String dhSKAlgo, PublicKey publicKey)
      throws Exception{
      if(_decrypt == null) {
        try {
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
          }
          else {
            String algoStr = getDhAlgoStr(dhSKAlgo);
            
            byte[] sKeyBytes = ka.generateSecret();
            SecretKeySpec sks = new SecretKeySpec(sKeyBytes, 0, keysize, algoStr);
            IvParameterSpec ivps = new IvParameterSpec(sKeyBytes, keysize, blocksize);
            
            decrypt = Cipher.getInstance(algoStr + "/CBC/PKCS5Padding");
            decrypt.init(Cipher.DECRYPT_MODE, sks, ivps);
          }
          
          _decrypt = decrypt;
          }catch(Exception ex) {
            throw ex;
          }            
      }
      return _decrypt;
  }
  /**
   * Populate the available server public keys into a local static HashMap. This
   * method is not thread safe.
   */
  public static void initCertsMap(Properties props) throws Exception {

    certificateMap = new HashMap();
    certificateFilePath = props.getProperty(PUBLIC_KEY_FILE_PROP);
    if (certificateFilePath != null && certificateFilePath.length() > 0) {
      KeyStore ks = KeyStore.getInstance("JKS");
      String keyStorePass = props.getProperty(PUBLIC_KEY_PASSWD_PROP);
      char[] passPhrase = (keyStorePass != null ? keyStorePass.toCharArray()
          : null);
      FileInputStream keystorefile = new FileInputStream(certificateFilePath);
      try {
        ks.load(keystorefile, passPhrase);
      }
      finally {
        keystorefile.close();
      }
      Enumeration aliases = ks.aliases();
      while (aliases.hasMoreElements()) {
        String alias = (String)aliases.nextElement();
        Certificate cert = ks.getCertificate(alias);
        if (cert instanceof X509Certificate) {
          String subject = ((X509Certificate)cert).getSubjectDN().getName();
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
      char[] passPhrase = (keyStorePass != null ? keyStorePass.toCharArray()
          : null);
      FileInputStream privateKeyFile = new FileInputStream(privateKeyFilePath);
      try {
        ks.load(privateKeyFile, passPhrase);
      }
      finally {
        privateKeyFile.close();
      }
      Key key = ks.getKey(privateKeyAlias, passPhrase);
      Certificate keyCert = ks.getCertificate(privateKeyAlias);
      if (key instanceof PrivateKey && keyCert instanceof X509Certificate) {
        privateKeyEncrypt = (PrivateKey)key;
        privateKeySignAlgo = ((X509Certificate)keyCert).getSigAlgName();
        privateKeySubject = ((X509Certificate)keyCert).getSubjectDN().getName();
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
    String authenticator = config.getSecurityClientAuthenticator();
    // Initialize the keys when either the host is a client that has
    // non-blank setting for DH symmetric algo, or this is a server
    // that has authenticator defined.
    if ((dhSKAlgo != null && dhSKAlgo.length() > 0)
        || (authenticator != null && authenticator.length() > 0)) {
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
  
  public void accept(OutputStream out, InputStream in, byte epType, int qSize,
      byte communicationMode, Principal principal) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);
    DataInputStream dis;
    if (clientVersion.compareTo(Version.CURRENT) < 0) {
      dis = new VersionedDataInputStream(in, clientVersion);
      dos = new VersionedDataOutputStream(dos, clientVersion);
    } else {
      dis = new DataInputStream(in);
    }
    // Write ok reply
    //TODO:HItesh
    if (communicationMode == Acceptor.GATEWAY_TO_GATEWAY && principal != null) {
      dos.writeByte(REPLY_WAN_CREDENTIALS);
    }
    else {
      dos.writeByte(REPLY_OK);//byte 59
    }

    
  //additional byte of wan site needs to send for Gateway BC
    if (communicationMode == Acceptor.GATEWAY_TO_GATEWAY) {
      Version.writeOrdinal(dos, ServerHandShakeProcessor.currentServerVersion.ordinal(),
          true);
    }

    dos.writeByte(epType);
    dos.writeInt(qSize);
    
    // Write the server's member
    DistributedMember member = this.system.getDistributedMember();
    ServerHandShakeProcessor.writeServerMember(member, dos);

    // Write no message
    dos.writeUTF("");

    // Write delta-propagation property value if this is not WAN.
    if (communicationMode != Acceptor.GATEWAY_TO_GATEWAY
        && this.clientVersion.compareTo(Version.GFE_61) >= 0) {
      dos.writeBoolean(((InternalDistributedSystem)this.system).getConfig()
          .getDeltaPropagation());
    }

    // Neeraj: Now if the communication mode is GATEWAY_TO_GATEWAY
    // and principal not equal to null then send the credentials also
    if (communicationMode == Acceptor.GATEWAY_TO_GATEWAY && principal != null) {
      sendCredentialsForWan(dos, dis);
    }
          
    // Write the distributed system id if this is a 6.6 or greater client
    //on the remote side of the gateway
    if (communicationMode == Acceptor.GATEWAY_TO_GATEWAY
        && this.clientVersion.compareTo(Version.GFE_66) >= 0
        && ServerHandShakeProcessor.currentServerVersion.compareTo(Version.GFE_66) >= 0) {
      dos.writeByte(((InternalDistributedSystem)this.system)
          .getDistributionManager().getDistributedSystemId());
    }
    
    if((communicationMode == Acceptor.GATEWAY_TO_GATEWAY) 
        && this.clientVersion.compareTo(Version.GFE_80) >= 0
        && ServerHandShakeProcessor.currentServerVersion.compareTo(Version.GFE_80) >= 0) {
      int pdxSize = PeerTypeRegistration.getPdxRegistrySize();
      dos.writeInt(pdxSize);
    }

    // Flush
    dos.flush();
  }

  /**
   * Return fake, temporary DistributedMember to represent the other vm this 
   * vm is connecting to
   * 
   * @param sock the socket this handshake is operating on
   * @return temporary id to reprent the other vm
   */
  private DistributedMember getDistributedMember(Socket sock) {
    return new InternalDistributedMember(
        sock.getInetAddress(), sock.getPort(), false);
  }

  public ServerQueueStatus greet(Connection conn, ServerLocation location, byte communicationMode) throws IOException,
      AuthenticationRequiredException, AuthenticationFailedException,
      ServerRefusedConnectionException {
    try {
      ServerQueueStatus serverQStatus = null;
      Socket sock = conn.getSocket();
      DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
      final InputStream in = sock.getInputStream();
      DataInputStream dis = new DataInputStream(in);
      DistributedMember member = getDistributedMember(sock);
      // if running in a loner system, use the new port number in the ID to 
      // help differentiate from other clients
      DM dm = ((InternalDistributedSystem)this.system).getDistributionManager();
      InternalDistributedMember idm = dm.getDistributionManagerId();
      synchronized(idm) {
        if (idm.getPort() == 0 && dm instanceof LonerDistributionManager) {
          int port = sock.getLocalPort();
          ((LonerDistributionManager)dm).updateLonerPort(port);
          updateProxyID(dm.getDistributionManagerId());
        }
      }
      if(communicationMode == Acceptor.GATEWAY_TO_GATEWAY) {
        this.credentials = getCredentials(member);
      }
      byte intermediateAcceptanceCode = write(dos, dis, communicationMode,
          REPLY_OK, this.clientReadTimeout, null, this.credentials, member,
          false);
      
      String authInit = this.system.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
      if (communicationMode != Acceptor.GATEWAY_TO_GATEWAY
          && intermediateAcceptanceCode != REPLY_AUTH_NOT_REQUIRED
          && (authInit != null && authInit.length() != 0)) {
        location.compareAndSetRequiresCredentials(true);
      }
      // Read the acceptance code
      byte acceptanceCode = dis.readByte();
      if (acceptanceCode == (byte)21 && !(sock instanceof SSLSocket)) {
        // This is likely the case of server setup with SSL and client not using
        // SSL
        throw new AuthenticationRequiredException(
          LocalizedStrings.HandShake_SERVER_EXPECTING_SSL_CONNECTION
          .toLocalizedString());
      }
      
      //Successful handshake for GATEWAY_TO_GATEWAY mode sets the peer version in connection
      if(communicationMode == Acceptor.GATEWAY_TO_GATEWAY  && !
          (acceptanceCode == REPLY_EXCEPTION_AUTHENTICATION_REQUIRED ||
          acceptanceCode ==  REPLY_EXCEPTION_AUTHENTICATION_FAILED)) {
        short wanSiteVersion = Version.readOrdinal(dis);
        conn.setWanSiteVersion(wanSiteVersion);
        // establish a versioned stream for the other site, if necessary         
        if (wanSiteVersion < Version.CURRENT_ORDINAL) {
          dis = new VersionedDataInputStream(dis, Version.fromOrdinalOrCurrent(wanSiteVersion));
        }
      } 

      // No need to check for return value since DataInputStream already throws
      // EOFException in case of EOF
      byte epType = dis.readByte();
      int qSize = dis.readInt();

      // Read the server member
      member = readServerMember(dis);
      serverQStatus = new ServerQueueStatus(epType, qSize,member);

      // Read the message (if any)
      readMessage(dis, dos, acceptanceCode, member);

      // Read delta-propagation property value from server.
      // [sumedh] Static variable below? Client can connect to different
      // DSes with different values of this. It shoule be a member variable.
      if (communicationMode != Acceptor.GATEWAY_TO_GATEWAY
          && currentClientVersion.compareTo(Version.GFE_61) >= 0) {
        deltaEnabledOnServer = dis.readBoolean();
      }

      //validate that the remote side has a different distributed system id.
      if (communicationMode == Acceptor.GATEWAY_TO_GATEWAY
          && Version.GFE_66.compareTo(conn.getWanSiteVersion()) <= 0
          && currentClientVersion.compareTo(Version.GFE_66) >= 0) {
        int remoteDistributedSystemId = in.read();
        int localDistributedSystemId = ((InternalDistributedSystem) system).getDistributionManager().getDistributedSystemId();
        if(localDistributedSystemId >= 0 && localDistributedSystemId == remoteDistributedSystemId) {
          throw new GatewayConfigurationException(
              "Remote WAN site's distributed system id "
                  + remoteDistributedSystemId
                  + " matches this sites distributed system id "
                  + localDistributedSystemId);
        }
      }
      //Read the PDX registry size from the remote size
      if(communicationMode == Acceptor.GATEWAY_TO_GATEWAY 
          && Version.GFE_80.compareTo(conn.getWanSiteVersion()) <= 0 
          && currentClientVersion.compareTo(Version.GFE_80) >= 0) {
        int remotePdxSize = dis.readInt();
        serverQStatus.setPdxSize(remotePdxSize);
      }

      return serverQStatus;
    }
    catch (IOException ex) {
      CancelCriterion stopper = this.system.getCancelCriterion();
      stopper.checkCancelInProgress(null);
      throw ex;
    }
  }

  public ServerQueueStatus greetNotifier(Socket sock, boolean isPrimary, ServerLocation location) throws IOException, AuthenticationRequiredException,
      AuthenticationFailedException, ServerRefusedConnectionException, ClassNotFoundException {
    ServerQueueStatus sqs = null;
    try {
      DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
      final InputStream in = sock.getInputStream();
      DataInputStream dis = new DataInputStream(in);
      DistributedMember member = getDistributedMember(sock);
      if (!this.multiuserSecureMode) {
        this.credentials = getCredentials(member);
      }
      byte mode = isPrimary ? Acceptor.PRIMARY_SERVER_TO_CLIENT 
              : Acceptor.SECONDARY_SERVER_TO_CLIENT;
      write(dos, dis, mode, REPLY_OK, 0, new ArrayList(), this.credentials, 
          member, true); 
      
      // Wait here for a reply before continuing. This ensures that the client
      // updater is registered with the server before continuing.
      byte acceptanceCode = dis.readByte();
      if (acceptanceCode == (byte)21 && !(sock instanceof SSLSocket)) {
        // This is likely the case of server setup with SSL and client not using
        // SSL
        throw new AuthenticationRequiredException(
            LocalizedStrings.HandShake_SERVER_EXPECTING_SSL_CONNECTION.toLocalizedString());
      }

      // No need to check for return value since DataInputStream already throws
      // EOFException in case of EOF
      byte qType = dis.readByte();
      // read and ignore qSize flag
      int qSize = dis.readInt();
      sqs = new ServerQueueStatus(qType, qSize, member);
            
      // Read the message (if any)
      readMessage(dis, dos, acceptanceCode, member);

      // [sumedh] nothing more to be done for older clients used in tests
      // there is a difference in serializer map registration for >= 6.5.1.6
      // clients but that is not used in tests
      if (currentClientVersion.compareTo(Version.GFE_61) < 0) {
        return sqs;
      }
      HashMap instantiatorMap = DataSerializer.readHashMap(dis);
      for (Iterator itr = instantiatorMap.entrySet().iterator(); itr.hasNext();) {
        Map.Entry instantiator = (Map.Entry)itr.next();
        Integer id = (Integer)instantiator.getKey();
        ArrayList instantiatorArguments = (ArrayList)instantiator.getValue();
        InternalInstantiator.register((String)instantiatorArguments
            .get(0), (String)instantiatorArguments.get(1), id, false);
      }

      HashMap dataSerializersMap = DataSerializer.readHashMap(dis);
      for (Iterator itr = dataSerializersMap.entrySet().iterator(); itr.hasNext();) {
        Map.Entry dataSerializer = (Map.Entry)itr.next();
        Integer id = (Integer)dataSerializer.getKey();
        InternalDataSerializer.register((String)dataSerializer.getValue(),
            false, null, null, id);
      }
      HashMap<Integer, ArrayList<String>> dsToSupportedClassNames = DataSerializer.readHashMap(dis);
      InternalDataSerializer.updateSupportedClassesMap(dsToSupportedClassNames);      
   }
    catch (IOException ex) {
      CancelCriterion stopper = this.system.getCancelCriterion();
      stopper.checkCancelInProgress(null);
      throw ex;
    }
    catch (ClassNotFoundException ex) {
      CancelCriterion stopper = this.system.getCancelCriterion();
      stopper.checkCancelInProgress(null);
      throw ex;
    }
    return sqs;
  }

  protected DistributedMember readServerMember(DataInputStream p_dis) throws IOException {

    byte[] memberBytes = DataSerializer.readByteArray(p_dis);
    ByteArrayInputStream bais = new ByteArrayInputStream(memberBytes);
    DataInputStream dis = new DataInputStream(bais);
    Version v = InternalDataSerializer.getVersionForDataStreamOrNull(p_dis);
    if (v != null) {
      dis = new VersionedDataInputStream(dis, v);
    }
    try {
      return (DistributedMember)DataSerializer.readObject(dis);
    }
    catch (EOFException e) {
      throw e;
    }
    catch (Exception e) {
      throw new InternalGemFireException(LocalizedStrings.HandShake_UNABLE_TO_DESERIALIZE_MEMBER.toLocalizedString(), e);
    }
  }

  protected void readMessage(DataInputStream dis, DataOutputStream dos, byte acceptanceCode, DistributedMember member)
      throws IOException, AuthenticationRequiredException,
      AuthenticationFailedException, ServerRefusedConnectionException {

    String message = dis.readUTF();
    if (message.length() == 0 && acceptanceCode != REPLY_WAN_CREDENTIALS) {
      return; // success
    }

    switch (acceptanceCode) {
      case REPLY_EXCEPTION_AUTHENTICATION_REQUIRED:
        throw new AuthenticationRequiredException(message);
      case REPLY_EXCEPTION_AUTHENTICATION_FAILED:
        throw new AuthenticationFailedException(message);
      case REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT:
        throw new ServerRefusedConnectionException(member,
            message);
      case REPLY_WAN_CREDENTIALS:
        checkIfAuthenticWanSite(dis, dos, member);
        break;
      default:
        throw new ServerRefusedConnectionException(member,
            message);
    }
  }

  public byte getCode()
  {
    return this.code;
  }

  public boolean isRead()
  {
    return this.isRead;
  }

  public boolean isOK()
  {
    return getCode() == REPLY_OK;
  }

  public void setClientReadTimeout(int clientReadTimeout) {
    this.clientReadTimeout = clientReadTimeout;
  }
  
  public int getClientReadTimeout() {
    return this.clientReadTimeout;
  }

  public void setMultiuserSecureMode(boolean bool) {
    this.multiuserSecureMode = bool;
  }

  public boolean isMultiuserSecureMode() {
    return this.multiuserSecureMode;
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param  other  the reference object with which to compare.
   * @return true if this object is the same as the obj argument;
   *         false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
//    if (other == null) return false;
    if (!(other instanceof HandShake)) return  false;
    final HandShake that = (HandShake) other;
    
    /*if (identity != null && identity.length > 0) {
     for (int i = 0; i < identity.length; i++) {
     if (this.identity[i] != that.identity[i]) return false;
     }
     }
     if (this.code != that.code) return false;
     return true;*/
    
    if(this.id.isSameDSMember(that.id) && this.code ==that.code) {
      return true;
    }
    else {
      return false;
    }
  }
  
  
  /**
   * Returns a hash code for the object. This method is supported for the
   * benefit of hashtables such as those provided by java.util.Hashtable.
   *
   * @return the integer 0 if description is null; otherwise a unique integer.
   */
  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;
    
    /*if (this.identity != null && this.identity.length > 0) {
     for (int i = 0; i < this.identity.length; i++) {
     result = mult * result + (int) this.identity[i];
     }
     }*/
    result = this.id.hashCode();
    result = mult * result + this.code;
    
    return result;
  }
  
  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer()
      .append("HandShake@")
      .append(System.identityHashCode(this))
      .append(" code: ")
      .append(this.code);
    if (this.id != null) {
      buf.append(" identity: ");
      /*for(int i=0; i<this.identity.length; ++i) {
        buf.append(this.identity[i]);
      }*/
      buf.append(this.id.toString());     
    }
    return buf.toString();
  }
  
  public ClientProxyMembershipID getMembership() {
    return this.id;
  }

  public static Properties getCredentials(String authInitMethod,
      Properties securityProperties, DistributedMember server, boolean isPeer,
      InternalLogWriter logWriter, InternalLogWriter securityLogWriter)
      throws AuthenticationRequiredException {

    Properties credentials = null;
    try {
      if (authInitMethod != null && authInitMethod.length() > 0) {
        Method instanceGetter = ClassLoadUtil.methodFromName(authInitMethod);
        AuthInitialize auth = (AuthInitialize)instanceGetter.invoke(null,
            (Object[])null);
        if (auth != null) {
          auth.init(logWriter, 
                    securityLogWriter);
          try {
            credentials = auth.getCredentials(securityProperties, server,
                isPeer);
          }
          finally {
            auth.close();
          }
        }
      }
    }
    catch (GemFireSecurityException ex) {
      throw ex;
    }
    catch (Exception ex) {
      throw new AuthenticationRequiredException(
          LocalizedStrings.HandShake_FAILED_TO_ACQUIRE_AUTHINITIALIZE_METHOD_0.toLocalizedString(authInitMethod), ex);
    }
    return credentials;
  }

  private Properties getCredentials(DistributedMember member) {

    String authInitMethod = this.system.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
    return getCredentials(authInitMethod, this.system.getSecurityProperties(),
        member, false, (InternalLogWriter)this.system.getLogWriter(), 
        (InternalLogWriter)this.system.getSecurityLogWriter());
  }

  // This assumes that authentication is the last piece of info in handshake
  public static Properties readCredentials(DataInputStream dis,
      DataOutputStream dos, String authenticator, DistributedSystem system)
      throws GemFireSecurityException, IOException {

    Properties credentials = null;
    boolean requireAuthentication = (authenticator != null && authenticator
        .length() > 0);
    try {
      byte secureMode = dis.readByte();
      if (secureMode == CREDENTIALS_NONE) {
        // when server is configured with authenticator, new joiner must
        // pass its credentials.
        if (requireAuthentication) {
          throw new AuthenticationRequiredException(
            LocalizedStrings.HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED
              .toLocalizedString());
        }
      }
      else if (secureMode == CREDENTIALS_NORMAL) {
        if (requireAuthentication) {
          credentials = DataSerializer.readProperties(dis);
        }
        else {
          DataSerializer.readProperties(dis); // ignore the credentials
        }
      }
      else if (secureMode == CREDENTIALS_DHENCRYPT) {
        boolean sendAuthentication = dis.readBoolean();
        InternalLogWriter securityLogWriter = (InternalLogWriter)system.getSecurityLogWriter();
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
                LocalizedStrings.HandShake_SERVER_PRIVATE_KEY_NOT_AVAILABLE_FOR_CREATING_SIGNATURE.toLocalizedString());
            }
            // Sign the challenge from client and send it to the client
            Signature sig = Signature.getInstance(privateKeySignAlgo);
            sig.initSign(privateKeyEncrypt);
            sig.update(clientChallenge);
            byte[] signedBytes = sig.sign();
            dos.writeByte(REPLY_OK);
            DataSerializer.writeByteArray(keyBytes, dos);
            //DataSerializer.writeString(privateKeyAlias, dos);
            DataSerializer.writeString(privateKeySubject, dos);
            DataSerializer.writeByteArray(signedBytes, dos);
            securityLogWriter.fine("HandShake: sent the signed client challenge");
          }
          else {
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
          }
          else {
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
              LocalizedStrings.HandShake_MISMATCH_IN_CHALLENGE_BYTES_MALICIOUS_CLIENT
                .toLocalizedString());
          }
          dinp.close();
        }
        else {
          if (sendAuthentication) {
            // Read and ignore the client challenge
            DataSerializer.readByteArray(dis);
          }
          dos.writeByte(REPLY_AUTH_NOT_REQUIRED);
          dos.flush();
        }
      }else if (secureMode == SECURITY_MULTIUSER_NOTIFICATIONCHANNEL)
      {
        //TODO:hitesh there will be no credential CCP will get credential(Principal) using ServerConnection..
        logger.debug("readCredential where multiuser mode creating callback connection");
      }
    }
    catch (IOException ex) {
      throw ex;
    }
    catch (GemFireSecurityException ex) {
      throw ex;
    }
    catch (Exception ex) {
      throw new AuthenticationFailedException(
        LocalizedStrings.HandShake_FAILURE_IN_READING_CREDENTIALS
          .toLocalizedString(),
        ex);
    }
    return credentials;
  }
  
  public static Principal verifyCredentials(String authenticatorMethod,
      Properties credentials, Properties securityProperties, InternalLogWriter logWriter,
      InternalLogWriter securityLogWriter, DistributedMember member)
      throws AuthenticationRequiredException, AuthenticationFailedException {

    Authenticator auth = null;
    try {
      if (authenticatorMethod == null || authenticatorMethod.length() == 0) {
        return null;
      }
      Method instanceGetter = ClassLoadUtil.methodFromName(authenticatorMethod);
      auth = (Authenticator)instanceGetter.invoke(null, (Object[])null);
    }
    catch (Exception ex) {
      throw new AuthenticationFailedException(
          LocalizedStrings.HandShake_FAILED_TO_ACQUIRE_AUTHENTICATOR_OBJECT.toLocalizedString(), ex);
    }
    if (auth == null) {
      throw new AuthenticationFailedException(
        LocalizedStrings.HandShake_AUTHENTICATOR_INSTANCE_COULD_NOT_BE_OBTAINED.toLocalizedString()); 
    }
    auth.init(securityProperties, logWriter, securityLogWriter);
    Principal principal;
    try {
      principal = auth.authenticate(credentials, member);
    }
    finally {
      auth.close();
    }
    return principal;
  }

  public Principal verifyCredentials() throws AuthenticationRequiredException,
      AuthenticationFailedException {

    String methodName = this.system.getProperties().getProperty(
        SECURITY_CLIENT_AUTHENTICATOR);
    return verifyCredentials(methodName, this.credentials, this.system
        .getSecurityProperties(), (InternalLogWriter)this.system.getLogWriter(), (InternalLogWriter)this.system
        .getSecurityLogWriter(), this.id.getDistributedMember());
  }

  public void sendCredentialsForWan(OutputStream out, InputStream in) {

    try {
      Properties wanCredentials = getCredentials(this.id.getDistributedMember());
      DataOutputStream dos = new DataOutputStream(out);
      DataInputStream dis = new DataInputStream(in);
      writeCredentials(dos, dis, wanCredentials, false, this.system.getDistributedMember());
    }
    // The exception while getting the credentials is just logged as severe
    catch (Exception e) {
      this.system.getSecurityLogWriter().convertToLogWriterI18n().severe(
          LocalizedStrings.HandShake_AN_EXCEPTION_WAS_THROWN_WHILE_SENDING_WAN_CREDENTIALS_0,
          e.getLocalizedMessage());
    }
  }

  private void checkIfAuthenticWanSite(DataInputStream dis,
      DataOutputStream dos, DistributedMember member)
      throws GemFireSecurityException, IOException {

    if (this.credentials == null) {
      return;
    }
    String authenticator = this.system.getProperties().getProperty(
        SECURITY_CLIENT_AUTHENTICATOR);
    Properties peerWanProps = readCredentials(dis, dos, authenticator,
        this.system);
    verifyCredentials(authenticator, peerWanProps, this.system
        .getSecurityProperties(), (InternalLogWriter)this.system.getLogWriter(), (InternalLogWriter)this.system
        .getSecurityLogWriter(), member);
  }

  private static int getKeySize(String skAlgo) {
    // skAlgo contain both algo and key size info
    int colIdx = skAlgo.indexOf(':');
    String algoStr;
    int algoKeySize = 0;
    if (colIdx >= 0) {
      algoStr = skAlgo.substring(0, colIdx);
      algoKeySize = Integer.parseInt(skAlgo.substring(colIdx + 1));
    }
    else {
      algoStr = skAlgo;
    }
    int keysize = -1;
    if (algoStr.equalsIgnoreCase("DESede")) {
      keysize = 24;
    }
    else if (algoStr.equalsIgnoreCase("Blowfish")) {
      keysize = algoKeySize > 128 ? algoKeySize / 8 : 16;
    }
    else if (algoStr.equalsIgnoreCase("AES")) {
      keysize = (algoKeySize != 192 && algoKeySize != 256) ? 16
          : algoKeySize / 8;
    }
    return keysize;
  }
  
  private static String getDhAlgoStr(String skAlgo) {
    int colIdx = skAlgo.indexOf(':');
    String algoStr;
    if (colIdx >= 0) {
      algoStr = skAlgo.substring(0, colIdx);
    }
    else {
      algoStr = skAlgo;
    }
    return algoStr;
  }
  
  private static int getBlockSize(String skAlgo) {
    int blocksize = -1;
    String algoStr = getDhAlgoStr(skAlgo);
    if (algoStr.equalsIgnoreCase("DESede")) {
      blocksize = 8;
    }
    else if (algoStr.equalsIgnoreCase("Blowfish")) {
      blocksize = 8;
    }
    else if (algoStr.equalsIgnoreCase("AES")) {
      blocksize = 16;
    }
    return blocksize;
  }
  
  public Version getVersion() {
    return this.clientVersion;
  }

  public static boolean isDeltaEnabledOnServer() {
    return deltaEnabledOnServer;
  }

  public boolean hasCredentials() {
    return this.credentials != null;
  }
}
