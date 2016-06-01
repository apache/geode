package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;
import static org.mockito.Mockito.*;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.crypto.KeyAgreement;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.DHParameterSpec;

/**
 * Created by bschuchardt on 5/6/2016.
 */
@Category(IntegrationTest.class)
public class GMSEncryptJUnitTest {

  Services services;

  InternalDistributedMember mockMembers[];

  NetView netView;

  private void initMocks() throws Exception {
    Properties nonDefault = new Properties();
    nonDefault.put(DistributionConfig.SECURITY_CLIENT_DHALGO_NAME, "AES:128");
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig tconfig = new RemoteTransportConfig(config,
      DistributionManager.NORMAL_DM_TYPE);

    ServiceConfig serviceConfig = new ServiceConfig(tconfig, config);

    services = mock(Services.class);
    when(services.getConfig()).thenReturn(serviceConfig);

    mockMembers = new InternalDistributedMember[4];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = new InternalDistributedMember("localhost", 8888 + i);
    }
    int viewId = 1;
    List<InternalDistributedMember> mbrs = new LinkedList<>();
    //mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(mockMembers[3]);

    //prepare the view
    netView = new NetView(mockMembers[1], viewId, mbrs);

  }


  @Test
  public void testOneMemberCanDecryptAnothersMessage() throws Exception{
    initMocks();

    GMSEncrypt gmsEncrypt1 = new GMSEncrypt(services, mockMembers[1]); // this will be the sender
    GMSEncrypt gmsEncrypt2 = new GMSEncrypt(services, mockMembers[2]); // this will be the receiver

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], gmsEncrypt1.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], gmsEncrypt2.getPublicKeyBytes());

    gmsEncrypt1.installView(netView, mockMembers[1]);
    gmsEncrypt2.installView(netView, mockMembers[2]);

    // sender encrypts a message, so use receiver's public key
    String ch = "Hello world";
    byte[] challenge =  ch.getBytes();
    byte[]  encryptedChallenge =  gmsEncrypt1.encryptData(challenge, mockMembers[2]);

    // receiver decrypts the message using the sender's public key
    byte[] decryptBytes = gmsEncrypt2.decryptData(encryptedChallenge,  mockMembers[1]);

    // now send a response
    String response = "Hello yourself!";
    byte[] responseBytes = response.getBytes();
    byte[] encryptedResponse = gmsEncrypt2.encryptData(responseBytes, mockMembers[1]);

    // receiver decodes the response
    byte[] decryptedResponse = gmsEncrypt1.decryptData(encryptedResponse,  mockMembers[2]);

    Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

    Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

    Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

    Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

  }
  
  @Test
  public void testPublicKeyPrivateKeyFromSameMember() throws Exception{
    initMocks();

    GMSEncrypt gmsEncrypt1 = new GMSEncrypt(services, mockMembers[1]); // this will be the sender
    GMSEncrypt gmsEncrypt2 = new GMSEncrypt(services, mockMembers[2]); // this will be the receiver
    
    gmsEncrypt1 = gmsEncrypt1.clone();
    gmsEncrypt2 = gmsEncrypt2.clone();

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], gmsEncrypt1.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], gmsEncrypt2.getPublicKeyBytes());

    gmsEncrypt1.installView(netView, mockMembers[1]);
    gmsEncrypt2.installView(netView, mockMembers[2]);

    // sender encrypts a message, so use receiver's public key
    String ch = "Hello world";
    byte[] challenge =  ch.getBytes();
    byte[]  encryptedChallenge =  gmsEncrypt1.encryptData(challenge, mockMembers[2]);

    // receiver decrypts the message using the sender's public key
    byte[] decryptBytes = gmsEncrypt2.decryptData(encryptedChallenge,  mockMembers[1]);

    // now send a response
    String response = "Hello yourself!";
    byte[] responseBytes = response.getBytes();
    byte[] encryptedResponse = gmsEncrypt2.encryptData(responseBytes, mockMembers[1]);

    // receiver decodes the response
    byte[] decryptedResponse = gmsEncrypt1.decryptData(encryptedResponse,  mockMembers[2]);

    Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

    Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

    Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

    Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));

  }
  
  @Test
  public void testForClusterSecretKey() throws Exception{
    initMocks();

    GMSEncrypt gmsEncrypt1 = new GMSEncrypt(services, mockMembers[1]); // this will be the sender
    gmsEncrypt1.initClusterSecretKey();
    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], gmsEncrypt1.getPublicKeyBytes());
    
    gmsEncrypt1.installView(netView, mockMembers[1]);
    
    // sender encrypts a message, so use receiver's public key
    String ch = "Hello world";
    byte[] challenge =  ch.getBytes();
    byte[]  encryptedChallenge =  gmsEncrypt1.encryptData(challenge);

    // receiver decrypts the message using the sender's public key
    byte[] decryptBytes = gmsEncrypt1.decryptData(encryptedChallenge);
    
    Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

    Assert.assertTrue(Arrays.equals(challenge, decryptBytes));    
  }
  
  @Test
  public void testForClusterSecretKeyFromOtherMember() throws Exception{
    initMocks();

    GMSEncrypt gmsEncrypt1 = new GMSEncrypt(services, mockMembers[1]); // this will be the sender
    gmsEncrypt1.initClusterSecretKey();
    GMSEncrypt gmsEncrypt2 = new GMSEncrypt(services, mockMembers[2]); // this will be the sender
    
    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], gmsEncrypt1.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], gmsEncrypt2.getPublicKeyBytes());
    
    gmsEncrypt1.installView(netView, mockMembers[1]);
    
    byte[] secretBytes = gmsEncrypt1.getClusterSecretKey();
    gmsEncrypt2.addClusterKey(secretBytes);
    
    gmsEncrypt2.installView(netView, mockMembers[1]);
    
    // sender encrypts a message, so use receiver's public key
    String ch = "Hello world";
    byte[] challenge =  ch.getBytes();
    byte[]  encryptedChallenge =  gmsEncrypt1.encryptData(challenge);

    // receiver decrypts the message using the sender's public key
    byte[] decryptBytes = gmsEncrypt2.decryptData(encryptedChallenge);
    
    // now send a response
    String response = "Hello yourself!";
    byte[] responseBytes = response.getBytes();
    byte[] encryptedResponse = gmsEncrypt2.encryptData(responseBytes);

    // receiver decodes the response
    byte[] decryptedResponse = gmsEncrypt1.decryptData(encryptedResponse);

    Assert.assertFalse(Arrays.equals(challenge, encryptedChallenge));

    Assert.assertTrue(Arrays.equals(challenge, decryptBytes));

    Assert.assertFalse(Arrays.equals(responseBytes, encryptedResponse));

    Assert.assertTrue(Arrays.equals(responseBytes, decryptedResponse));  
  }
  
    
  @Test
    public void testDHAlgo() throws Exception {

      DHParameterSpec dhSkipParamSpec;

      System.out.println("Using SKIP Diffie-Hellman parameters");
      dhSkipParamSpec = new DHParameterSpec(skip1024Modulus, skip1024Base);

      // Alice creates her own DH key pair
      System.out.println("ALICE: Generate DH keypair ...");
      KeyPairGenerator aliceKpairGen = KeyPairGenerator.getInstance("DH");
      aliceKpairGen.initialize(dhSkipParamSpec);
      KeyPair aliceKpair = aliceKpairGen.generateKeyPair();

      // Bob creates his own DH key pair
      System.out.println("BOB: Generate DH keypair ...");
      KeyPairGenerator bobKpairGen = KeyPairGenerator.getInstance("DH");
      bobKpairGen.initialize(dhSkipParamSpec);
      KeyPair bobKpair = bobKpairGen.generateKeyPair();

      // Carol creates her own DH key pair
      System.out.println("CAROL: Generate DH keypair ...");
      KeyPairGenerator carolKpairGen = KeyPairGenerator.getInstance("DH");
      carolKpairGen.initialize(dhSkipParamSpec);
      KeyPair carolKpair = carolKpairGen.generateKeyPair();


      // Alice initialize
      System.out.println("ALICE: Initialize ...");
      KeyAgreement aliceKeyAgree = KeyAgreement.getInstance("DH");
      aliceKeyAgree.init(aliceKpair.getPrivate());

      // Bob initialize
      System.out.println("BOB: Initialize ...");
      KeyAgreement bobKeyAgree = KeyAgreement.getInstance("DH");
      bobKeyAgree.init(bobKpair.getPrivate());

      // Carol initialize
      System.out.println("CAROL: Initialize ...");
      KeyAgreement carolKeyAgree = KeyAgreement.getInstance("DH");
      carolKeyAgree.init(carolKpair.getPrivate());


      // Alice uses Carol's public key
      Key ac = aliceKeyAgree.doPhase(carolKpair.getPublic(), false);

      // Bob uses Alice's public key
      Key ba = bobKeyAgree.doPhase(aliceKpair.getPublic(), false);

      // Carol uses Bob's public key
      Key cb = carolKeyAgree.doPhase(bobKpair.getPublic(), false);


      // Alice uses Carol's result from above
      aliceKeyAgree.doPhase(cb, true);

      // Bob uses Alice's result from above
      bobKeyAgree.doPhase(ac, true);

      // Carol uses Bob's result from above
      carolKeyAgree.doPhase(ba, true);


      // Alice, Bob and Carol compute their secrets
      byte[] aliceSharedSecret = aliceKeyAgree.generateSecret();
      System.out.println("Alice secret: " + toHexString(aliceSharedSecret));

      byte[] bobSharedSecret = bobKeyAgree.generateSecret();
      System.out.println("Bob secret: " + toHexString(bobSharedSecret));

      byte[] carolSharedSecret = carolKeyAgree.generateSecret();
      System.out.println("Carol secret: " + toHexString(carolSharedSecret));


      // Compare Alice and Bob
      if (!java.util.Arrays.equals(aliceSharedSecret, bobSharedSecret))
          throw new Exception("Alice and Bob differ");
      System.out.println("Alice and Bob are the same");

      // Compare Bob and Carol
      if (!java.util.Arrays.equals(bobSharedSecret, carolSharedSecret))
          throw new Exception("Bob and Carol differ");
      System.out.println("Bob and Carol are the same");
  }
  
  @Test
  public void testDHAlgo2() throws Exception {

    DHParameterSpec dhSkipParamSpec;

    System.out.println("Using SKIP Diffie-Hellman parameters");
    dhSkipParamSpec = new DHParameterSpec(skip1024Modulus, skip1024Base);

 // Alice creates her own DH key pair
    System.out.println("ALICE: Generate DH keypair ...");
    KeyPairGenerator aliceKpairGen = KeyPairGenerator.getInstance("DH");
    aliceKpairGen.initialize(dhSkipParamSpec);
    KeyPair aliceKpair = aliceKpairGen.generateKeyPair();

    
    // Bob creates his own DH key pair
    System.out.println("BOB: Generate DH keypair ...");
    KeyPairGenerator bobKpairGen = KeyPairGenerator.getInstance("DH");
    bobKpairGen.initialize(dhSkipParamSpec);
    KeyPair bobKpair = bobKpairGen.generateKeyPair();

    
    // Alice initialize
    System.out.println("ALICE: Initialize ...");
    KeyAgreement aliceKeyAgree = KeyAgreement.getInstance("DH");
    aliceKeyAgree.init(aliceKpair.getPrivate()); 

    // Bob initialize
    System.out.println("BOB  : Initialize ...");
    KeyAgreement bobKeyAgree = KeyAgreement.getInstance("DH");
    bobKeyAgree.init(bobKpair.getPrivate());
    
    // Alice uses Carol's public key
    aliceKeyAgree.doPhase(bobKpair.getPublic(), true);       
    
    // Bob uses Alice's public key
    bobKeyAgree.doPhase(aliceKpair.getPublic(), true);

    // Alice, Bob and Carol compute their secrets
    byte[] aliceSharedSecret = aliceKeyAgree.generateSecret();
    System.out.println("Alice secret: " + toHexString(aliceSharedSecret));

    
    byte[] bobSharedSecret = bobKeyAgree.generateSecret();
    System.out.println("Bob   secret: " + toHexString(bobSharedSecret));
    
    
    // Compare Alice and Bob
    if (!java.util.Arrays.equals(aliceSharedSecret, bobSharedSecret))
        throw new Exception("Alice and Bob differ");
    System.out.println("Alice and Bob are the same");

  }
  
  @Test
  public void testDHAlgo3() throws Exception {

    DHParameterSpec dhSkipParamSpec;

    System.out.println("Using SKIP Diffie-Hellman parameters");
    dhSkipParamSpec = new DHParameterSpec(skip1024Modulus, skip1024Base);

 // Alice creates her own DH key pair
    System.out.println("ALICE: Generate DH keypair ...");
    KeyPairGenerator aliceKpairGen = KeyPairGenerator.getInstance("DH");
    aliceKpairGen.initialize(dhSkipParamSpec);
    KeyPair aliceKpair = aliceKpairGen.generateKeyPair();

    
    // Bob creates his own DH key pair
    System.out.println("BOB: Generate DH keypair ...");
    KeyPairGenerator bobKpairGen = KeyPairGenerator.getInstance("DH");
    bobKpairGen.initialize(dhSkipParamSpec);
    KeyPair bobKpair = bobKpairGen.generateKeyPair();

    
    // Alice initialize
    System.out.println("ALICE: Initialize ...");
    KeyAgreement aliceKeyAgree = KeyAgreement.getInstance("DH");
    aliceKeyAgree.init(aliceKpair.getPrivate()); 

    // Bob initialize
    System.out.println("BOB  : Initialize ...");
    KeyAgreement bobKeyAgree = KeyAgreement.getInstance("DH");
    bobKeyAgree.init(bobKpair.getPrivate());
    
    // Alice uses Carol's public key
    aliceKeyAgree.doPhase(bobKpair.getPublic(), true);       
    
    // Bob uses Alice's public key
    bobKeyAgree.doPhase(aliceKpair.getPublic(), true);

    String dhKalgo = "AES"; 
    // Alice, Bob and Carol compute their secrets
    SecretKey aliceSharedSecret = aliceKeyAgree.generateSecret(dhKalgo);
    System.out.println("Alice secret: " + toHexString(aliceSharedSecret.getEncoded()));

    
    
    SecretKey bobSharedSecret = bobKeyAgree.generateSecret(dhKalgo);
    System.out.println("Bob   secret: " + toHexString(bobSharedSecret.getEncoded()));
    
    applyMAC(aliceSharedSecret);
    applyMAC(bobSharedSecret);
    
    // Compare Alice and Bob
    if (!java.util.Arrays.equals(aliceSharedSecret.getEncoded(), bobSharedSecret.getEncoded()))
        throw new Exception("Alice and Bob differ");
    System.out.println("Alice and Bob are the same");
  }
  
  private void applyMAC(Key key) throws Exception {
    SecretKey key2 = new SecretKey() {
      
      @Override
      public String getFormat() {
        // TODO Auto-generated method stub
        return key.getFormat();
      }
      
      @Override
      public byte[] getEncoded() {
        // TODO Auto-generated method stub
        String hitesh = "This is from Hitesh";
        byte[] secbytes = hitesh.getBytes();
        byte[] origsecret = key.getEncoded(); 
        byte[] ns = new byte[origsecret.length + secbytes.length];
        System.arraycopy(origsecret, 0, ns, 0, origsecret.length);
        System.arraycopy(secbytes, 0, ns, origsecret.length, secbytes.length);
        return ns;
      }
      
      @Override
      public String getAlgorithm() {
        // TODO Auto-generated method stub
        return key.getAlgorithm();
      }
    };
 // Generate secret key for HMAC-MD5
    //KeyGenerator kg = KeyGenerator.getInstance("HmacMD5");
    //SecretKey sk = kg.generateKey();

    // Get instance of Mac object implementing HMAC-MD5, and
    // initialize it with the above secret key
    
    System.out.println("Key2 secret " + toHexString(key2.getEncoded()));
    
    Mac mac = Mac.getInstance("HmacMD5");
    mac.init(key2);
    byte[] result = mac.doFinal("Hi There".getBytes());
   
    
    System.out.println("Message Authentication code length: " + mac.getMacLength());
    System.out.println("Message Authentication code : " + toHexString(result));
    
    verifyMacBody(mac, result);
  }
  
  private byte[] verifyMacBody(Mac hmac, byte[] encryptedAndMac) throws Exception {               
    byte[] encrypted = new byte[encryptedAndMac.length - hmac.getMacLength()];
    System.arraycopy(encryptedAndMac, 0, encrypted, 0, encrypted.length);

    byte[] remoteMac = new byte[hmac.getMacLength()];
    System.arraycopy(encryptedAndMac, encryptedAndMac.length - remoteMac.length, remoteMac, 0, remoteMac.length);

    byte[] localMac  = hmac.doFinal(encrypted);

    System.out.println("Message Authentication code remoteMac : " + toHexString(remoteMac));
    System.out.println("Message Authentication code localMac : " + toHexString(localMac));
    if (!Arrays.equals(remoteMac, localMac))
      throw new Exception("MAC doesen't match.");

    return encrypted;
  }
  // The 1024 bit Diffie-Hellman modulus values used by SKIP
  private static final byte skip1024ModulusBytes[] = {
      (byte)0xF4, (byte)0x88, (byte)0xFD, (byte)0x58,
      (byte)0x4E, (byte)0x49, (byte)0xDB, (byte)0xCD,
      (byte)0x20, (byte)0xB4, (byte)0x9D, (byte)0xE4,
      (byte)0x91, (byte)0x07, (byte)0x36, (byte)0x6B,
      (byte)0x33, (byte)0x6C, (byte)0x38, (byte)0x0D,
      (byte)0x45, (byte)0x1D, (byte)0x0F, (byte)0x7C,
      (byte)0x88, (byte)0xB3, (byte)0x1C, (byte)0x7C,
      (byte)0x5B, (byte)0x2D, (byte)0x8E, (byte)0xF6,
      (byte)0xF3, (byte)0xC9, (byte)0x23, (byte)0xC0,
      (byte)0x43, (byte)0xF0, (byte)0xA5, (byte)0x5B,
      (byte)0x18, (byte)0x8D, (byte)0x8E, (byte)0xBB,
      (byte)0x55, (byte)0x8C, (byte)0xB8, (byte)0x5D,
      (byte)0x38, (byte)0xD3, (byte)0x34, (byte)0xFD,
      (byte)0x7C, (byte)0x17, (byte)0x57, (byte)0x43,
      (byte)0xA3, (byte)0x1D, (byte)0x18, (byte)0x6C,
      (byte)0xDE, (byte)0x33, (byte)0x21, (byte)0x2C,
      (byte)0xB5, (byte)0x2A, (byte)0xFF, (byte)0x3C,
      (byte)0xE1, (byte)0xB1, (byte)0x29, (byte)0x40,
      (byte)0x18, (byte)0x11, (byte)0x8D, (byte)0x7C,
      (byte)0x84, (byte)0xA7, (byte)0x0A, (byte)0x72,
      (byte)0xD6, (byte)0x86, (byte)0xC4, (byte)0x03,
      (byte)0x19, (byte)0xC8, (byte)0x07, (byte)0x29,
      (byte)0x7A, (byte)0xCA, (byte)0x95, (byte)0x0C,
      (byte)0xD9, (byte)0x96, (byte)0x9F, (byte)0xAB,
      (byte)0xD0, (byte)0x0A, (byte)0x50, (byte)0x9B,
      (byte)0x02, (byte)0x46, (byte)0xD3, (byte)0x08,
      (byte)0x3D, (byte)0x66, (byte)0xA4, (byte)0x5D,
      (byte)0x41, (byte)0x9F, (byte)0x9C, (byte)0x7C,
      (byte)0xBD, (byte)0x89, (byte)0x4B, (byte)0x22,
      (byte)0x19, (byte)0x26, (byte)0xBA, (byte)0xAB,
      (byte)0xA2, (byte)0x5E, (byte)0xC3, (byte)0x55,
      (byte)0xE9, (byte)0x2F, (byte)0x78, (byte)0xC7
  };
  


  // The SKIP 1024 bit modulus
  private static final BigInteger skip1024Modulus
  = new BigInteger(1, skip1024ModulusBytes);

  // The base used with the SKIP 1024 bit modulus
  private static final BigInteger skip1024Base = BigInteger.valueOf(2);
  
  /*
   * Converts a byte to hex digit and writes to the supplied buffer
   */
  private void byte2hex(byte b, StringBuffer buf) {
      char[] hexChars = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
                          '9', 'A', 'B', 'C', 'D', 'E', 'F' };
      int high = ((b & 0xf0) >> 4);
      int low = (b & 0x0f);
      buf.append(hexChars[high]);
      buf.append(hexChars[low]);
  }

  /*
   * Converts a byte array to hex string
   */
  private String toHexString(byte[] block) {
      StringBuffer buf = new StringBuffer();

      int len = block.length;

      for (int i = 0; i < len; i++) {
           byte2hex(block[i], buf);
           if (i < len-1) {
               buf.append(":");
           }
      }
      return buf.toString();
  }
}
