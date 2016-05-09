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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by bschuchardt on 5/6/2016.
 */
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
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);

    //prepare the view
    netView = new NetView(mockMembers[0], viewId, mbrs);

  }


  @Test
  public void testOneMemberCanDecryptAnothersMessage() throws Exception{
    initMocks();

    GMSEncrypt gmsEncrypt1 = new GMSEncrypt(services); // this will be the sender
    GMSEncrypt gmsEncrypt2 = new GMSEncrypt(services); // this will be the receiver

    // establish the public keys for the sender and receiver
    netView.setPublicKey(mockMembers[1], gmsEncrypt1.getPublicKeyBytes());
    netView.setPublicKey(mockMembers[2], gmsEncrypt2.getPublicKeyBytes());

    gmsEncrypt1.installView(netView);
    gmsEncrypt2.installView(netView);

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
}
