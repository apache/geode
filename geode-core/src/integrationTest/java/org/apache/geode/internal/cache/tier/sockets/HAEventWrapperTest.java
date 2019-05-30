package org.apache.geode.internal.cache.tier.sockets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;

import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.ha.HAContainerMap;

public class HAEventWrapperTest {
  @Test
  public void serializeDeserializeHAEventWrapper() throws IOException, ClassNotFoundException {
    LocalRegion localRegion = mock(LocalRegion.class);
    when(localRegion.getFullPath()).thenReturn("testRegion");
    EventID eventID = new EventID(new byte[]{1, 2, 3}, 0, 0);
    InternalDistributedMember internalDistributedMember
        = new InternalDistributedMember(mock(InetAddress.class), 0);
    ClientProxyMembershipID clientProxyMembershipID = new ClientProxyMembershipID(internalDistributedMember);

    ClientUpdateMessageImpl clientUpdateMessage = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) localRegion, "key", "value".getBytes(), (byte) 0x01, null,
        clientProxyMembershipID, eventID);
    ClientUpdateMessageImpl.CqNameToOp cqNameToOp = mock(ClientUpdateMessageImpl.CqNameToOp.class);
    ClientUpdateMessageImpl.ClientCqConcurrentMap clientCqConcurrentMap = new ClientUpdateMessageImpl.ClientCqConcurrentMap();
    ClientUpdateMessageImpl.CqNameToOpHashMap cqNameToOpHashMap =
        new ClientUpdateMessageImpl.CqNameToOpHashMap(5);
    cqNameToOp.add("test",1);

    clientUpdateMessage.setClientCqs(clientCqConcurrentMap);

    clientCqConcurrentMap.put(clientProxyMembershipID, cqNameToOpHashMap);

    HAEventWrapper haEventWrapper = new HAEventWrapper(clientUpdateMessage);
    HAContainerMap haContainerMap = mock(HAContainerMap.class);
    when(haContainerMap.get(any())).thenReturn(clientUpdateMessage);

    haEventWrapper.setHAContainer(haContainerMap);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    haEventWrapper.toData(dataOutputStream);

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

    HAEventWrapper haEventWrapperDeserialized = new HAEventWrapper();

    haEventWrapper.fromData(dataInputStream);
  }
}
