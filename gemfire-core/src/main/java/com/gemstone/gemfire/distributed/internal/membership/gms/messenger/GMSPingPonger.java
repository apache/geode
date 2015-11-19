package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;

import com.gemstone.gemfire.internal.Version;

public class GMSPingPonger {
  private byte[] pingInBytes = new byte[] { 'p', 'i', 'n', 'g' };
  private byte[] pongInBytes = new byte[] { 'p', 'o', 'n', 'g' };
  
  public boolean isPingMessage(byte[] buffer) {
    return buffer.length == 4 && (buffer[0] == 'p' && buffer[1] == 'i' && buffer[2] == 'n' && buffer[3] == 'g');
  }
  
  public boolean isPongMessage(byte[] buffer) {
    return buffer.length == 4 && (buffer[0] == 'p' && buffer[1] == 'o' && buffer[2] == 'n' && buffer[3] == 'g');
  }
  
  public void sendPongMessage(JChannel channel, Address src, Address dest) throws Exception {
    channel.send(createJGMessage(pongInBytes, src, dest, Version.CURRENT_ORDINAL)); 
  }
  
  public Message createPongMessage(Address src, Address dest) {
	  return createJGMessage(pongInBytes, src, dest, Version.CURRENT_ORDINAL);
  }
  
  public void sendPingMessage(JChannel channel, Address src, JGAddress dest) throws Exception {
    channel.send(createJGMessage(pingInBytes, src, dest, Version.CURRENT_ORDINAL));
  }

  private Message createJGMessage(byte[] msgBytes, Address src, Address dest, short version) {
	Message msg = new Message();
	msg.setDest(dest);
	msg.setSrc(src);
	msg.setObject(msgBytes);
	return msg;
  }

}
