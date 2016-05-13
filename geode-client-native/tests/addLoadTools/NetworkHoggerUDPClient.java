/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
import java.io.*;
import java.net.*;
import java.util.*;

public class NetworkHoggerUDPClient {
    static boolean moreNetworkHoggerUDPs = true;
    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
             System.out.println("Usage: java NetworkHoggerUDPClient <hostname>");
             return;
        }
	int len = 64*1024;
	int cnt =0;

            // get a datagram socket
        DatagramSocket socket = new DatagramSocket();

            // send request
        byte[] buf = new byte[64*1024];
	String initString = "get started";
	buf = initString.getBytes();
        InetAddress address = InetAddress.getByName(args[0]);
	
	while(moreNetworkHoggerUDPs) {
	  DatagramPacket packet = new DatagramPacket(buf, buf.length, address, 4445);
	  socket.send(packet);
    
            // get response
	  packet = new DatagramPacket(buf, buf.length);
	  socket.receive(packet);

	    // display response
	 String received = new String(packet.getData(), 0, packet.getLength());
        // System.out.println("NetworkHoggerUDP : len=" + received.length() + " msg:" + received);
	  if(received == "Bye")
	     moreNetworkHoggerUDPs=false;
	  String tmp = initString + received;
	  if(tmp.length() < len){
	     initString = tmp;
	     buf = initString.getBytes();
	  }
	}
    
        socket.close();
    }
}
