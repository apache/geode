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

class NetworkHoggerUDPServerThread extends Thread {

    protected DatagramSocket socket = null;
    protected boolean moreNetworkHoggerUDPs = true;

    public NetworkHoggerUDPServerThread() throws IOException {
	this("NetworkHoggerUDPServerThread");
    }

    public NetworkHoggerUDPServerThread(String name) throws IOException {
        super(name);
        socket = new DatagramSocket(4445);

    }

    public void run() {

        while (moreNetworkHoggerUDPs) {
            byte[] buf = new byte[64*1024];
            try {

                    // receive request
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
        
		String received = new String(packet.getData(), 0, packet.getLength());
                //System.out.println("From Client: " + received);
                buf = received.getBytes();

		    // send the response to the client at "address" and "port"
                InetAddress address = packet.getAddress();
                int port = packet.getPort();
                packet = new DatagramPacket(buf, buf.length, address, port); 
		socket.send(packet); 
	    } catch (IOException e) { e.printStackTrace();
		moreNetworkHoggerUDPs = false;
            }
        }
        socket.close();
    }

}
public class NetworkHoggerUDPServer {
      public static void main(String[] args) throws IOException {
	        new NetworkHoggerUDPServerThread().start();
		    }
}
