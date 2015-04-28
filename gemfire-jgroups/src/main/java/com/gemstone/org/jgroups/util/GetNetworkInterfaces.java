/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Lists all network interfaces on a system
 * @author Bela Ban Dec 18
 * @author 2003
 * @version $Id: GetNetworkInterfaces.java,v 1.1 2005/06/23 13:31:09 belaban Exp $
 */
public class GetNetworkInterfaces {

    public static void main(String[] args) throws SocketException {
        Enumeration en=NetworkInterface.getNetworkInterfaces();
        while(en.hasMoreElements()) {
            NetworkInterface i=(NetworkInterface)en.nextElement();
            System.out.println(i.getName() + ':');
            System.out.println("  \t" + i.getDisplayName());
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=(InetAddress)en2.nextElement();
                System.out.println("  \t" + addr + " (" + addr.getHostName() + ')');
            }
            System.out.println("---------------------");
        }
    }

}
