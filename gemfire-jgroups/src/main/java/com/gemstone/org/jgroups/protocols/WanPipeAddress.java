/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: WanPipeAddress.java,v 1.9 2005/08/08 12:45:46 belaban Exp $

package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;


/**
 * Logical address for a WAN pipe (logical link)
 */
public class WanPipeAddress implements Address {
    String logical_name=null;
    static final GemFireTracer log=GemFireTracer.getLog(WanPipeAddress.class);


    // GemStoneAddition
    public boolean preferredForCoordinator() {
      return true;
    }
    public boolean splitBrainEnabled() {
      return false;
    }

    @Override
    public int getBirthViewId() {
      return -1;
    }
    
    @Override
    public short getVersionOrdinal() {
      return -1;
    }

    // Used only by Externalization
    public WanPipeAddress() {
    }


    public WanPipeAddress(String logical_name) {
        this.logical_name=logical_name;
    }


    public boolean isMulticastAddress() {
        return true;
    }

    public int size(short version) {
        return logical_name != null? logical_name.length()+2 : 22;
    }


    /**
     * Establishes an order between 2 addresses. Assumes other contains non-null WanPipeAddress.
     *
     * @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
     */
    public int compareTo(Object other) throws ClassCastException {
        if(other == null) {
            log.error(ExternalStrings.WanPipeAddress_WANPIPEADDRESSCOMPARETO_OTHER_ADDRESS_IS_NULL_);
            return -1;
        }

        if(!(other instanceof WanPipeAddress)) {
            log.error(ExternalStrings.WanPipeAddress_WANPIPEADDRESSCOMPARETO_OTHER_ADDRESS_IS_NOT_OF_TYPE_WANPIPEADDRESS_);
            return -1;
        }

        if(((WanPipeAddress)other).logical_name == null) {
            log.error(ExternalStrings.WanPipeAddress_WANPIPEADDRESSCOMPARETO_OTHER_ADDRESS_IS_NULL_);
            return -1;
        }

        return logical_name.compareTo(((WanPipeAddress)other).logical_name);
    }


    @Override // GemStoneAddition
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof WanPipeAddress)) return false; // GemStoneAddition
        return compareTo(obj) == 0;
    }


    @Override // GemStoneAddition
    public int hashCode() {
        return logical_name.hashCode();
    }


    @Override // GemStoneAddition
    public String toString() {
        return logical_name;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(logical_name);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        logical_name=(String)in.readObject();
    }



//    public static void main(String args[]) {
//
//        WanPipeAddress a=new WanPipeAddress("daddy");
//        System.out.println(a);
//
//        WanPipeAddress b=new WanPipeAddress("daddy.nms.fnc.fujitsu.com");
//        System.out.println(b);
//
//
//        if(a.equals(b))
//            System.out.println("equals");
//        else
//            System.out.println("does not equal");
//    }


    public void writeTo(DataOutputStream outstream) throws IOException {
        Util.writeString(logical_name, outstream);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        logical_name=Util.readString(instream);
    }
}
