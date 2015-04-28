/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: JoinRsp.java,v 1.8 2005/12/08 13:13:07 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;



import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.StreamableFixedID;
import com.gemstone.org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;


public class JoinRsp implements Serializable, Streamable, StreamableFixedID {
  /** GemStoneAddition - tells joiner to reattempt with new address */
  static final String SHUNNED_ADDRESS = "[internal]Your address is shunned";
  
    View view=null;
    Digest digest=null;
    String fail_reason = null ;  //GemstoneAddition for member authentication failure msg  
    private static final long serialVersionUID = 2949193438640587597L;

    public JoinRsp() {

    }

    public JoinRsp(View v, Digest d) {
        view=v;
        digest=d;
    }
    public JoinRsp(String fail_reason) { //GemstoneAddition 
        this.fail_reason = fail_reason;
    }
    
    public String getFailReason() { //GemstoneAddition 
      return fail_reason;
    }
    
    public void setFailReason(String fail_reason) { //GemstoneAddition 
      this.fail_reason = fail_reason;
    }

    View getView() {
        return view;
    }

    Digest getDigest() {
        return digest;
    }

    public void writeTo(DataOutputStream out) throws IOException {
      JChannel.getGfFunctions().invokeToData(this, out);
    }
    
    public int getDSFID() {
      return StreamableFixedID.JGROUPS_JOIN_RESP;
    }
    
    public short[] getSerializationVersions() {
      return null;
    }

    public void toData(DataOutput out) throws IOException {
        JChannel.getGfFunctions().writeObject(view, out);
        JChannel.getGfFunctions().writeObject(digest, out);
        JChannel.getGfFunctions().writeString(fail_reason, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
      try {
        JChannel.getGfFunctions().invokeFromData(this, in);
      } catch (ClassNotFoundException e) {
        InstantiationException ex = new InstantiationException("problem deserializing join response");
        ex.initCause(e);
        throw ex;
      }
    }
    
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
        view=JChannel.getGfFunctions().readObject(in);
        digest=JChannel.getGfFunctions().readObject(in);
        fail_reason=JChannel.getGfFunctions().readString(in);
    }

    public int serializedSize(short version) {
        int retval=Global.BYTE_SIZE * 2; // presence for view and digest
        if(view != null)
            retval+=view.serializedSize(version);
        if(digest != null)
            retval+=digest.serializedSize(version);
        return retval;
    }

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append("view: ");
        if(view == null)
            sb.append("<null>");
        else
            sb.append(view);
        sb.append(", digest: ");
        if(digest == null)
            sb.append("<null>");
        else
            sb.append(digest);
        sb.append(", fail_reason: ");
        if(fail_reason == null)
            sb.append("<null>");
        else
            sb.append(fail_reason);
        return sb.toString();
    }
}
