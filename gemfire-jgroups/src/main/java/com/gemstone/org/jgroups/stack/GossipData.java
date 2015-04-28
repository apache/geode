/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: GossipData.java,v 1.2 2004/03/30 06:47:27 belaban Exp $

package com.gemstone.org.jgroups.stack;


import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.util.VersionedStreamable;



/**
 * Encapsulates data sent between GossipServer and GossipClient
 * @author Bela Ban Oct 4 2001
 */
public class GossipData implements VersionedStreamable {
    private static final long serialVersionUID = 309080226207432135L;
    public static final int REGISTER_REQ = 1;
    public static final int GET_REQ      = 2;
    public static final int GET_RSP      = 3;
    public static final int GEMFIRE_VERSION = 4;

    int     type=0;
    String  group=null;  // REGISTER, GET_REQ and GET_RSP
    Address mbr=null;    // REGISTER
    List mbrs=null;   // GET_RSP
    short versionOrdinal = -1;
    
    boolean hasDistributedSystem; // GemStoneAddition
    boolean floatingCoordinatorDisabled; // GemStoneAddition
    private boolean networkPartitionDetectionEnabled; // GemStoneAddition
    Vector locators; // GemStoneAddition
    Address localAddress; // GemStoneAddition


    public GossipData() {
        ; // used for externalization
    }

    public GossipData(int type, String group, Address mbr, List mbrs, Vector locators) {
        this.type=type;
        this.group=group;
        this.mbr=mbr;
        this.mbrs=mbrs;
    }
    
    /**
     * GemStoneAddition - added a flag for whether gossip server has a
     * distributed system, and a flag for whether splitBrainDetection
     * is enabled
     */
    public GossipData(int type, String group, Address mbr, List mbrs,
        boolean hasDistributedSystem, boolean floatingDisabled,
        boolean networkPartitionDetectionEnabled, Vector locators, Address localAddress) {
        this.type=type;
        this.group=group;
        this.mbr=mbr;
        this.mbrs=mbrs;
        this.hasDistributedSystem = hasDistributedSystem;
        this.floatingCoordinatorDisabled = floatingDisabled;
        this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
        this.locators = locators;
        this.localAddress = localAddress;
    }
    

    public int     getType()  {return type;}
    public String  getGroup() {return group;}
    public Address getMbr()   {return mbr;}
    public List getMbrs()  {return mbrs;}
    
    // GemStoneAddition
    public boolean getHasDistributedSystem() {
      return hasDistributedSystem;
    }
    
    // GemStoneAddition
    public boolean getFloatingCoordinatorDisabled() {
      return this.floatingCoordinatorDisabled;
    }
    
    // GemStoneAddition
    public boolean getNetworkPartitionDetectionEnabled() {
      return this.networkPartitionDetectionEnabled;
    }

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append("type=").append(type2String(type));
        switch(type) {
        case REGISTER_REQ:
            sb.append(" group=" + group + ", mbr=" + mbr /*+ ", locators=" + this.locators*/);
            break;

        case GET_REQ:
            sb.append(" group=" + group /*+ ", locators=" + this.locators*/);
            break;

        case GET_RSP:
            sb.append(" group=" + group + ", mbrs=" + mbrs
                + " hasDS=" + hasDistributedSystem + " coordinator="+mbr+" locators=" + this.locators); // GemStoneAddition
            break;

        case GEMFIRE_VERSION:
          if (this.versionOrdinal > 0) {
            sb.append(" version ordinal =" + versionOrdinal);
          }
          break;
        }

        return sb.toString();
    }


    public static String type2String(int t) {
        switch(t) {
        case REGISTER_REQ: return "REGISTER_REQ";
        case GET_REQ:      return "GET_REQ";
        case GET_RSP:      return "GET_RSP";
        case GEMFIRE_VERSION:      return "GEMFIRE_VERSION";
        default:           return "<unknown("+t+")>";
        }
    }

    
    public void toDataPre_GFE_8_0_0_0(DataOutput out) throws IOException {
      out.writeInt(type);
      if (type == GEMFIRE_VERSION) {
        out.writeShort(versionOrdinal);
      } else {
        out.writeUTF(group == null ? "" : group);
        JChannel.getGfFunctions().writeObject(mbr, out);
        JChannel.getGfFunctions().writeObject(mbrs, out);
        //DataSerializer.writeObject(this.locators, out);
        out.writeBoolean(hasDistributedSystem);
        out.writeBoolean(this.floatingCoordinatorDisabled);
        out.writeBoolean(this.networkPartitionDetectionEnabled);
      }
    }

    public void toData(DataOutput out) throws IOException {
      out.writeInt(type);
      if (type == GEMFIRE_VERSION) {
        out.writeShort(versionOrdinal);
      } else {
        out.writeUTF(group == null ? "" : group);
        JChannel.getGfFunctions().writeObject(mbr, out);
        JChannel.getGfFunctions().writeObject(mbrs, out);
        out.writeBoolean(hasDistributedSystem);
        out.writeBoolean(this.floatingCoordinatorDisabled);
        out.writeBoolean(this.networkPartitionDetectionEnabled);
        JChannel.getGfFunctions().writeObject(this.locators, out);
        JChannel.getGfFunctions().writeObject(this.localAddress, out);
      }
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      type=in.readInt();
      if (type == GEMFIRE_VERSION) {
        versionOrdinal = in.readShort();
      } else {
        group = in.readUTF();
        mbr = JChannel.getGfFunctions().readObject(in);
        mbrs = JChannel.getGfFunctions().readObject(in);
        //this.locators = (Vector)DataSerializer.readObject(in);
        hasDistributedSystem = in.readBoolean();
        this.floatingCoordinatorDisabled = in.readBoolean();
        this.networkPartitionDetectionEnabled = in.readBoolean();
        this.locators = JChannel.getGfFunctions().readObject(in);
        this.localAddress = JChannel.getGfFunctions().readObject(in);
      }
    }

    public void fromDataPre_GFE_8_0_0_0(DataInput in) throws IOException,
            ClassNotFoundException {
      type=in.readInt();
      if (type == GEMFIRE_VERSION) {
        versionOrdinal = in.readShort();
      } else {
        group = in.readUTF();
        mbr = JChannel.getGfFunctions().readObject(in);
        mbrs = JChannel.getGfFunctions().readObject(in);
        //this.locators = (Vector)DataSerializer.readObject(in);
        hasDistributedSystem = in.readBoolean();
        this.floatingCoordinatorDisabled = in.readBoolean();
        this.networkPartitionDetectionEnabled = in.readBoolean();
      }
    }

    /*
     * Versions in which the serialized form of this class or its contents has
     * been changed
     */
    private static short[] serializationVersions = new short[]{ JGroupsVersion.GFE_80_ORDINAL };
    
    @Override
    public short[] getSerializationVersions() {
      return serializationVersions;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
      toData(out);
    }

    @Override
    public void readFrom(DataInputStream in) throws IOException,
        IllegalAccessException, InstantiationException {
      try {
        fromData(in);
      } catch (ClassNotFoundException e) {
        throw new IOException("Error reading a GossipData structure", e);
      }
    }

}

