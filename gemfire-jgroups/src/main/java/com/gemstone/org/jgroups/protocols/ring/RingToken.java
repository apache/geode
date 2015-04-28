/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
//$Id: RingToken.java,v 1.5 2004/09/15 17:40:59 belaban Exp $

package com.gemstone.org.jgroups.protocols.ring;

import com.gemstone.org.jgroups.Address;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.TreeSet;


public class RingToken implements Externalizable
{

   public static final int OPERATIONAL = 0;
   public static final int RECOVERY = 1;

   private int type = -1;
   private long tokenSeq;
   private long seq;
   private long aru;
   private int fcc;
   private int backlog;
   private int windowSize;
   private int windowThreshold;
   private Address aruId;
   private Collection retransmissionRequests;
   private Collection recoveredMembers;

   public RingToken()
   {
      this(OPERATIONAL);
   }

   public RingToken(int type)
   {
      if (type != OPERATIONAL && type != RECOVERY)
      {
         throw new IllegalArgumentException("Illegal Ring type");
      }
      this.type = type;
      initToken();
   }

   private void initToken()
   {
      retransmissionRequests = new TreeSet();
   }

   public void setAruId(Address address)
   {
      aruId= address;
   }

   public Address getAruId()
   {
      return aruId;
   }

   public int getType()
   {
      return type;
   }

   public void setType(int type)
   {
      if (type != OPERATIONAL && type != RECOVERY)
      {
         throw new IllegalArgumentException("Illegal Ring type");
      }
      this.type = type;

   }


   public long getTokenSequence()
   {
      return tokenSeq;
   }

   public void incrementTokenSequence()
   {
      tokenSeq++;
   }

   public long getHighestSequence()
   {
      return seq;
   }

   public void setHighestSequence(long highestSequence)
   {
      if (seq > highestSequence)
         throw new IllegalArgumentException("Can not set highest sequence to be" +
                                            " lower than current higest sequence " + seq);
      this.seq = highestSequence;
   }

   public long getAllReceivedUpto()
   {
      return aru;
   }

   public void setAllReceivedUpto(long aru)
   {
      this.aru = aru;

      if (aru > seq)
      {
         seq = aru;
      }
   }

   public int getLastRoundBroadcastCount()
   {
      return fcc;
   }

   public void addLastRoundBroadcastCount(int transmitCount)
   {
      this.fcc += transmitCount;
      if (fcc < 0)
         fcc = 0;
   }

   public int getBacklog()
   {
      return backlog;
   }

   public void addBacklog(int back)
   {
      this.backlog += back;

      if (backlog < 0) backlog = 0;
   }

   public void setWindowSize(int newSize)
   {
      if (newSize < 0)
      {
         windowSize = 0;
      }
      else
      {
         windowSize = newSize;
      }
   }

   public void addRecoveredMember(Address member)
   {

      if (recoveredMembers == null)
         recoveredMembers = new TreeSet();

      recoveredMembers.add(member);
   }

   public Collection getRecoveredMembers()
   {
      return recoveredMembers;
   }

   public int getWindowSize()
   {
      return windowSize;
   }

   public void setWindowThreshold(int newSize)
   {
      if (newSize < 0)
      {
         windowThreshold = 0;
      }
      else
      {
         windowThreshold = newSize;
      }
   }

   public int getWindowThreshold()
   {
      return windowThreshold;
   }

   public Collection getRetransmissionRequests()
   {
      return retransmissionRequests;
   }

   public void writeExternal(ObjectOutput oo) throws IOException
   {
      oo.writeLong(tokenSeq);
      oo.writeLong(seq);
      oo.writeInt(type);
      oo.writeLong(aru);
      oo.writeInt(fcc);
      oo.writeInt(backlog);
      oo.writeInt(windowSize);
      oo.writeInt(windowThreshold);
      oo.writeObject(aruId);
      oo.writeObject(retransmissionRequests);
      oo.writeObject(recoveredMembers);

   }

   public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException
   {
      tokenSeq = oi.readLong();
      seq = oi.readLong();
      type = oi.readInt();
      aru = oi.readLong();
      fcc = oi.readInt();
      backlog = oi.readInt();
      windowSize = oi.readInt();
      windowThreshold = oi.readInt();
      aruId = (Address)oi.readObject();
      retransmissionRequests = (Collection) oi.readObject();
      recoveredMembers = (Collection) oi.readObject();
   }

   @Override // GemStoneAddition
   public String toString()
   {
      StringBuffer buf = new StringBuffer(200);
      buf.append("Token[tokenSeq=").append(tokenSeq);
      buf.append(",type=").append(type);
      buf.append(",highestseq=").append(seq);
      buf.append(",aru=").append(aru);
      buf.append(",lastRoundTransmitCount=").append(fcc);
      buf.append(",backlog=").append(backlog);
      buf.append(",windowSize=").append(windowSize);
      buf.append(",windowThreshold=").append(windowThreshold);
      buf.append(",retransmissionList=").append(getRetransmissionRequests());
      buf.append(']');
      return buf.toString();
   }
}
