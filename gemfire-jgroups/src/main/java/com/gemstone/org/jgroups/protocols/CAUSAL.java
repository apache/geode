/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: CAUSAL.java,v 1.8 2005/07/17 11:36:15 chrislott Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Vector;




/** <p>
 * Implements casual ordering layer using vector clocks.
 * </p>
 * <p>
 * Causal protocol layer guarantees that if message m0 multicasted
 * by a process group member p0 causes process group member
 * p1 to multicast message p1 then all other remaining process group
 * members in a current view will receive messages in order m0
 * followed by m1.
 * </p>
 * <p>
 * First time encountered, causal order seems very similar to FIFO order but
 * there is an important distinction.  While FIFO order gurantees that
 * if process group member p0 multicasts m0 followed by m1 the messages
 * will be delivered in order m0,m1 to all other group members, causal
 * order expands this notion of an order from a single group member "space"
 * to a whole group space i.e if p0 sends message m0 which causes member
 * p1 to send message m1 then all other group members are guaranteed to
 * receive m0 followed by m1.
 * </p>
 * <p>
 * Causal protocol layer achieves this ordering type by introducing sense of
 * a time in a group using vector clocks.  The idea is very simple. Each message
 * is labeled by a vector, contained in a causal header, representing the number of
 * prior causal messages received by the sending group member. Vector time of [3,5,2,4] in
 * a group of four members [p0,p1,p2,p3] means that process p0 has sent 3 messages
 * and has received 5,2 and 4 messages from a member p1,p2 and p3 respectively.
 * </p>
 * <p>
 * Each member increases its counter by 1 when it sends a message. When receiving
 * message mi from a member pi , (where pi != pj) containing vector time VT(mi),
 * process pj delays delivery of a message mi until:
 * </p>
 * <p>
 * for every k:1..n
 *
 *                VT(mi)[k] == VT(pj)[k] + 1    if k=i,
 *                VT(mi)[k] <= VT(pj)[k]        otherwise
 * </p>
 * <p>
 * After the next causal message is delivered at process group pj, VT(pj) is
 * updated as follows:
 *</p>
 *<p>
 *    for every k:1...n VT(pj)[k] == max(VT(mi)[k],VT(pj)[k])
 *</p>
 *  @author Vladimir Blagojevic vladimir@cs.yorku.ca
 *  @version $Revision: 1.8 $
 *
 **/


public class CAUSAL extends Protocol
  {

   public static class CausalHeader extends Header
    {
      /**
       * vector timestamp of this header/message
       */
      private TransportedVectorTime t;

      /**
       *used for externalization
       */
      public CausalHeader()
      {
      }

      public CausalHeader(TransportedVectorTime timeVector)
      {
         t = timeVector;
      }

      /**
       *Returns a vector timestamp carreid by this header
       *@return Vector timestamp contained in this header
       */
      public TransportedVectorTime getVectorTime()
      {
         return t;
      }

      /**
       * Size of this vector timestamp estimation, used in fragmetation
       * @return headersize in bytes
       */
      @Override // GemStoneAddition
      public long size(short version) {

         /*why 231, don't know but these are this values I get when
         flattening the object into byte buffer*/
         return 231 + (t.size() * 4);
      }

      /**
       * Manual serialization
       *
       *
       */
      public void writeExternal(ObjectOutput out) throws IOException
      {
         out.writeObject(t);
      }

      /**
       * Manual deserialization
       *
       */
      public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException
      {
         t = (TransportedVectorTime) in.readObject();
      }

      @Override // GemStoneAddition
      public String toString()
      {
         return "[CAUSALHEADER:" + t + ']';
      }
   }


   /**
    *Vector time clock belonging to the member that "owns" this stack
    */
   private VectorTime localVector;

   /**
    * dealy queue containg messages waiting for the delivery i.e causal order
    */
   private LinkedList delayQueue;

   /**
    *Address of this group member
    */
   private Address localAddress;


   /**
    *default constructor
    */
   public CAUSAL()
   {
   }

   /**
    * Adds a vectortimestamp to a sorted queue
    * @param tvt A vector time stamp
    */
   private void addToDelayQueue(TransportedVectorTime tvt)
   {
      ListIterator i = delayQueue.listIterator(0);
      TransportedVectorTime current = null;
      while (i.hasNext())
      {
         current = (TransportedVectorTime) i.next();
         if (tvt.lessThanOrEqual(current))
         {
            delayQueue.add(i.previousIndex(), tvt);
            return;
         }
      }
      delayQueue.add(tvt);
   }

   /**
    * Processes Event going down in the stack
    * @param evt Event passed from the stack above Causal
    */
   @Override // GemStoneAddition
   public void down(Event evt)
   {
      switch (evt.getType())
      {
         case Event.MSG:
            Message msg = (Message) evt.getArg();

            //dont stamp unicasts
            if (msg.getDest() != null && !msg.getDest().isMulticastAddress())
               break;

              Message causalMsg=new Message(msg.getDest(), msg.getSrc(), msg);
              synchronized (this)
            {
               localVector.increment();
               causalMsg.putHeader(getName(), new CausalHeader(localVector.getTransportedVectorTime()));
            }
            passDown(new Event(Event.MSG, causalMsg));
            return;
      }
      passDown(evt);
   }

   /**
    * Processes Event going up through the stack
    * @param evt Event passed from the stack below Causal
    */
   @Override // GemStoneAddition
   public void up(Event evt)
   {
      switch (evt.getType())
      {
         case Event.SET_LOCAL_ADDRESS:
            localAddress = (Address) evt.getArg();
            localVector = new VectorTime(localAddress);
            delayQueue = new LinkedList();
            break;

         case Event.VIEW_CHANGE:
            Vector newViewMembers = ((View) evt.getArg()).getMembers();
            localVector.merge((Vector) newViewMembers.clone());
            localVector.reset();
            break;

         case Event.MSG:
            Object obj = null;
            Message msg = (Message) evt.getArg();

            obj = msg.getHeader(getName()); // GemStoneAddition rewritten because Eclipse generates spurious warning
            if (!(obj instanceof CausalHeader))
            {
               if((msg.getDest() == null || msg.getDest().isMulticastAddress()) 
                       && log.isErrorEnabled()) log.error(ExternalStrings.CAUSAL_NO_CAUSALHEADER_FOUND);
	       passUp(evt);
	       return;
            }

            CausalHeader header = (CausalHeader) obj;
            TransportedVectorTime messageVector = header.getVectorTime();

            synchronized (this)
            {
               if (localVector.isCausallyNext(messageVector))
               {
                   Message tmp=(Message)msg.getObject();
                   tmp.setSrc(msg.getSrc());
                   passUp(new Event(Event.MSG, tmp));
                  localVector.max(messageVector);
               }
               else
               {
                  messageVector.setAssociatedMessage(msg);
                  addToDelayQueue(messageVector);
               }
               TransportedVectorTime queuedVector = null;
               while ((delayQueue.size() > 0) &&
                     localVector.isCausallyNext((queuedVector = (TransportedVectorTime) delayQueue.getFirst())))
               {
                  delayQueue.remove(queuedVector);
                   Object tmp=queuedVector.getAssociatedMessage().getObject();
                   passUp(new Event(Event.MSG, tmp));
                  localVector.max(queuedVector);
               }
               return;
            }

      }
      passUp(evt);
   }

   /**
    * Returns a name of this stack, each stackhas to have unique name
    * @return stack's name - CAUSAL
    */
   @Override // GemStoneAddition
   public String getName()
   {
      return "CAUSAL";
   }


}
