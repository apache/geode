/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: VectorTime.java,v 1.5 2005/07/17 11:36:15 chrislott Exp $


package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.Address;

import java.util.*;



/**
 * Vector timestamp used in CAUSAL order protocol stack
 *
 * @author Vladimir Blagojevic vladimir@cs.yorku.ca
 * @version $Revision: 1.5 $
 */
public class VectorTime
{
   /**
    * vector entry sorted map of members
    */
   private final TreeMap entries;

   /**
    * index of the group member that owns this VectorTime
    */
   private int ownerIndex;

   /**
    * address of the owner member
    */
   private final Address owner;

   /**
    * Constructs VectorTime given an adress of a owning group member
    * @param owner Address of the owner group member
    */
   public VectorTime(Address owner)
   {
      this.owner = owner;
      entries = new TreeMap();
      entries.put(owner, Integer.valueOf(0));
   }

   /**
    * Returns Collection containing addresses of other group members from this Vector clock
    * @return Addresses of other group members
    */
   public Collection getMembers()
   {
      return entries.keySet();
   }

   /**
    *Returns Vector clock values of this Vector clock
    * @return values of the Vector clock
    */
   public java.util.Collection getVectorValues()
   {
      return entries.values();
   }

   /**
    *Returns Vector clock values of this Vector clock
    * @return values of the Vector clock as an array
    */
   public int[] getValues()
   {
      int count = 0;
      Collection valuesEntries = entries.values();
      int values [] = new int[valuesEntries.size()];
      Iterator iter = valuesEntries.iterator();

      while (iter.hasNext())
      {
         values[count++] = ((Integer) iter.next()).intValue();
      }
      return values;
   }

   /**
    * Incerements owners current vector value by 1
    */
   public void increment()
   {
      Integer value = (Integer) entries.get(owner);
      entries.put(owner, Integer.valueOf(value.intValue() + 1));
   }

   /**
    * Resets all the values in this vector clock to 0
    */
   public void reset()
   {
      Address member = null;
      Set keyEntries = entries.keySet();
      Iterator iter = keyEntries.iterator();
      while (iter.hasNext())
      {
         member = (Address) iter.next();
         entries.put(member, Integer.valueOf(0));
      }

   }

   /**
    * Returns a minimal lightweight representation of this Vector Time
    * suitable for network transport.
    * @return lightweight representation of this VectorTime in the
    * form of TransportedVectorTime object
    */
   public TransportedVectorTime getTransportedVectorTime()
   {
      return new TransportedVectorTime(ownerIndex, getValues());
   }

   /**
    *<p>
    *Maxes this VectorTime with the specified TransportedVectorTime.
    *Updates this VectorTime as follows:
    *</p>
    *<p>
    *    for every k:1...n VT(pj)[k] == max(VT(mi)[k],VT(pj)[k])
    *</p>
    *
    * @param other TransportedVectorTime that is max-ed with this VectorTime
    */
   public void max(TransportedVectorTime other)
   {
      int count = 0;
      int thisVectorValue = 0;
      int otherVectorValue = 0;
      Address member = null;

      int values[] = other.getValues();
      Set keyEntries = entries.keySet();
      Iterator iter = keyEntries.iterator();


      while (iter.hasNext())
      {
         member = (Address) iter.next();
         thisVectorValue = ((Integer) entries.get(member)).intValue();
         otherVectorValue = values[count++];
         if (otherVectorValue > thisVectorValue)
            entries.put(member, Integer.valueOf(otherVectorValue));
      }

   }

   /**
    * Determines if the vector clock represented by TransportedVectorTime is
    * causally next to this VectorTime
    * @param other TransportedVectorTime representation of vector clock
    * @return true if the given TransportedVectorTime is the next causal to this VectorTime
    */
   public boolean isCausallyNext(TransportedVectorTime other)
   {
      int senderIndex = other.getSenderIndex();
      int receiverIndex = ownerIndex;

      int[] sender = other.getValues();
      int[] receiver = getValues();

      boolean nextCasualFromSender = false;
      boolean nextCasual = true;

      if (receiverIndex == senderIndex) return true;
      for (int k = 0; k < receiver.length; k++)
      {
         if ((k == senderIndex) && (sender[k] == receiver[k] + 1))
         {
            nextCasualFromSender = true;
            continue;
         }
         if (k == receiverIndex) continue;
         if (sender[k] > receiver[k])
            nextCasual = false;
      }
      return (nextCasualFromSender && nextCasual);
   }

   /**
    * Returns owner index in this VectorTime clock
    * @return index of the owner of this VectorTime or -1 if not found
    */
   public int getOwnerIndex()
   {
      return indexOf(owner);
   }

   /**
    * Returns index of the given member represented by it's Address
    * @param member group member represented by this Address
    * @return index of the group member or -1 if not found
    */
   public int indexOf(Address member)
   {
      Set set = entries.keySet();
      Iterator iter = set.iterator();
      int index = -1;
      Address temp = null;


      while (iter.hasNext())
      {
         temp = (Address) iter.next();
         index++;
         if (temp.hashCode() == member.hashCode())
         {
            return index;
         }
      }
      return -1;
   }

   /**
    * Merges this VectorTime with new members of the group
    * VectorTime can possibly either grow or shrink
    * @param newMembers members of this group
    */
   public void merge(Vector newMembers)
   {
      if (newMembers.size() > entries.size())
      {
         newMembers.removeAll(entries.keySet());
         intializeEntries(newMembers);
      }
      else
      {
         entries.keySet().retainAll(newMembers);
      }
      ownerIndex = indexOf(owner);
   }

   /**
    * return String representation of the VectorTime
    * @return String representation of this VectorTime object
    */
   @Override // GemStoneAddition
   public String toString()
   {
      String classType = "VectorTime";
      int bufferSize = (entries.size() *2) + classType.length() +2; //2 for brackets
      StringBuffer buf = new StringBuffer(bufferSize);
      buf.append(classType);
      buf.append(entries);
      return buf.toString();
   }

   /**
    * Initializes entries of the new members in the VectorTime clock
    * @param c Collection containing members of this VectorTime that need to be initialized
    */
   private void intializeEntries(Collection c)
   {
      Iterator iter = c.iterator();
      while (iter.hasNext())
      {
         Address newMember = (Address) iter.next();
         entries.put(newMember, Integer.valueOf(0));
      }
   }
}

