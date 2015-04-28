/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Membership.java,v 1.8 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;


import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.GemFireTracer;


import java.util.*;


/**
 * Class to keep track of Addresses.
 * The membership object holds a vector of Address objects that are in the same membership.
 * Each unique address can only exist once; i.e., doing Membership.add(existing_address) 
 * will be ignored.
 */
public class Membership implements Cloneable {
  // GemStoneAddition - hook added for split-brain testing
  public final static boolean FLOATING_COORD_DISABLED = Boolean.getBoolean("gemfire.disable-floating-coordinator");
  
    /* private vector to hold all the addresses */
    private final LinkedList members=new LinkedList();
    protected static final GemFireTracer log=GemFireTracer.getLog(Membership.class);

    /**
     * Public constructor
     * Creates a member ship object with zero members
     */
    public Membership() {
    }


    /**
     * Creates a member ship object with the initial members.
     * The Address references are copied out of the vector, so that the
     * vector passed in as parameters is not the same reference as the vector
     * that the membership class is using
     *
     * @param initial_members - a list of members that belong to this membership
     */
    public Membership(Collection initial_members) {
        if(initial_members != null)
            add(initial_members);
    }



    /**
     * returns a copy (clone) of the members in this membership.
     * the vector returned is immutable in reference to this object.
     * ie, modifying the vector that is being returned in this method
     * will not modify this membership object.
     *
     * @return a list of members,
     */
    public Vector getMembers() {
        /*clone so that this objects members can not be manipulated from the outside*/
        synchronized(members) {
            return new Vector(members);
        }
    }


    /**
     * Adds a new member to this membership.
     * If the member already exist (Address.equals(Object) returns true then the member will
     * not be added to the membership
     */
    public void add(Address new_member) {
        synchronized(members) {
            if(new_member != null && !members.contains(new_member)) {
                members.add(new_member);
            }
        }
    }


    /**
     * Adds a list of members to this membership
     *
     * @param v - a vector containing Address objects
     * @throws ClassCastException if v contains objects that don't implement the Address interface
     * @see #add(Collection)
     */
    public void add(Collection v) {
        if(v != null) {
          synchronized(members) { // GemStoneAddition
            HashSet currentMembers = new HashSet(members); // GemStoneAddition - avoid scanning on each add()
            for(Iterator it=v.iterator(); it.hasNext();) {
                Address addr=(Address)it.next();
                if (!currentMembers.contains(addr)) {
                  members.add(addr);
                }
            }
          }
        }
    }


    /**
     * removes an member from the membership.
     * If this member doesn't exist, no action will be performed on the existing membership
     *
     * @param old_member - the member to be removed
     */
    public void remove(Address old_member) {
        if(old_member != null) {
            synchronized(members) {
                members.remove(old_member);
            }
        }
    }


    /**
     * removes all the members contained in v from this membership
     *
     * @param v - a vector containing all the members to be removed
     */
    public void remove(Collection v) {
        if(v != null) {
            synchronized(members) {
                members.removeAll(v);
            }
        }
    }


    /**
     * removes all the members from this membership
     */
    public void clear() {
        synchronized(members) {
            members.clear();
        }
    }

    /**
     * Clear the membership and adds all members of v
     * This method will clear out all the old members of this membership by
     * invoking the <code>Clear</code> method.
     * Then it will add all the all members provided in the vector v
     *
     * @param v - a vector containing all the members this membership will contain
     */
    public void set(Collection v) {
      synchronized(members) { // GemStoneAddition
        clear();
        if(v != null) {
            add(v);
        }
      }
    }


    /**
     * Clear the membership and adds all members of v
     * This method will clear out all the old members of this membership by
     * invoking the <code>Clear</code> method.
     * Then it will add all the all members provided in the vector v
     *
     * @param m - a membership containing all the members this membership will contain
     */
    public void set(Membership m) {
      synchronized(members) { // GemStoneAddition - needs to be atomic
        clear();
        if(m != null) {
            add(m.getMembers());
        }
      }
    }


    /**
     * merges membership with the new members and removes suspects
     * The Merge method will remove all the suspects and add in the new members.
     * It will do it in the order
     * 1. Remove suspects
     * 2. Add new members
     * the order is very important to notice.
     *
     * @param new_mems - a vector containing a list of members (Address) to be added to this membership
     * @param suspects - a vector containing a list of members (Address) to be removed from this membership
     */
    public void merge(Collection new_mems, Collection suspects) {
      synchronized(members) { // GemStoneAddition - needs to be atomic
        remove(suspects);
        add(new_mems);
      }
    }


    /**
     * Returns true if the provided member belongs to this membership
     *
     * @param member
     * @return true if the member belongs to this membership
     */
    public boolean contains(Address member) {
        if(member == null) return false;
        synchronized(members) {
            return members.contains(member);
        }
    }
    
    /**
     * Returns true if the provide member belongs to this membership using
     * unique ID information as well as the basic checks
     */
    public boolean containsExt(Address member) {
      if (member == null || !(member instanceof IpAddress)) return false;
      IpAddress pmbr = (IpAddress)member;
      synchronized(members) {
        for (Iterator<IpAddress> it = members.iterator(); it.hasNext(); ) {
          IpAddress each = it.next();
          if (each.equals(pmbr) && each.getBirthViewId() == pmbr.getBirthViewId()) {
            return true;
          }
        }
      }
      return false;
    }


    /* Simple inefficient bubble sort, but not used very often (only when merging) */
    public void sort() {
        synchronized(members) {
            Collections.sort(members);
        }
    }




    /**
     * returns a copy of this membership
     *
     * @return an exact copy of this membership
     */
    public Membership copy() {
      synchronized(members) { // GemStoneAddition
        return ((Membership)clone());
      }
    }


    /**
     * @return a clone of this object. The list of members is copied to a new
     *         container
     */
    @Override // GemStoneAddition
    public Object clone() {
        return new Membership(this.members);
    }


    /**
     * Returns the number of addresses in this membership
     *
     * @return the number of addresses in this membership
     */
    public int size() {
        synchronized(members) {
            return members.size();
        }
    }

    /**
     * Returns the component at the specified index
     *
     * @param index - 0..size()-1
     * @throws ArrayIndexOutOfBoundsException - if the index is negative or not less than the current size of this Membership object.
     * @see java.util.Vector#elementAt
     */

    public Object elementAt(int index) {
        synchronized(members) {
            return members.get(index);
        }
    }
    
    /**
     * GemStoneAddition - force coordinator to be in a locator
     * @return the current coordinator's address
     */
    public Address getCoordinator() {
      synchronized(members) {
        for (Iterator it=members.iterator(); it.hasNext(); ) {
          Address addr = (Address)it.next();
          if (addr.preferredForCoordinator()) {
            return addr;
          }
        }
        if (members.size() > 0) {
          return (Address)members.get(0);
        }
      }
      return null;
    }
    
    /**
     * GemStoneAddition - is the given address the next coordinator?
     * @return true if so
     */

    public boolean wouldBeNewCoordinator(Address possible) {
      Address coord = null;
      synchronized(members) {
        for (Iterator it=members.iterator(); it.hasNext(); ) {
          Address a = (Address)it.next();
          if (a.preferredForCoordinator()) {
            if (coord == null) {
              coord = a; // current coordinator
            }
            else {
              return a.equals(possible);
            }
          }
        }
      }
      return false;
    }
    

    @Override // GemStoneAddition
    public String toString() {
        synchronized(members) {
            return members.toString();
        }
    }


    /**
     * GemStoneAddition - determine whether there are any members aside
     * from the ones given in <i>suspects</i> that can be coordinator
     * @param suspects members that should be ignored
     * @return whether there are other members that could be coordinator (e.g., a locator)
     */
    public boolean hasOtherCoordinators(Vector suspects) {
      synchronized(members) {
        for (Iterator it=members.iterator(); it.hasNext(); ) {
          Address a = (Address)it.next();
          if (a.preferredForCoordinator() && !suspects.contains(a)) {
            return true;
          }
        }
      }
      return false;
    }


}
