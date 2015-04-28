/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;


/**
 * A bounded subclass of List, oldest elements are removed once max capacity is exceeded
 * @author Bela Ban Nov 20, 2003
 * @version $Id: BoundedList.java,v 1.2 2004/07/26 15:23:26 belaban Exp $
 */
public class BoundedList extends List {
    int max_capacity=10;



    public BoundedList() {
    }

    public BoundedList(int size) {
        super();
        max_capacity=size;
    }


    /**
     * Adds an element at the tail. Removes an object from the head if capacity is exceeded
     * @param obj The object to be added
     */
    @Override // GemStoneAddition
    public void add(Object obj) {
        if(obj == null) return;
        while(size >= max_capacity && size > 0) {
            removeFromHead();
        }
        super.add(obj);
    }


    /**
     * Adds an object to the head, removes an element from the tail if capacity has been exceeded
     * @param obj The object to be added
     */
    @Override // GemStoneAddition
    public void addAtHead(Object obj) {
        if(obj == null) return;
        while(size >= max_capacity && size > 0) {
            remove();
        }
        super.addAtHead(obj);
    }
}
