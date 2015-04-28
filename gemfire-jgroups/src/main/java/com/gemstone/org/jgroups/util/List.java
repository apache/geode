/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: List.java,v 1.10 2005/04/12 12:59:21 belaban Exp $

package com.gemstone.org.jgroups.util;

import java.io.*;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.Vector;


/**
 * Doubly-linked list. Elements can be added at head or tail and removed from head/tail.
 * This class is tuned for element access at either head or tail, random access to elements
 * is not very fast; in this case use Vector. Concurrent access is supported: a thread is blocked
 * while another thread adds/removes an object. When no objects are available, removal returns null.
 * @author Bela Ban
 */
public class List implements Externalizable, Cloneable {
    protected Element head=null, tail=null;
    protected int     size=0;
    protected transient final Object  mutex=new Object();



    static/*GemStoneAddition*/ class Element {
        Object obj=null;
        Element next=null;
        Element prev=null;

        Element(Object o) {
            obj=o;
        }
    }


    public List() {
    }


    /**
     Adds an object at the tail of the list.
     */
    public void add(Object obj) {
        Element el=new Element(obj);

        synchronized(mutex) {
            if(head == null) {
                head=el;
                tail=head;
                size=1;
            }
            else {
                el.prev=tail;
                tail.next=el;
                tail=el;
                size++;
            }
        }
    }

    /**
     Adds an object at the head of the list.
     */
    public void addAtHead(Object obj) {
        Element el=new Element(obj);

        synchronized(mutex) {
            if(head == null) {
                head=el;
                tail=head;
                size=1;
            }
            else {
                el.next=head;
                head.prev=el;
                head=el;
                size++;
            }
        }
    }


    /**
     Removes an object from the tail of the list. Returns null if no elements available
     */
    public Object remove() {
        Element retval=null;

        synchronized(mutex) {
            if(tail == null)
                return null;
            retval=tail;
            if(head == tail) { // last element
                head=null;
                tail=null;
            }
            else {
                tail.prev.next=null;
                tail=tail.prev;
                retval.prev=null;
            }

            size--;
        }
        return retval.obj;
    }


    /** Removes an object from the head of the list. Returns null if no elements available */
    public Object removeFromHead() {
        Element retval=null;

        synchronized(mutex) {
            if(head == null)
                return null;
            retval=head;
            if(head == tail) { // last element
                head=null;
                tail=null;
            }
            else {
                head=head.next;
                head.prev=null;
                retval.next=null;
            }
            size--;
        }
        return retval.obj;
    }


    /**
     Returns element at the tail (if present), but does not remove it from list.
     */
    public Object peek() {
        synchronized(mutex) {
            return tail != null ? tail.obj : null;
        }
    }


    /**
     Returns element at the head (if present), but does not remove it from list.
     */
    public Object peekAtHead() {
        synchronized(mutex) {
            return head != null ? head.obj : null;
        }
    }


    /**
     Removes element <code>obj</code> from the list, checking for equality using the <code>equals</code>
     operator. Only the first duplicate object is removed. Returns the removed object.
     */
    public Object removeElement(Object obj) {
        Element el=null;
        Object retval=null;

        synchronized(mutex) {
            el=head;
            while(el != null) {
                if(el.obj.equals(obj)) {
                    retval=el.obj;
                    if(head == tail) {           // only 1 element left in the list
                        head=null;
                        tail=null;
                    }
                    else
                        if(el.prev == null) {  // we're at the head
                            head=el.next;
                            head.prev=null;
                            el.next=null;
                        }
                        else
                            if(el.next == null) {  // we're at the tail
                                tail=el.prev;
                                tail.next=null;
                                el.prev=null;
                            }
                            else {                      // we're somewhere in the middle of the list
                                el.prev.next=el.next;
                                el.next.prev=el.prev;
                                el.next=null;
                                el.prev=null;
                            }
                    size--;
                    break;
                }

                el=el.next;
            }
        }
        return retval;
    }


    public void removeAll() {
        synchronized(mutex) {
            size=0;
            head=null;
            tail=null;
        }
    }


    public int size() {
        return size;
    }

    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer("[");
        Element el=head;

        while(el != null) {
            if(el.obj != null)
                ret.append(el.obj + " ");
            el=el.next;
        }
        ret.append(']');
        return ret.toString();
    }


    public String dump() {
        StringBuffer ret=new StringBuffer("[");
        for(Element el=head; el != null; el=el.next)
            ret.append(el.obj + " ");

        return ret.toString() + ']';
    }


    public Vector getContents() {
        Vector retval=new Vector(size);
        Element el;

        synchronized(mutex) {
            el=head;
            while(el != null) {
                retval.addElement(el.obj);
                el=el.next;
            }
        }
        return retval;
    }


    public Enumeration elements() {
        return new ListEnumerator(head);
    }


    public boolean contains(Object obj) {
        Element el=head;

        while(el != null) {
            if(el.obj != null && el.obj.equals(obj))
                return true;
            el=el.next;
        }
        return false;
    }


    public List copy() {
        List retval=new List();

        synchronized(mutex) {
            for(Element el=head; el != null; el=el.next)
                retval.add(el.obj);
        }
        return retval;
    }


    @Override // GemStoneAddition
    protected Object clone() throws CloneNotSupportedException {
        // calling clone() is superfluous because we don't want a shallow copy
        return copy();
    }



    public void writeExternal(ObjectOutput out) throws IOException {
        Element el;

        synchronized(mutex) {
            el=head;
            out.writeInt(size);
            for(int i=0; i < size; i++) {
                out.writeObject(el.obj);
                el=el.next;
            }
        }
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        Object obj;
        int new_size=in.readInt();

        if(new_size == 0)
            return;
        for(int i=0; i < new_size; i++) {
            obj=in.readObject();
            add(obj);
        }
    }


//    public void writeTo(DataOutputStream out) throws IOException {
//        Element el;
//        Object obj;
//        if(size == 0) {
//            out.writeInt(0);
//            return;
//        }
//        out.writeInt(size);
//        el=head;
//        while(el != null) {
//            obj=el.obj;
//            if(obj instanceof Streamable) {
//                out.writeByte(1);
//                ((Streamable)obj).writeTo(out);
//            }
//            else {
//                out.writeByte(0);
//                ObjectOutputStream oos=new ObjectOutputStream(out); // very inefficient
//                oos.writeObject(obj);
//                oos.close();
//            }
//            el=el.next;
//        }
//    }
//
//    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
//        Object obj;
//        int    size=in.readInt();
//        byte   b;
//
//        for(int i=0; i < size; i++) {
//            b=in.readByte();
//            if(b == 1) {
//
//            }
//            else if(b == 0) {
//
//            }
//            else
//                throw new InstantiationException("byte '" + b + "' not recognized (needs to be 1 or 0)");
//        }
//
//    }




    static/*GemStoneAddition*/ class ListEnumerator implements Enumeration {
        Element curr=null;

        ListEnumerator(Element start) {
            curr=start;
        }

        public boolean hasMoreElements() {
            return curr != null;
        }

        public Object nextElement() {
            Object retval;

            if(curr == null)
                throw new NoSuchElementException();
            retval=curr.obj;
            curr=curr.next;
            return retval;
        }

    }




//      public static void main(String args[]) {
//  	List l=new List();

//  	l.add("Bela");
//  	l.add("Janet");
//  	l.add("Marco");
//  	l.add("Ralph");

//  	for(Enumeration e=l.elements(); e.hasMoreElements();) {
//  	    System.out.println(e.nextElement());
//  	}

//  	System.out.println(l + ".contains(\"Bela\"): " + l.contains("Bela"));


//  	l.add(Integer.valueOf(1));
//  	l.add(Integer.valueOf(2));
//  	l.add(Integer.valueOf(5));
//  	l.add(Integer.valueOf(6));


//  	System.out.println(l + ".contains(2): " + l.contains(Integer.valueOf(2)));
//      }






//    public static void main(String[] args) {
//        List l=new List();
//        Long n;
//
//
//        l.addAtHead(Integer.valueOf(1));
//        l.addAtHead(Integer.valueOf(2));
//        l.addAtHead(Integer.valueOf(3));
//        l.addAtHead(Integer.valueOf(4));
//        l.addAtHead(Integer.valueOf(5));
//
//        System.out.println("Removed from head: " + l.removeFromHead());
//        System.out.println("Removed from head: " + l.removeFromHead());
//        System.out.println("Removed from head: " + l.removeFromHead());
//        System.out.println("Removed from head: " + l.removeFromHead());
//        System.out.println("Removed from head: " + l.removeFromHead());
//        System.out.println("Removed from head: " + l.removeFromHead());
//        System.out.println("Removed from head: " + l.removeFromHead());
//
//
//        System.out.print("Adding 50000 numbers:");
//        for(long i=0; i < 50000; i++) {
//            n=Long.valueOf(i);
//            if(i % 2 == 0) {
//                l.addAtHead(n);
//            }
//            else {
//                l.add(n);
//            }
//        }
//        System.out.println(" OK");
//
//        long num=0;
//        System.out.print("Removing all elements: ");
//        while((l.remove()) != null)
//            num++;
//        System.out.println("OK, removed " + num + " objects");
//    }





}
