/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Marshaller.java,v 1.5 2004/10/04 20:43:35 belaban Exp $

package com.gemstone.org.jgroups.util;



import com.gemstone.org.jgroups.ChannelException;
import com.gemstone.org.jgroups.conf.ClassConfigurator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;




/**
 * Title:        JGroups Communications
 * Description:  Contact me at <a href="mailto:mail@filip.net">mail@filip.net</a>
 * Copyright:    Copyright (c) 2002
 * Company:      www.filip.net
 * @author Filip Hanik
 * @author Bela Ban
 * @version 1.0
 *
 * This class marshalls classes, in other words it serializes and deserializes classes
 * to and from object streams.
 * It performs a magic number matching to decrease the number of bytes that are being sent
 * over the wire.
 * If no magic number is available for the class, the classname is sent over instead
 */
public class Marshaller {
    /**
     * The class configurator maps classes to magic numbers
     */
    private static final ClassConfigurator mConfigurator;

    static {
        try {
            mConfigurator=ClassConfigurator.getInstance(true);
        }
        catch(ChannelException e) {
            throw new ExceptionInInitializerError(e.toString());
        }
    }


    public Marshaller() {
    }

    /**
     * reads the magic number, instantiates the class (from the
     * configurator) and invokes the readExternal method on the object.
     * If no magic number is present, the method will read the
     * string and then get the class from the configurator.
     * @param in an ObjectInput stream - the stream should be composed as follows:<BR>
     *   [boolean -> int|string -> object data]
     * <BR>
     * If the boolean is true, then the next value is an int, the magic number.<BR>
     * If the boolean is false, then the next value is a string (the class name)<BR>
     * The object data is what the object instance uses to populate its fields<BR>
     */
    public static Externalizable read(ObjectInput in) throws IOException {
        try {
            boolean is_null=in.readBoolean();
            if(is_null)
                return null;

            //see if we want to use a magic number
            boolean usemagic=in.readBoolean();
            //the class that we will use to instantiate the object with
            Class extclass=null;
            if(usemagic) {
                //read the magic number
                int magic=in.readInt();
                //from the magic number, get the class
                extclass=mConfigurator.get(magic);
            }
            else {
                //we don't have a magic number, read the class name
                String magic=in.readUTF();
                //get the class, ie let the configurator load it
                extclass=mConfigurator.get(magic);
            }//end if
            //instantiate the object
            Externalizable newinstance=(Externalizable)extclass.newInstance();
            //populate the object with its data
            newinstance.readExternal(in);
            //return the instance
            return newinstance;
        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch(Throwable x) {
            if(x instanceof IOException)
                throw (IOException)x;
            else
                throw new IOException(x.toString());
        }
    }

    /**
     * Writes an object to the ObjectOutput stream.
     * If possible, we will send over a magic number instead of the class name
     * so that we transfer less amount of data.
     * @param inst - an object instance to be serialized, can not be null
     * @param out - the ObjectOutput stream we will write the serialized data to
     */
    public static void write(Externalizable inst, ObjectOutput out) throws IOException {
        boolean is_null=(inst == null);
        try {
            // if inst is a null value we write this first
            out.writeBoolean(is_null);
            if(is_null)
                return;

            //find out if we have a magic number for this class
            int magic=mConfigurator.getMagicNumber(inst.getClass());
            //-1 means no magic number otherwise we have one
            if(magic != -1) {
                //true means we use a magic number
                out.writeBoolean(true);
                //write the magic number
                out.writeInt(magic);
            }
            else {
                //we don't have a magic number
                out.writeBoolean(false);
                //write the classname instead
                out.writeUTF(inst.getClass().getName());
            }//end if
            //write the object data
            inst.writeExternal(out);
        }
        catch(Exception x) {
            if(x instanceof IOException)
                throw (IOException)x;
            else
                throw new java.io.IOException(x.toString());
        }
    }


}
