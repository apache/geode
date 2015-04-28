/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.ChannelException;
import com.gemstone.org.jgroups.conf.ClassConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;

/**
 * Uses magic numbers for class descriptors
 * @author Bela Ban
 * @version $Id: MagicObjectInputStream.java,v 1.4 2004/10/04 20:47:57 belaban Exp $
 */
public class MagicObjectInputStream extends ContextObjectInputStream  {
    static ClassConfigurator conf=null;
    static final GemFireTracer log=GemFireTracer.getLog(MagicObjectInputStream.class);


    public MagicObjectInputStream(InputStream is) throws IOException {
        super(is);
        if(conf == null) {
            try {
                conf=ClassConfigurator.getInstance(false);
            }
            catch(ChannelException e) {
                log.error(ExternalStrings.MagicObjectInputStream_CLASSCONFIGURATOR_COULD_NOT_BE_INSTANTIATED, e);
            }
        }
    }


    @Override // GemStoneAddition
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        ObjectStreamClass retval;
        int magic_num=super.readInt();

        if(conf == null || magic_num == -1) {
            return super.readClassDescriptor();
        }

        retval=conf.getObjectStreamClassFromMagicNumber(magic_num);
        if(retval == null)
            throw new ClassNotFoundException("failed fetching class descriptor for magic number " + magic_num);
        //if(log.isTraceEnabled())
            //log.trace("reading descriptor (from " + magic_num + "): " + retval.getName());
        return retval;
    }
}

