/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.ChannelException;
import com.gemstone.org.jgroups.conf.ClassConfigurator;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;

/**
 * Uses magic numbers for class descriptors
 * @author Bela Ban
 * @version $Id: MagicObjectOutputStream.java,v 1.4 2004/10/04 20:43:35 belaban Exp $
 */
public class MagicObjectOutputStream extends ObjectOutputStream  {
    static ClassConfigurator conf=null;
    static final GemFireTracer log=GemFireTracer.getLog(MagicObjectOutputStream.class);


    public MagicObjectOutputStream(OutputStream out) throws IOException {
        super(out);
        if(conf == null) {
            try {
                conf=ClassConfigurator.getInstance(false);
            }
            catch(ChannelException e) {
                log.error(ExternalStrings.MagicObjectOutputStream_CLASSCONFIGURATOR_COULD_NOT_BE_INSTANTIATED, e);
            }
        }
    }

    @Override
    protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
        int magic_num;
        if(conf == null) {
            super.writeInt(-1);
            super.writeClassDescriptor(desc);
            return;
        }
        magic_num=conf.getMagicNumberFromObjectStreamClass(desc);
        super.writeInt(magic_num);
        if(magic_num == -1) {
            if(log.isTraceEnabled()) // todo: remove
                log.trace("could not find magic number for '" + desc.getName() + "': writing full class descriptor");
            super.writeClassDescriptor(desc);
        }
        else {
            //if(log.isTraceEnabled())
               // log.trace("writing descriptor (num=" + magic_num + "): " + desc.getName());
        }
    }

}

