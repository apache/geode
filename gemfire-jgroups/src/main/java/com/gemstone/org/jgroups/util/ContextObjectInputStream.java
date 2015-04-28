/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.HashMap;

/**
 * ObjectInputStream which sets a contact classloader for reading bytes into objects. Copied from
 * MarshalledValueInputStream of JBoss
 * @author Bela Ban
 * @version $Id: ContextObjectInputStream.java,v 1.6 2005/08/22 06:49:04 belaban Exp $
 */
public class ContextObjectInputStream extends ObjectInputStream {


    /**
     * A class wide cache of proxy classes populated by resolveProxyClass
     */
    private static final HashMap classCache=new HashMap();

    private static final HashMap primClasses = new HashMap(9, 1.0F);
    static {
        primClasses.put("boolean", boolean.class);
        primClasses.put("byte", byte.class);
        primClasses.put("char", char.class);
        primClasses.put("short", short.class);
        primClasses.put("int", int.class);
        primClasses.put("long", long.class);
        primClasses.put("float", float.class);
        primClasses.put("double", double.class);
        primClasses.put("void", void.class);
    }

    /**
     * Creates a new instance of MarshalledValueOutputStream
     */
    public ContextObjectInputStream(InputStream is) throws IOException {
        super(is);
    }


    @Override // GemStoneAddition
    protected Class resolveClass(ObjectStreamClass v) throws IOException, ClassNotFoundException {
        String className=v.getName();
        Class resolvedClass=null;
        // Check the class cache first if it exists
        if(classCache != null) {
            synchronized(classCache) {
                resolvedClass=(Class)classCache.get(className);
            }
        }

        if(resolvedClass == null) {
            try {
                resolvedClass=Util.loadClass(className, this.getClass());
            }
            catch(ClassNotFoundException e) {
                
            }
            if(resolvedClass == null) {
                /* This is a backport von JDK 1.4's java.io.ObjectInputstream to support
                * retrieval of primitive classes (e.g. Boolean.TYPE) in JDK 1.3.
                * This is required for org.jgroups.blocks.MethodCall to support primitive
                * Argument types in JDK1.3:
                */
                resolvedClass = (Class) primClasses.get(className);
                if (resolvedClass == null) {

                    /* Use the super.resolveClass() call which will resolve array
                       classes and primitives. We do not use this by default as this can
                       result in caching of stale values across redeployments.
                    */
                    resolvedClass=super.resolveClass(v);
                }
            }
            if(classCache != null) {
                synchronized(classCache) {
                    classCache.put(className, resolvedClass);
                }
            }
        }
        return resolvedClass;
    }
}

