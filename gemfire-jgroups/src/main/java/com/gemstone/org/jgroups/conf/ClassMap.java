/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ClassMap.java,v 1.4 2005/08/08 14:58:32 belaban Exp $

package com.gemstone.org.jgroups.conf;

import com.gemstone.org.jgroups.util.Util;


/**
 * Maintains mapping between magic number and class
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
public class ClassMap {
    private final String  mClassname;
    private final String  mDescription;
    private final boolean mPreload;
    private final int     mMagicNumber;

    public ClassMap(String clazz,
                    String desc,
                    boolean preload,
                    int magicnumber) {
        mClassname=clazz;
        mDescription=desc;
        mPreload=preload;
        mMagicNumber=magicnumber;
    }

    @Override // GemStoneAddition
    public int hashCode() {
        return getMagicNumber();
    }

    public String getClassName() {
        return mClassname;
    }

    public String getDescription() {
        return mDescription;
    }

    public boolean getPreload() {
        return mPreload;
    }

    public int getMagicNumber() {
        return mMagicNumber;
    }


    /**
     * Returns the Class object for this class<BR>
     */
    public Class getClassForMap() throws ClassNotFoundException {
        return Util.loadClass(getClassName(), this.getClass());
    }


    @Override // GemStoneAddition
    public boolean equals(Object another) {
        if(another instanceof ClassMap) {
            ClassMap obj=(ClassMap)another;
            return getClassName().equals(obj.getClassName());
        }
        else
            return false;
    }


}
