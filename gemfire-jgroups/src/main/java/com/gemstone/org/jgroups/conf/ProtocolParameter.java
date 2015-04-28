/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ProtocolParameter.java,v 1.4 2004/09/23 16:29:14 belaban Exp $

package com.gemstone.org.jgroups.conf;

/**
 * Data holder for protocol data
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */

public class ProtocolParameter {

    private final String mParameterName;
    private final Object mParameterValue;

    public ProtocolParameter(String parameterName,
                             Object parameterValue) {
        mParameterName=parameterName;
        mParameterValue=parameterValue;
    }

    public String getName() {
        return mParameterName;
    }

    public Object getValue() {
        return mParameterValue;
    }

    @Override // GemStoneAddition
    public int hashCode() {
        if(mParameterName != null)
            return mParameterName.hashCode();
        else
            return -1;
    }

    @Override // GemStoneAddition
    public boolean equals(Object another) {
        if(another instanceof ProtocolParameter)
            return getName().equals(((ProtocolParameter)another).getName());
        else
            return false;
    }

    public String getParameterString() {
        StringBuffer buf=new StringBuffer(mParameterName);
        if(mParameterValue != null)
            buf.append('=').append(mParameterValue.toString());
        return buf.toString();
    }

    public String getParameterStringXml() {
        StringBuffer buf=new StringBuffer(mParameterName);
        if(mParameterValue != null)
            buf.append("=\"").append(mParameterValue.toString()).append('\"');
        return buf.toString();
    }
}
