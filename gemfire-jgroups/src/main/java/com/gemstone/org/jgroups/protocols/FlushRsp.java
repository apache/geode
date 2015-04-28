/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: FlushRsp.java,v 1.1.1.1 2003/09/09 01:24:10 belaban Exp $

package com.gemstone.org.jgroups.protocols;

import java.util.Vector;




public class FlushRsp {
    public boolean  result=true;
    public Vector   unstable_msgs=new Vector();
    public Vector   failed_mbrs=null;           // when result is false

    public FlushRsp() {}

    public FlushRsp(boolean result, Vector unstable_msgs, Vector failed_mbrs) {
	this.result=result;
	this.unstable_msgs=unstable_msgs;
	this.failed_mbrs=failed_mbrs;
    }

    @Override // GemStoneAddition
    public String toString() {
	return "result=" + result + "\nunstable_msgs=" + unstable_msgs + "\nfailed_mbrs=" + failed_mbrs;
    }
}
