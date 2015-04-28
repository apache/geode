/**
 * 
 */
package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;

/**
 * FRAG3 is a version of FRAG2 that is used below GMS to allow for larger
 * membership view messages than will fit in a single packet.
 * 
 * @author bruces
 *
 */
public class FRAG3 extends FRAG2 {

  @Override // GemStoneAddition  
  public String getName() {
      return "FRAG3";
  }

  @Override
  public void down(Event evt) {
    if (evt.getType() == Event.MSG) {
      Message msg=(Message)evt.getArg();
      if (msg.getHeader("FRAG2") != null) {
        // the message is already fragmented - don't mess with it
        passDown(evt);
        return;
      }
    }
    super.down(evt);
  }
}
