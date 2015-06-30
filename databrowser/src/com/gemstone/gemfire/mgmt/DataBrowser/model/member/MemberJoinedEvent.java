package com.gemstone.gemfire.mgmt.DataBrowser.model.member;

import com.gemstone.gemfire.mgmt.DataBrowser.model.IMemberEvent;

/**
 * Implementation of {@link IMemberEvent}, representing event for member joined
 * 
 * @author mghosh
 */
public class MemberJoinedEvent implements IMemberEvent {

  private final GemFireMember[] members_;

  /**
   * 
   */
  public MemberJoinedEvent(GemFireMember[] mbrs) {
    if((mbrs != null)) {
      members_ = new GemFireMember[mbrs.length];
      System.arraycopy(mbrs, 0, members_, 0, members_.length);
     } else {
       members_ = new GemFireMember[0]; 
     }
  }

  public MemberJoinedEvent(GemFireMember mbr) {
    members_ = new GemFireMember[] { mbr };
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.mgmt.DataBrowser.model.IMemberEvent#getMember()
   */
  public final GemFireMember getMember() {
    return members_[0];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.mgmt.DataBrowser.model.IMemberEvent#getMembers()
   */
  public GemFireMember[] getMembers() {
    GemFireMember[] temp = new GemFireMember[members_.length];
    System.arraycopy(members_, 0, temp, 0, members_.length);
    return temp;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("member joined event for: ");
    for (int i = 0; i < members_.length; i++) {
      builder.append("\n" + members_[i].getName());
    }
    return builder.toString();
  }

}
