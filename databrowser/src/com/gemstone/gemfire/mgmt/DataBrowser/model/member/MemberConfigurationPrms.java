/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.model.member;

/**
 * @author hgadre
 *
 */
public class MemberConfigurationPrms {
  private String name;
  private Object value;
  
  public MemberConfigurationPrms() {
   name = null;
   value = null;
  }

  public MemberConfigurationPrms(String nm, Object val) {
    super();
    name = nm;
    value = val;
  }

  public String getName() {
    return name;
  }

  public void setName(String nm) {
    name = nm;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object val) {
    value = val;
  }
  
  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("MemberConfigurationPrms[");
    buffer.append("  name :"+name);
    buffer.append(", value :"+value);
    buffer.append("]");
    return buffer.toString();
  }
}
