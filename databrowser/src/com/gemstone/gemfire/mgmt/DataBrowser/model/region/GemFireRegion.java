/*=========================================================================
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.model.region;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a GemFire region.
 *
 * @author Hrishi
 **/
public final class GemFireRegion {

  private static final String ROOT_ELEM = "/Root";
  private String              name;
  private String              fullPath;
  private String              dataPolicy;
  private String              scope;
  private List<GemFireRegion> subRegions = new ArrayList<GemFireRegion>();
  private String              memberId;

  GemFireRegion() {
    // Do nothing.
  }

  void addSubRegion(GemFireRegion reg) {
    this.subRegions.add(reg);
  }

  void resetSubRegionInfo() {

    for (GemFireRegion reg : this.subRegions) {
      reg.resetSubRegionInfo();
    }
    this.subRegions.clear();
  }

  void setName(String nm) {
    this.name = nm;
  }

  void setFullPath(String FQN) {
    this.fullPath = FQN;
  }

  void setDataPolicy(String dataPol) {
    this.dataPolicy = dataPol;
  }

  void setScope(String s) {
    this.scope = s;
  }

  void setMemberId(String mbrID) {
    this.memberId = mbrID;
  }

  /**
   * This method returns name of this region.
   */
  public String getName() {
    return this.name;
  }

  /**
   * This method returns path of this region.
   */
  public String getFullPath() {
    if(this.fullPath != null) {
      String result = this.fullPath;
      int length = result.length();
      if(this.fullPath.startsWith(ROOT_ELEM)) {
       return result.substring(ROOT_ELEM.length(), length-1 );
      }
    }

    return this.fullPath;
  }

  /**
   * This method returns data-policy for this region.
   **/
  public String getDataPolicy() {
    return this.dataPolicy;
  }

  /**
   * This method returns scope for this region.
   **/
  public String getScope() {
    return this.scope;
  }

  /**
   * This method returns a list of subregions.
   * */
  public GemFireRegion[] getSubRegions() {
    return this.subRegions.toArray(new GemFireRegion[0]);
  }

  /**
   * This method returns a list of subregions.
   * */
  public List<GemFireRegion> getSubRegions(boolean recursive) {
    List<GemFireRegion> allRegions = subRegions;
    if(recursive){
      for (int i = 0; i < subRegions.size(); i++) {
        allRegions.addAll(subRegions.get(i).getSubRegions(true));
      }
    }

    return allRegions;
  }

  public String getMemberId() {
    return memberId;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("GemFireRegion [");
    buffer.append("\n name :" + this.name);
    buffer.append("\n full-path :" + this.getFullPath());
    buffer.append("\n scope :" + this.scope);
    buffer.append("\n data-policy :" + this.dataPolicy);
    for (GemFireRegion reg : this.subRegions) {
      buffer.append("\n sub-regions :" + reg);
    }
    buffer.append("\n]");
    return buffer.toString();
  }

}
