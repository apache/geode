/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.app;

/**
 * @author mghosh
 *
 */
public final class VersionInfo {

  private static String RESOURCE_NAME = null; // "DataBrowser.properties";
  private static boolean fReadFromResource_ = false;
  
  private int              iMajor_;
  private int              iMinor_;
  private int              iDotRel_;
  private int              iPatchNum_;

  static final VersionInfo defVI_      = new VersionInfo();
  static VersionInfo       viInstance_ = new VersionInfo();

  /*
   * Default (sane but nonsensical) value
   * for later version, this information should be in a resource file 
   */
  static {
    VersionInfo.defVI_.iPatchNum_ = 0;
    VersionInfo.defVI_.iDotRel_ = 0;
    VersionInfo.defVI_.iMinor_ = 0;
    VersionInfo.defVI_.iMajor_ = 1;

  }

  /**
   * Empty ctor
   */
  private VersionInfo() {
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + iDotRel_;
    result = prime * result + iMajor_;
    result = prime * result + iMinor_;
    result = prime * result + iPatchNum_;
    return result;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof VersionInfo))
      return false;
    VersionInfo other = (VersionInfo) obj;
    if (iDotRel_ != other.iDotRel_)
      return false;
    if (iMajor_ != other.iMajor_)
      return false;
    if (iMinor_ != other.iMinor_)
      return false;
    if (iPatchNum_ != other.iPatchNum_)
      return false;
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    VersionInfo vi = getInstance();
    return vi.iMajor_ + "." + vi.iMinor_;
  }

  static public VersionInfo getInstance() {
    VersionInfo viRet = ( false == fReadFromResource_ ) ? VersionInfo.defVI_ : VersionInfo.viInstance_; 
    return viRet;
  }

}
