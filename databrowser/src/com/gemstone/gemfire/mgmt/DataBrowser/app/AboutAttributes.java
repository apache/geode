/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.app;

import java.util.Map;
import java.util.HashMap;

/**
 * @author mghosh
 *
 */
public final class AboutAttributes {

  private Map<String, String>  attr_;

  static final AboutAttributes defAttr_;
  static AboutAttributes       instAboutAttr_;

  static {
    defAttr_ = new AboutAttributes();
    AboutAttributes.defAttr_.attr_.put("Copyright", "Copyright_val");
    AboutAttributes.defAttr_.attr_.put("Name", "Name_val");
    AboutAttributes.instAboutAttr_ = new AboutAttributes();

    // TODO Can we get a file name from the enivronment and read the values in
    // it? - something akin to metaprogramming
  }

  /**
	 *
	 */
  private AboutAttributes() {
    attr_ = new HashMap<String, String>();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    // if attr_ is not empty, then is 'stringizes' that map, otherwise takes the
    // default map.
    Map<String, String> mout = (true == attr_.isEmpty()) ? AboutAttributes.defAttr_.attr_
        : attr_;
    return generateString(mout);
  }

  private String generateString(Map<String, String> mapAttr) {

    StringBuffer sOutBuf = new StringBuffer();
    for (Map.Entry<String, String> e : mapAttr.entrySet()) {
      sOutBuf.append(e.getKey() + "=" + e.getValue() + "\n");
    }

    return sOutBuf.toString();
  }

  static public AboutAttributes getInstance() {
    return AboutAttributes.instAboutAttr_;
  }

}
