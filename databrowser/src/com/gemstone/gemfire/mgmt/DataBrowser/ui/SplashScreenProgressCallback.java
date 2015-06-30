/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

/**
 * @author mghosh
 *
 */
public interface SplashScreenProgressCallback {

  static public final class ActionCode {
    @SuppressWarnings("unused")
    private int                    iVal_       = -1;
    private String                 sLabel_     = "";

    public static final ActionCode CANCEL      = new ActionCode( 0, "CANCEL" );
    public static final ActionCode CONTINUE    = new ActionCode( 1, "CONTINUE" );
    public static final ActionCode END_SUCCESS = new ActionCode( 2, "END_SUCCESS" );
    public static final ActionCode END_ERROR   = new ActionCode( 3, "END_ERROR" );

    private ActionCode(int iV, String lbl) {
      iVal_ = iV;
    }

    @Override
    public String toString() {
      return sLabel_;
    }
  }

  // MGH Hack with '1 element' array to circumvent inability to have 'out'
  // params
  ActionCode handleProgress(Integer[] iPercentComplete);

}
