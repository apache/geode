/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;


//import org.eclipse.jface.action.ActionContributionItem;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.CoolBar;

/**
 * @author mghosh
 * 
 */
public class CQWindowCoolBarManager extends MainWindowCoolBarManager {

  private CqAppWindow parentWin_;

  /**
	 * 
	 */
  public CQWindowCoolBarManager() {
    init();
  }

  /**
   * @param coolBar
   */
  public CQWindowCoolBarManager(CoolBar coolBar) {
    super(coolBar);
  }

  /**
   * @param style
   */
  public CQWindowCoolBarManager(CqAppWindow win, int style) {
    super(style);
    parentWin_= win;
    init();
  }

  @Override
  protected void init() {
    if (parentWin_ != null) {
      CQWindowToolBarManager tbm = new CQWindowToolBarManager(parentWin_,
          SWT.FLAT, 0);
      this.add(tbm);
    }
  }


}
