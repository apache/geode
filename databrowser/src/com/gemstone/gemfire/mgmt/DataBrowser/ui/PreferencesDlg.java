/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.preference.PreferenceDialog;
import org.eclipse.jface.preference.PreferenceManager;
import org.eclipse.swt.widgets.Shell;

/**
 * @author mghosh
 *
 */
public class PreferencesDlg extends PreferenceDialog {

  /**
   * @param parentShell
   * @param manager
   */
  public PreferencesDlg(Shell parentShell, PreferenceManager manager) {
    super(parentShell, manager);
    // TODO Auto-generated constructor stub
  }

  
  
  /* (non-Javadoc)
   * @see org.eclipse.jface.dialogs.TrayDialog#isHelpAvailable()
   */
  @Override
  public boolean isHelpAvailable() {
    return false;
  }



  /**
   * @param args
   * 
   * Test hook
   */
  static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
