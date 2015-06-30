/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.dialogs.TrayDialog;
import org.eclipse.jface.window.IShellProvider;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.HelpEvent;
import org.eclipse.swt.events.HelpListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite.Data;

/**
 * @author mghosh
 * 
 */
public class SpecifySecuritiesDlg extends TrayDialog {

  private final static String   DIALOG_TITLE  = "Specify Security Properties";
  private SecurityPropComposite securityComp_ = null;
  private Data securityPropdata_              = null;

  /**
   * @param shell
   */
  public SpecifySecuritiesDlg(Shell shell) {
    super(shell);
  }
  
  public void  setSecurityData(Data data){
    securityPropdata_ = data;
  }

  /**
   * @param parentShell
   */
  public SpecifySecuritiesDlg(IShellProvider parentShell) {
    super(parentShell);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.jface.dialogs.Dialog#createContents(org.eclipse.swt.widgets
   * .Composite)
   */
  @Override
  protected Control createContents(Composite prnt) {
    securityComp_ = new SecurityPropComposite(prnt, SWT.NONE, securityPropdata_);
    securityComp_.getShell().setText(SpecifySecuritiesDlg.DIALOG_TITLE);

    return super.createContents(securityComp_);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.dialogs.TrayDialog#isHelpAvailable()
   */
  @Override
  public boolean isHelpAvailable() {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.jface.dialogs.TrayDialog#createHelpControl(org.eclipse.swt.
   * widgets.Composite)
   */
  // @Override
  @Override
  protected Control createHelpControl(Composite parent) {
    Control ctrlHelp = super.createHelpControl(parent);
    // getParentShell().addHelpListener(new C2DSHelpLstnr(this));
    return ctrlHelp;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.dialogs.Dialog#okPressed()
   */
  @Override
  protected void okPressed() {
    // -- Get security stuff
    this.securityComp_.populateData();

    super.okPressed();

  }

  public Data getData() {
    return this.securityComp_.getSecurityPropsData();
  }

  // -----------------------------------------------------------------
  // Listener for help control
  // -----------------------------------------------------------------
  private static class C2DSHelpLstnr implements HelpListener {
    private final SpecifySecuritiesDlg parent_;

    C2DSHelpLstnr(SpecifySecuritiesDlg dlg) {
      parent_ = dlg;
    }

    public void helpRequested(HelpEvent e) {
      parent_.showHelp();
    }
  } // C2DSHelpLstnr

  public void showHelp() {
    // TODO MGH - hook to display Help window with relevant information
  }

  public static void main(String[] args) {
    SpecifySecuritiesDlg specifySecuritiesDlg = new SpecifySecuritiesDlg(
        (Shell) null);
    specifySecuritiesDlg.open();
  }
}
