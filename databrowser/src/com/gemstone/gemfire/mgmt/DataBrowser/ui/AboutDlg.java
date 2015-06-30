/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.dialogs.TitleAreaDialog;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Shell;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.swt.widgets.Button;


/**
 * @author mghosh
 *
 */
public class AboutDlg extends TitleAreaDialog {
  private final String dialogText;
  /**
   * @param parentShell
   */
  public AboutDlg(Shell parentShell, String text) {
    super(parentShell);
    dialogText = text; 
    create();
  }

  @Override
  public void create() {
    super.create();
    getShell().setText("About GemFire DataBrowser"); 
  }

   /* (non-Javadoc)
   * @see org.eclipse.jface.dialogs.Dialog#createDialogArea(org.eclipse.swt.widgets.Composite)
   */
  @Override
  protected Control createDialogArea(Composite parent) {
    // MGH: Guaranteed by javadocs that the returned Control is a Composite
    Composite ctrl = ( Composite )super.createDialogArea(parent);
    Text text = new Text(ctrl, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    
    GridData data = new GridData(SWT.FILL, SWT.FILL, true, true);
    data.heightHint = 250;
    data.widthHint = 450;    
    text.setLayoutData(data);   
    
    text.setText(dialogText);
    return ctrl; 
  }
 
  /*
   * Add buttons to the dialog's button bar.
   *
   * @param parent
   *            the button bar composite
   */
  protected void createButtonsForButtonBar(Composite parent) {
     Button b = createButton(parent, IDialogConstants.OK_ID,
             IDialogConstants.OK_LABEL, true);
     b.setFocus();
  } 

}
