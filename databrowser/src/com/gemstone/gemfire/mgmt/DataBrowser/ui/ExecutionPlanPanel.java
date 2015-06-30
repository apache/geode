/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.HelpListener;
import org.eclipse.swt.widgets.Composite;

/**
 * @author mghosh
 * 
 */
public class ExecutionPlanPanel extends Composite {

  /**
   * @param parent
   * @param style
   */
  public ExecutionPlanPanel(Composite parent, int style) {
    super(parent, style);
    setBackground(getDisplay().getSystemColor(SWT.COLOR_BLUE));
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.swt.widgets.Control#addHelpListener(org.eclipse.swt.events.
   * HelpListener)
   */
  @Override
  public void addHelpListener(HelpListener listener) {
    // TODO Auto-generated method stub
    super.addHelpListener(listener);
  }

}
