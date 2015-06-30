/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;

/**
 * @author mghosh
 * 
 */
public class TracePanel extends Composite {

  /**
   * @param parent
   * @param style
   */
  public TracePanel(Composite parent, int style) {
    super(parent, style);
    setBackground(getDisplay().getSystemColor(SWT.COLOR_YELLOW));
  }

}
