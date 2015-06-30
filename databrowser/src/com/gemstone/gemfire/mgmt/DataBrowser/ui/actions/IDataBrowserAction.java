/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import org.eclipse.swt.events.SelectionListener;

/**
 * @author mghosh
 * 
 */
public interface IDataBrowserAction {
  String label();

  SelectionListener getSelectionListener();
}
