/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.action.ToolBarManager;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ToolBar;

import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteCQuery;

/**
 * @author mjha
 * 
 */
public class CQWindowToolBarManager extends ToolBarManager {

  // MGH - If the order of the actions here is changed, change the indices for
  // the connect and disconnect items
  // in inner class TBMgr_CnxnMsgHndlr
  private final AbstractDataBrowserAction[][] actionsTypes_ = new AbstractDataBrowserAction[1][1];

  /**
	 * 
	 */
  @SuppressWarnings("unused")
  private CQWindowToolBarManager() {
    // TODO Auto-generated constructor stub
  }

  /**
	 * 
	 */
  public CQWindowToolBarManager(int iOrdinal) {
    // TODO Auto-generated constructor stub
    init(iOrdinal);
  }

  /**
   * @param style
   */
  public CQWindowToolBarManager(CqAppWindow win, int style, int iOrdinal) {
    super(style);
    actionsTypes_[0][0] = new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteCQuery(win);
    win.setExecuteCQueryAction((ExecuteCQuery)actionsTypes_[0][0]);
    init(iOrdinal);
  }

  /**
   * @param toolbar
   */
  public CQWindowToolBarManager(ToolBar toolbar, int iOrdinal) {
    super(toolbar);
    init(iOrdinal);
    // TODO Auto-generated constructor stub
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.jface.action.ToolBarManager#createControl(org.eclipse.swt.widgets
   * .Composite)
   */
  @Override
  public ToolBar createControl(Composite parent) {
    
    return super.createControl(parent);
  }

  private void init(int iOrdinal) {
    if (iOrdinal < actionsTypes_.length) {
      int iNumActions = actionsTypes_[iOrdinal].length;
      for (int j = 0; j < iNumActions; j++) {
        this.add(actionsTypes_[iOrdinal][j]);
      }
    }
  }
  
}
