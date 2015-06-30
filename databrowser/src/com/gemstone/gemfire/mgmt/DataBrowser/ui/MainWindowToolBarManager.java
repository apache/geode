/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.util.ArrayList;

import org.eclipse.jface.action.IAction;
import org.eclipse.jface.action.IContributionItem;
import org.eclipse.jface.action.ToolBarManager;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ToolBar;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DSSnapShot;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomMsgDispatcher.ICustomMessageListener;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction;

/**
 * @author mghosh
 * 
 */
public class MainWindowToolBarManager extends ToolBarManager {

  // MGH - If the order of the actions here is changed, change the indices for
  // the connect and disconnect items
  // in inner class TBMgr_CnxnMsgHndlr
  private static final AbstractDataBrowserAction[][] actionsTypes_ = {
      {
      // -- File menu actions
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ConnectToDS(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.DisconnectFromDS(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.SpecifySecurity(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.Exit() },

      // -- Query menu
      { new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteQuery(), 
        new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExportQueryResults(),},

      // -- Help menu
      { new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.HelpContents(),
      new com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AboutDataBrowser(), }, };


  private final static IAction          actn_Connect = actionsTypes_[0][0];
  private final static IAction          actn_Disconnect = actionsTypes_[0][1];
  private final static IAction          actn_SpecifySecurity = actionsTypes_[0][2];
  private final static IAction          actn_ExportResults = actionsTypes_[1][1];
  private final static IAction          actn_ExecuteQuery = actionsTypes_[1][0];

  /**
	 * 
	 */
  @SuppressWarnings("unused")
  private MainWindowToolBarManager() {
    // TODO Auto-generated constructor stub
  }

  /**
	 * 
	 */
  public MainWindowToolBarManager(int iOrdinal) {
    // TODO Auto-generated constructor stub
    init(iOrdinal);
  }

  /**
   * @param style
   */
  public MainWindowToolBarManager(int style, int iOrdinal) {
    super(style);
    init(iOrdinal);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param toolbar
   */
  public MainWindowToolBarManager(ToolBar toolbar, int iOrdinal) {
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
    if( null != find( MainWindowToolBarManager.actn_Connect.getId() )) {
      this.registerForDSConnectMsg();
    }
    if( null != find( MainWindowToolBarManager.actn_Disconnect.getId() )) {
      this.registerForDSDisconnectMsg();
    }
    if( null != find( MainWindowToolBarManager.actn_ExportResults.getId() )) {
      this.registerForQueryExecution();
    }
    
    if( null != find( MainWindowToolBarManager.actn_ExecuteQuery.getId() )) {
      this.registerForDSConnectMsg();
      this.registerForDSDisconnectMsg();
    }

    
    return super.createControl(parent);
  }

  private void init(int iOrdinal) {
    if (iOrdinal < MainWindowToolBarManager.actionsTypes_.length) {
      int iNumActions = MainWindowToolBarManager.actionsTypes_[iOrdinal].length;
      for (int j = 0; j < iNumActions; j++) {
        // this.appendToGroup( "Group_" + i, this.actionsTypes_[i][j] );
        this.add(MainWindowToolBarManager.actionsTypes_[iOrdinal][j]);
      }
    }
  }

  // TODO MGH - not sure whether this should go here or in the main window class
  private boolean registerForDSConnectMsg() {
    return registerForMessage( CustomUIMessages.DS_CONNECTED, hndlrCnxnMsgs );
  }
  
  private boolean registerForDSDisconnectMsg() {
    return registerForMessage( CustomUIMessages.DS_DISCONNECTED, hndlrCnxnMsgs );
  }
  
  private boolean registerForQueryExecution() {
    return registerForMessage( CustomUIMessages.QRY_MSG_ADD_QUERY_SINGLE_RESULT, hndlrCnxnMsgs );
  }
  
  private boolean registerForMessage( String msg, ICustomMessageListener hndlr ) {
    final DataBrowserApp app = DataBrowserApp.getInstance();
    boolean fRegistered = false;
    if (null != app) {
      final MainAppWindow wnd = app.getMainWindow();

      if (null != wnd) {
        // TODO MGH - Perhaps we should log failure to register and continue!
        fRegistered = wnd.addCustomMessageListener(
            msg, hndlr);
      }
    }
    
    return fRegistered;
  }

  private TBMgr_CnxnMsgHndlr hndlrCnxnMsgs = new TBMgr_CnxnMsgHndlr(this);

  static private class TBMgr_CnxnMsgHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {
    final MainWindowToolBarManager parent_;

    TBMgr_CnxnMsgHndlr(MainWindowToolBarManager prnt) {
      parent_ = prnt;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.gemstone.gemfire.mgmt.DataBrowser.ui.CustomMsgDispatcher.
     * ICustomMessageListener#handleEvent(java.lang.String, java.util.ArrayList,
     * java.util.ArrayList)
     */
    public void handleEvent(String msg, ArrayList<Object> params,
        ArrayList<Object> results) {
      if (false == params.isEmpty()) {
        // -- MGH - only one param, a DSSSnapShot
        Object oParam = params.get(0);

        if (oParam instanceof DSSnapShot) {
          if (( true == CustomUIMessages.DS_CONNECTED.equals(msg)) || (CustomUIMessages.DS_DISCONNECTED.equals(msg))) {
            IContributionItem itm = parent_.find(actn_Connect.getId());
            if( null != itm ) {
              itm.update(IAction.ENABLED);
            }
            
            itm = parent_.find(actn_Disconnect.getId());
            if( null != itm ) {
              itm.update(IAction.ENABLED);
            }
            
            itm = parent_.find(actn_SpecifySecurity.getId());
            if( null != itm ) {
              itm.update(IAction.ENABLED);
            }
            
            itm = parent_.find(actn_ExportResults.getId());
            if( null != itm ) {
              itm.update(IAction.ENABLED);
            }
            
            itm = parent_.find(actn_ExecuteQuery.getId());
            if( null != itm ) {
              itm.update(IAction.ENABLED);
            }
          }
        }
        if (oParam instanceof QueryResult) {
          IContributionItem itm = parent_.find(actn_ExportResults.getId());
          if( null != itm ) {
            itm.update(IAction.ENABLED);
          }
        }
      }
    }


  } // class TBMgr_CnxnMsgHndlr

}
