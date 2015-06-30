/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.util.ArrayList;
import java.util.HashMap;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Composite;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ICQEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteCQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteQuery;

/**
 * @author mjha
 *
 */
public class CQueryView extends QueryView {
  // -- this needs to be done in a cleaner fashion
  static protected final int        IDX_PANEL_RESULTS = 0;
  static protected final int        IDX_PANEL_MESSAGE = 1;

  static final String               sTabIconPaths[]   = {
      "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/Results.ico",
      "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/Messages.ico",
                                                      };

  static final String[]             sTabLabels        = { "Results", "Messages" };

  private CqAppWindow               cqWindow_;
  private Composite[]               tabPanels_;
  private CTabFolder                taFolder_;
  private CQQuery                   cQuery_;
  private CqExec_CqEvtHndlr         cqEventHandler;
  private CqExec_SetCqQueryEvtHndlr setCqQueryHandler;
  private ExecuteCQuery             executeCQuery_;
  private GetConnect_MsgHndlr       hndlrConnectMsgs;
  private GetDisconnect_MsgHndlr    hndlrDisconnectMsgs;

  /**
   * @param parent
   * @param style
   */
  public CQueryView(CqAppWindow cqwin, Composite parent, int style) {
    super(parent, style);
    cqWindow_ = cqwin;
    cqEventHandler = new CqExec_CqEvtHndlr(this);
    setCqQueryHandler = new CqExec_SetCqQueryEvtHndlr(this);
    hndlrConnectMsgs     = new GetConnect_MsgHndlr(this);
    hndlrDisconnectMsgs  = new GetDisconnect_MsgHndlr(this);

    addShortCutForExecuteQuery();
    registerForMessages();
  }

  @Override
  protected void registerForMessages() {
    if (cqWindow_ != null) {
      cqWindow_.addCustomMessageListener(
          CustomUIMessages.QRY_MSG_GET_CQ_QUERY_STR_FOR_EXEC,
          hndlrGetQueryStrMsgs);
      cqWindow_.addCustomMessageListener(
          CustomUIMessages.QRY_MSG_SET_CQ_QUERY_FOR_DISP, setCqQueryHandler);
      cqWindow_.addCustomMessageListener(
          CustomUIMessages.QRY_MSG_PROCESS_CQ_QUERY_EVT, cqEventHandler);
      cqWindow_.addCustomMessageListener(CustomUIMessages.DS_CONNECTED,
          hndlrConnectMsgs);
      cqWindow_.addCustomMessageListener(CustomUIMessages.DS_DISCONNECTED,
          hndlrDisconnectMsgs);
    }
  }
  @Override
  protected ExecuteQuery getExecuteQueryAction(){  
    return cqWindow_.getExecuteCQueryAction();
  }

  @Override
  protected void addTabs(CTabFolder folder) {
    taFolder_ = folder;
    tabPanels_ = new Composite[1];
    tabPanels_[0] = new CQResultsPanel(folder, SWT.NULL);
    // tabPanels_[1] = new MessagePanel(folder, SWT.NULL);

    final HashMap<String, Image> imgs = getTabItemImages();
    for (int i = 0; i < tabPanels_.length; i++) {
      CTabItem ti = new CTabItem(folder, SWT.NONE, i);
      ti.setText(CQueryView.sTabLabels[i]);
      ti.setControl(tabPanels_[i]);
      ti.setImage(imgs.get(CQueryView.sTabLabels[i]));
    }
    taFolder_.setSelection(CQueryView.IDX_PANEL_RESULTS);
  }

  private CQResultsPanel getCQPanel() {
    return (CQResultsPanel) tabPanels_[0];
  }

  private MessagePanel getMsgPanel() {
    return (MessagePanel) tabPanels_[1];
  }

  public void setQuery(CQQuery query) {
    cQuery_ = query;
    getCQPanel().setQuery(cQuery_);
  }

  public void close() {
    // TODO Auto-generated method stub

  }

  private void onEvent(final ICQEvent cqEvent) {
    getCQPanel().processCqEvent(cqEvent);
  }

  private void onNewTypeAdded(final IntrospectionResult result) {
    getCQPanel().processNewTypeEvent(result);
  }

  static private class CqExec_CqEvtHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {
    final CQueryView parent_;

    CqExec_CqEvtHndlr(CQueryView qv) {
      parent_ = qv;
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
        for (int i = 0; i < params.size(); i++) {
          Object oParam = params.get(i);
          if(oParam instanceof IntrospectionResult)
            parent_.onNewTypeAdded((IntrospectionResult) oParam);
          else if(oParam instanceof ICQEvent)
            parent_.onEvent((ICQEvent) oParam);
        }

      } // params.isEmpty()
    }
  }

  static private class CqExec_SetCqQueryEvtHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {
    final CQueryView parent_;

    CqExec_SetCqQueryEvtHndlr(CQueryView qv) {
      parent_ = qv;
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
        // -- MGH - only one param, cq event
        Object oParam = params.get(0);
        parent_.setQuery((CQQuery) oParam);
      } // params.isEmpty()
    }
  }
  private static class GetConnect_MsgHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {
    final QueryView parent_;

    GetConnect_MsgHndlr(QueryView qv) {
      parent_ = qv;
    }

    public void handleEvent(String msg, ArrayList<Object> params,
        ArrayList<Object> results) {
      parent_.addShortCutForExecuteQuery();
    }
  }
  
  private static class GetDisconnect_MsgHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {
    final QueryView parent_;

    GetDisconnect_MsgHndlr(QueryView qv) {
      parent_ = qv;
    }

    public void handleEvent(String msg, ArrayList<Object> params,
        ArrayList<Object> results) {
      parent_.removeShortCutForExecuteQuery();
    }
}
}
