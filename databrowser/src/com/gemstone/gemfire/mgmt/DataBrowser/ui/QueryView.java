/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomMsgDispatcher.ICustomMessageListener;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 *
 */
public class QueryView extends Composite {

  // -- this needs to be done in a cleaner fashion
  static private HashMap<String, Image> imgsTabItem_         = null;
  static private final int              IDX_PANEL_RESULTS    = 0;
  static private final int              IDX_PANEL_STATISTICS = 1;
  static private final int              IDX_PANEL_MESSAGE    = 3;
  static private final int              IDX_PANEL_TRACE      = 2;
  static private final int              IDX_PANEL_EXECPLAN   = 4;

  static final String                   sTabIconPaths[]      = {
    "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/Results.ico",
    "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/Messages.ico",
    "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/Trace.ico",
    "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/Statistics.ico",
    "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/ExecutionPlanTab.ico",
  };
//  static final String[]                 sTabLabels           = {
//      "Results", "Messages", "Trace", "Statistics", "Execution Plan" };

  static final String[]                 sTabLabels           = { "Results"};

  private Composite[]                   tabPanels_;
  /*
   * { new ResultsPanel(), new MessagePanel(), new TracePanel(), new
   * StatisticsPanel(), new ExecutionPlanPanel() }
   */

  private SashForm                      sashForm_            = null;
  private Text                          txtQueryEntry_       = null;
  private Composite                     resultsPane_         = null;
  private CTabFolder                    resultsTabFolder_    = null;
  private ExecuteQuery executeQuery_;

//  private StringBuffer                  strBufQuery_         = new StringBuffer();
//  private StringBuffer                  strBufMessages_      = new StringBuffer();

  synchronized private void initResources(Display dsply) {
    if (null == QueryView.imgsTabItem_) {
      QueryView.imgsTabItem_ = new HashMap<String, Image>();
      for (int i = 0; i < QueryView.sTabLabels.length; i++) {
        Image img = null;
        InputStream isImage = null;
        try {
          isImage = QueryView.class.getResourceAsStream(QueryView.sTabIconPaths[i]);

          if (null != isImage) {
            img = new Image(dsply, isImage);
          }
        }
        catch (SWTException xptn) {
          // MGH - we simply show no icon
          // handler for org.eclipse.swt.graphics.Image ctor
          // we continue an try to add the other nodes
          LogUtil.error( "Unable to create icon for tab " + sTabLabels[i] + ". No icon will be rendered." , xptn );
          img = null;
        }
        catch (SWTError err) {
          // MGH - we simply show no icon
          LogUtil.error( "Unable to create icon for tab " + sTabLabels[i] + ". No icon will be rendered." , err );
          img = null;
        }
        finally {
          if( null != isImage ) {
            try {
              isImage.close();
            } catch (IOException xptn) {
              LogUtil.error( "Error closing image resource input stream when creating icon for " + sTabLabels[i] + ". Ignoring error." , xptn );
            }
          }
        }

        if( null != img ) {
          QueryView.imgsTabItem_.put(QueryView.sTabLabels[i], img);
        }
      }  // for (int i = 0; i < QueryView.sTabLabels.length; i++)
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.swt.widgets.Widget#dispose()
   */
  @Override
  public void dispose() {
    Set<Map.Entry<String, Image>> imgs2dispose = QueryView.imgsTabItem_
        .entrySet();
    for (Map.Entry<String, Image> i : imgs2dispose) {
      Image img = i.getValue();
      img.dispose();
    }

    QueryView.imgsTabItem_.clear();
    super.dispose();
  }

  /**
   * @param parent
   * @param style
   */
  public QueryView(Composite parent, int style) {
    super(parent, style);

    initResources(parent.getShell().getDisplay());
    registerForMessages();
    FillLayout fl = new FillLayout();
    fl.type = SWT.VERTICAL;
    setLayout(fl);

    setToolTipText("Query View");
    sashForm_ = new SashForm(this, SWT.FLAT | SWT.VERTICAL);

    FillLayout lytSash = new FillLayout();
    lytSash.type = SWT.HORIZONTAL;
    sashForm_.setLayout(lytSash);

    txtQueryEntry_ = new Text(sashForm_, SWT.FLAT | SWT.MULTI | SWT.BORDER
        | SWT.V_SCROLL | SWT.H_SCROLL | SWT.VIRTUAL);
    txtQueryEntry_.setToolTipText("Enter query string");
    // this.txtQueryEntry_.setVisible( true );
    // MGH: With this set, the sash control stop working!
    // this.txtQueryEntry_.addControlListener( new ChildResizeHandler( this ));

    resultsPane_ = new Composite(sashForm_, SWT.BORDER);
    // MGH: With this set, the sash control stop working!
    // this.resultsPane_.addControlListener( new ChildResizeHandler( this ));
    resultsPane_.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    resultsPane_.setLayout(new GridLayout());

    resultsTabFolder_ = new CTabFolder(resultsPane_, SWT.FLAT);
    resultsTabFolder_.setBorderVisible(true);
    resultsTabFolder_.setLayoutData(new GridData(SWT.FILL, SWT.FILL,true, true));
    // Display display = this.resultsPane_.getShell().getDisplay();
//    resultsTabFolder_
//        .addControlListener(new ResultsTabFolderResizeHandler(this));

    resultsTabFolder_.setSimple(false);
    resultsTabFolder_.setUnselectedCloseVisible(true);
    resultsTabFolder_.setUnselectedImageVisible(true);
    // this.resultsTabFolder_.setToolTipText("Tab folder");
    // this.resultsTabFolder_.setUnselectedCloseVisible(false);
    resultsTabFolder_.setMinimizeVisible(false);
    resultsTabFolder_.setMaximizeVisible(false);

    addTabs(resultsTabFolder_);
    sashForm_.setWeights(new int[]{20,80});
    this.pack();
  }

  protected ExecuteQuery getExecuteQueryAction(){
    if(executeQuery_ == null)
       executeQuery_ = new ExecuteQuery();
    
    return executeQuery_;
  }
  
  protected void addTabs(CTabFolder folder){
    // Set up a gradient background for the selected tab
    /*
     * this.resultsTabFolder_.setSelectionBackground(new Color[] {
     * display.getSystemColor(SWT.COLOR_WIDGET_DARK_SHADOW),
     * display.getSystemColor(SWT.COLOR_WIDGET_NORMAL_SHADOW),
     * display.getSystemColor(SWT.COLOR_WIDGET_LIGHT_SHADOW)}, new int[] { 0,
     * 100});
     */
    tabPanels_ = new Composite[1];
    tabPanels_[0] =  new ResultsPanel(folder, SWT.NULL);
//    tabPanels_[1] = new MessagePanel(resultsTabFolder_, SWT.NULL);
//    tabPanels_[2] = new TracePanel(resultsTabFolder_, SWT.NULL);
//    tabPanels_[1] = new StatisticsPanel(folder, SWT.NULL);
//    tabPanels_[4] = new ExecutionPlanPanel(resultsTabFolder_, SWT.NULL);

    for (int i = 0; i < QueryView.sTabLabels.length; i++) {
      int iStyle = (i == QueryView.IDX_PANEL_RESULTS) ? SWT.NONE : SWT.CLOSE;
      iStyle |= SWT.BORDER;
      CTabItem ti = new CTabItem(folder, iStyle, i);
      ti.setText(QueryView.sTabLabels[i]);
      ti.setControl(tabPanels_[i]);
      ti.setImage(QueryView.imgsTabItem_.get(QueryView.sTabLabels[i]));
    }

    // TODO MGH: Set focus on the 'default' results tab
    folder.setSelection(QueryView.IDX_PANEL_RESULTS);
  }
  
  final protected void addShortCutForExecuteQuery(){
    txtQueryEntry_.addKeyListener(keyListener);
  }
  
  final protected void removeShortCutForExecuteQuery(){
    txtQueryEntry_.removeKeyListener(keyListener);
  }

  protected void registerForMessages() {
    final DataBrowserApp app = DataBrowserApp.getInstance();
    if (null != app) {
      final MainAppWindow wnd = app.getMainWindow();

      if (null != wnd) {
        // TODO MGH - Perhaps we should log failure to register and continue!
        wnd.addCustomMessageListener(
            CustomUIMessages.QRY_MSG_SHOW_QUERY_TRACE_PANE, hndlrCustomMsgs);
        wnd.addCustomMessageListener(
            CustomUIMessages.QRY_MSG_SHOW_QUERY_MESSAGES_PANE, hndlrCustomMsgs);
        wnd.addCustomMessageListener(
            CustomUIMessages.QRY_MSG_SHOW_QUERY_STATISTICS_PANE,
            hndlrCustomMsgs);
        wnd.addCustomMessageListener(
            CustomUIMessages.QRY_MSG_SHOW_QUERY_EXECPLAN_PANE, hndlrCustomMsgs);
        wnd.addCustomMessageListener(
            CustomUIMessages.QRY_MSG_ADD_QUERY_SINGLE_RESULT,
            hndlrCustomMsges);
        wnd.addCustomMessageListener(
            CustomUIMessages.QRY_MSG_GET_QUERY_STR_FOR_EXEC,
            hndlrGetQueryStrMsgs);
        wnd.addCustomMessageListener(CustomUIMessages.DS_CONNECTED,
            hndlrCustomMsges);
        wnd.addCustomMessageListener(CustomUIMessages.DISPOSE_QUERY_RESULTS,
            hndlrResultDisposer);
        wnd.addCustomMessageListener(CustomUIMessages.DS_DISCONNECTED,
            hndlrCustomMsges);
      }
    }
  }

  protected GetQryStr_MsgHndlr       hndlrGetQueryStrMsgs = new GetQryStr_MsgHndlr(
                                                            this);
  private GetCustom_MsgHndlr       hndlrCustomMsges     = new GetCustom_MsgHndlr(
                                                            this);
  private ShowPanel_CustomMsgHndlr   hndlrCustomMsgs      = new ShowPanel_CustomMsgHndlr(
                                                            this);
  private Result_DisposeHandlr   hndlrResultDisposer      = new Result_DisposeHandlr(
                                                            this);
  
  private KeyListener             keyListener             = new KeyListener() {
                                                                  public void keyPressed(KeyEvent e) {
                                                                      if (e.keyCode == SWT.CR || e.keyCode == SWT.KEYPAD_CR) {
                                                                        if (e.stateMask == SWT.CONTROL) {
                                                                          getExecuteQueryAction().run();
                                                                          e.doit = false;
                                                                        }
                                                                      }
                                                                  }

                                                                  public void keyReleased(KeyEvent e) {

                                                                  }

                                                              };

  private static class Result_DisposeHandlr implements ICustomMessageListener {
    QueryView view;

    public Result_DisposeHandlr(QueryView view) {
      this.view = view;
    }

    public void handleEvent(String msg, ArrayList<Object> params,
        ArrayList<Object> results) {
      if (!(view instanceof CQueryView)) {
        // Cleanup the old data and request for the GC. This is required to fix
        // BUG669.
        LogUtil.fine("Cleaning up the old Query results from the views...");
        ((ResultsPanel)view.tabPanels_[IDX_PANEL_RESULTS]).disposeOldResults();
        System.gc();
      }
    }
  }


  private static class GetQryStr_MsgHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {
    final QueryView parent_;

    GetQryStr_MsgHndlr(QueryView qv) {
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
      Object oParam = params.get(0);

      String strRet = null;
      if (oParam instanceof Integer) {
        int iWhatToReturn = ((Integer)oParam).intValue();

        switch (iWhatToReturn) {
          case 0: { // get selection
            // MGH: We are not selecting the whole line to allow the user to select a subquery in
            //          embedded in a complex query
            strRet = parent_.txtQueryEntry_.getSelectionText();
            break;
          }

          case 1: { // get last line
            String delimLine = parent_.txtQueryEntry_.getLineDelimiter();
            String tmp = parent_.txtQueryEntry_.getText().trim();

            int iStartIdx = 0;
            int iEndIdx = tmp.length();
            if (0 < tmp.length()) {
              iStartIdx = tmp.lastIndexOf(delimLine);

              if (-1 == iStartIdx) { // only a single line in the pane
                iStartIdx = 0;
              }

              strRet = tmp.substring(iStartIdx, iEndIdx);
            }

            break;
          }

          case 2: { // get complete contents of pane
            strRet = parent_.txtQueryEntry_.getText();
            break;
          }

          default: {
            break;
          }
        } // switch

        if ((null != strRet) && (0 == strRet.length())) {
          strRet = null;
        }

        results.add(0, strRet);
      }
    }
  }

  private static class GetCustom_MsgHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {
    final QueryView parent_;

    GetCustom_MsgHndlr(QueryView qv) {
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
      if (CustomUIMessages.QRY_MSG_ADD_QUERY_SINGLE_RESULT.equals(msg)
          && false == params.isEmpty()) {

        ResultsPanel rp = (ResultsPanel)parent_.tabPanels_[IDX_PANEL_RESULTS];
        if (null != rp && false == rp.isDisposed()) {
          rp.showResults(params.get(0));
        }

//        StatisticsPanel sp = (StatisticsPanel)parent_.tabPanels_[IDX_PANEL_STATISTICS];
//        if (null != sp && false == sp.isDisposed()) {
//          sp.showResults(params.get(0));
//        }

      } // QRY_MSG_ADD_QUERY_SINGLE_RESULT
      else if (CustomUIMessages.DS_CONNECTED.equals(msg)
          && false == params.isEmpty()) {

        ResultsPanel rp = (ResultsPanel)parent_.tabPanels_[IDX_PANEL_RESULTS];
        if (null != rp && false == rp.isDisposed()) {
          rp.handleEventForConnection();
        }

//        StatisticsPanel sp = (StatisticsPanel)parent_.tabPanels_[IDX_PANEL_STATISTICS];
//        if (null != sp && false == sp.isDisposed()) {
//          sp.handleEventForConnection();
//        }
        parent_.addShortCutForExecuteQuery();

      } // DS_CONNECTED
      else if (CustomUIMessages.DS_DISCONNECTED.equals(msg)) {
        parent_.removeShortCutForExecuteQuery();
      } // DS_CONNECTED
    }
  }

  static private class ShowPanel_CustomMsgHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {
    final QueryView parent_;

    ShowPanel_CustomMsgHndlr(QueryView qv) {
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
        // -- MGH - only one param, a boolean
        Object oParam = params.get(0);

        if (oParam instanceof Boolean) {
          Boolean fShow = (Boolean) oParam;
          if (CustomUIMessages.QRY_MSG_SHOW_QUERY_EXECPLAN_PANE.equals(msg)) {
            parent_.showExecPlanPanel(fShow.booleanValue());
          } else if (CustomUIMessages.QRY_MSG_SHOW_QUERY_STATISTICS_PANE
              .equals(msg)) {
            parent_.showStatisticsPanel(fShow.booleanValue());
          } else if (CustomUIMessages.QRY_MSG_SHOW_QUERY_MESSAGES_PANE
              .equals(msg)) {
            parent_.showMessagesPanel(fShow.booleanValue());
          } else if (CustomUIMessages.QRY_MSG_SHOW_QUERY_TRACE_PANE.equals(msg)) {
            parent_.showTracePanel(fShow.booleanValue());
          }
        } // instanceof Boolean
      } // params.isEmpty()
    }
  }

//  static private class ChildResizeHandler implements ControlListener {
//    QueryView parent_;
//
//    ChildResizeHandler(QueryView p) {
//      parent_ = p;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see
//     * org.eclipse.swt.events.ControlListener#controlMoved(org.eclipse.swt.events
//     * .ControlEvent)
//     */
//    public void controlMoved(ControlEvent e) {
//      // MGH: Do nothing; let the system handle it
//      // System.out.println( "ChildResizeHandler.controlMoved" );
//
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see
//     * org.eclipse.swt.events.ControlListener#controlResized(org.eclipse.swt
//     * .events.ControlEvent)
//     */
//    public void controlResized(ControlEvent e) {
//      Rectangle rcClntArea = parent_.getClientArea();
//      int clntWd = rcClntArea.width;
//      int clntHt = rcClntArea.height;
//
//      if (e.widget == parent_.txtQueryEntry_) {
//        int iHt = 80;
//        parent_.txtQueryEntry_.setBounds(0, 0, clntWd, iHt);
//      } else if (e.widget == parent_.resultsPane_) {
//        Rectangle rcList = parent_.txtQueryEntry_.getBounds();
//
//        parent_.resultsPane_.setBounds(0, rcList.height + 10, clntWd, clntHt
//            - rcList.height);
//      }
//
//    }
//
//  } // ChildResizeHandler

//  static private class ResultsTabFolderResizeHandler implements ControlListener {
//    QueryView parent_;
//
//    ResultsTabFolderResizeHandler(QueryView p) {
//      parent_ = p;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see
//     * org.eclipse.swt.events.ControlListener#controlMoved(org.eclipse.swt.events
//     * .ControlEvent)
//     */
//    public void controlMoved(ControlEvent e) {
//      // MGH: Do nothing; let the system handle it
//      // System.out.println( "ResultsTabFolderResizeHandler.controlMoved" );
//
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see
//     * org.eclipse.swt.events.ControlListener#controlResized(org.eclipse.swt
//     * .events.ControlEvent)
//     */
//    public void controlResized(ControlEvent e) {
//      Rectangle rcClntArea = parent_.getClientArea();
//      int clntWd = rcClntArea.width;
//      int clntHt = rcClntArea.height;
//
//      if (e.widget == parent_.resultsTabFolder_) {
//        // System.out.println(
//        // "ResultsTabFolderResizeHandler.controlResized for txtQueryEntry_" );
//        parent_.resultsTabFolder_.setBounds(0, 0, clntWd, clntHt);
//      }
//    }
//  } // ResultsTabFolderResizeHandler

  public void showExecPlanPanel(boolean fShow) {
    this.tabPanels_[QueryView.IDX_PANEL_EXECPLAN].setVisible(fShow);
  }

  public void showStatisticsPanel(boolean fShow) {
    CTabItem ti = resultsTabFolder_.getItem(QueryView.IDX_PANEL_MESSAGE);
    ti.getControl().setVisible(fShow);
    this.tabPanels_[QueryView.IDX_PANEL_STATISTICS].setVisible(fShow);
  }

  public void showTracePanel(boolean fShow) {
    this.tabPanels_[QueryView.IDX_PANEL_TRACE].setVisible(fShow);
  }

  public void showMessagesPanel(boolean fShow) {
    CTabItem ti = resultsTabFolder_.getItem(QueryView.IDX_PANEL_MESSAGE);
    ti.getControl().setVisible(fShow);
    // this.tabPanels_[QueryView.IDX_PANEL_MESSAGE].setVisible( fShow );
  }

  protected final HashMap<String, Image> getTabItemImages() {
    return imgsTabItem_;
  }
}
