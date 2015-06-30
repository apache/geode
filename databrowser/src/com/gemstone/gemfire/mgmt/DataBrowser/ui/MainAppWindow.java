/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.util.ArrayList;

import org.eclipse.jface.action.CoolBarManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.StatusLineManager;
import org.eclipse.jface.window.ApplicationWindow;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.DSConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DSSnapShot;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomMsgDispatcher.ICustomMessageListener;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ConnectToDS;

/**
 * @author mghosh
 *
 */
public class MainAppWindow extends ApplicationWindow {
  /*
   * public static final String CSTM_MSG_SHOW_QUERY_TRACE_PANE =
   * "Hide Query Trace Pane"; public static final String
   * CSTM_MSG_SHOW_QUERY_STATISTICS_PANE = "Hide Query Log Pane";
   * public static final String CSTM_MSG_SHOW_QUERY_MESSAGES_PANE =
   * "Hide Query Messages Pane"; public static final String
   * CSTM_MSG_SHOW_QUERY_EXECPLAN_PANE = "Hide Query ExecutionPlan Pane";
   */

  // -- some defaults used if needed
  final static Display             display_           = new Display();
  final static Shell               shell_             = new Shell(
                                                          MainAppWindow.display_);
  final static String              STR_TITLE_BASE     = "GemFire DataBrowser - ";
  final static Color               colorWHITE         = new Color(
                                                          MainAppWindow.display_,
                                                          0xFF, 0xFF, 0xFF);
  final static Color               colorPaneBkGrnd_   = MainAppWindow.colorWHITE;

  final static CustomMsgDispatcher msgDispatcher_     = new CustomMsgDispatcher();

  // ImageDescriptor imgMenuManager = null; //
  // ImageDescriptor.createFromFile(...)
  // -- UI element managers
  MainWindowMenuManager            menuManager_       = null;
  MainWindowCoolBarManager         coolBarManager_    = null;
  MainWindowStatusLineManager      statusLineManager_ = null;

  // -- 'control/feedback' elements : [cool/tool]bars, status, menus
  Composite                        parentPane_        = null;
  SashForm                         sash               = null;
  DSView                           treeDS_            = null;
  QueryView                        queryView_         = null;
  ArrayList<CqAppWindow>           cQWindowList_      = null;

  static Shell                     parentShell_       = null;
  private DSConfiguration dsConfig;
  /**
   * @param parentShell
   */
  public MainAppWindow(Shell parentShl) {
    super(parentShl);
    setBlockOnOpen(true);
    addMenuBar();
    addCoolBar(SWT.NONE);
    addStatusLine();
    registerForDSConnectMsg();
    registerForDSDisconnectMsg();
  }

  private boolean registerForDSConnectMsg() {
    return registerForMessage(CustomUIMessages.DS_CONNECTED, hndlrCnxnMsgs);
  }

  private boolean registerForDSDisconnectMsg() {
    return registerForMessage(CustomUIMessages.DS_DISCONNECTED, hndlrCnxnMsgs);
  }

  private boolean registerForMessage(String msg, ICustomMessageListener hndlr) {
    return addCustomMessageListener(msg, hndlr);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.window.Window#handleShellCloseEvent()
   */
  @Override
  protected void handleShellCloseEvent() {
    DataBrowserApp.getInstance().exit();
    super.handleShellCloseEvent();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.window.ApplicationWindow#createMenuManager()
   */
  @Override
  protected MenuManager createMenuManager() {
    if (null == menuManager_) {
      menuManager_ = new MainWindowMenuManager(
          "DataBrowserMainWindowMenuManager");
    }

    return menuManager_;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.window.ApplicationWindow#createCoolBarManager(int)
   */
  @Override
  protected CoolBarManager createCoolBarManager(int style) {
    if (null == coolBarManager_) {
      coolBarManager_ = new MainWindowCoolBarManager(SWT.FLAT);
    }

    return coolBarManager_;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.window.ApplicationWindow#createStatusLineManager()
   */
  @Override
  protected StatusLineManager createStatusLineManager() {
    if (null == statusLineManager_) {
      statusLineManager_ = new MainWindowStatusLineManager();
    }

    return statusLineManager_;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.window.ApplicationWindow#configureShell(org.eclipse.swt
   * .widgets.Shell)
   */
  @Override
  protected void configureShell(Shell shell) {
    super.configureShell(shell);

    parentShell_ = (null == shell) ? MainAppWindow.shell_ : shell;

    // -- set title
    parentShell_.setText(MainAppWindow.STR_TITLE_BASE);

    // -- set any visual attributes based on previously saved state and user
    // prefs
    // -- set location & size on screen
    // shell.setBounds(x, y, width, height);
    // -- set background
    // shell.setBackground(color);
    // shell.setBackgroundImage(image);
//    StartupSplashUpdater ssu = new StartupSplashUpdater(this);
//    SplashScreen ss = new SplashScreen(parentShell_.getDisplay(), ssu);
//    ss.show();

  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.window.ApplicationWindow#canHandleShellCloseEvent()
   */
  @Override
  protected boolean canHandleShellCloseEvent() {
    // TODO MGH: Check if any background ops are in progress before returning
    return super.canHandleShellCloseEvent();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.window.Window#createContents(org.eclipse.swt.widgets.
   * Composite)
   */
  @Override
  protected Control createContents(Composite parent) {
    parentPane_ = parent;

    createSash(parentPane_);
    createLeftPane(sash);
    createRightPane(sash);
    sash.setWeights(new int[] { 30, 70 });
    if(dsConfig != null){
      ConnectToDS.connect(dsConfig);
    }
    return parentPane_;
  }

  public DSConfiguration getDsConfig() {
    return dsConfig;
  }

  public void setDsConfig(DSConfiguration dsConfig) {
    this.dsConfig = dsConfig;
  }

  private void createSash(Composite prnt) {
    sash = new SashForm(prnt, SWT.FLAT | SWT.HORIZONTAL | SWT.CLIP_SIBLINGS);

    FillLayout lytSash = new FillLayout();
    lytSash.type = SWT.VERTICAL;
    sash.setLayout(lytSash);
  }

  private void createLeftPane(SashForm prnt) {
    treeDS_ = new DSView(prnt, SWT.FLAT);
  }

  private void createRightPane(SashForm prnt) {
    queryView_ = new QueryView(prnt, SWT.FLAT);
  }

  // *****************************************************************
  //
  // Splash screen stuff
  //
  // *****************************************************************

  private static class StartupSplashUpdater implements
      SplashScreenProgressCallback {
    // TODO MGH: This for demonstrative purposes.
    private static final int    counterLimit = 1000;
    private int                 currCounter  = StartupSplashUpdater.counterLimit;

    @SuppressWarnings("unused")
    private final MainAppWindow mainWindow_;

    StartupSplashUpdater(MainAppWindow maw) {
      mainWindow_ = maw;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gemstone.gemfire.mgmt.DataBrowser.ui.SplashScreenProgressCallback
     * #handleProgress(java.lang.Integer)
     */
    public ActionCode handleProgress(Integer[] percentComplete) {
      int iStat = (100 - ((100 * currCounter) / StartupSplashUpdater.counterLimit));
      percentComplete[0] = Integer.valueOf(iStat);
      ActionCode ac = null;
      if (iStat < 100)
        ac = ActionCode.CONTINUE;
      else
        ac = ActionCode.END_SUCCESS;

      currCounter--;

      try {
        Thread.sleep(0);
      } catch (Throwable e) {
        //TODO logging of error??
      }

      return ac;
    }
  } // StartupSplashUpdater

  // ---------------------------------------------------------------
  // App custom messages
  // ---------------------------------------------------------------
  /*
   * public interface ICustomMessageListener { void handleEvent( String msg,
   * ArrayList< Object > params, ArrayList< Object > results ); }
   *
   * static private ArrayList< String > customMessages_;
   *
   * static private HashMap< String, ArrayList< ICustomMessageListener > >
   * customMsgHandlers_;
   */
  // -- Init custom message handling
  static {
    /*
     * MainAppWindow.customMessages_ = new ArrayList< String >();
     * MainAppWindow.customMessages_.add(
     * MainAppWindow.CSTM_MSG_SHOW_QUERY_TRACE_PANE );
     * MainAppWindow.customMessages_.add(
     * MainAppWindow.CSTM_MSG_SHOW_QUERY_STATISTICS_PANE );
     * MainAppWindow.customMessages_.add(
     * MainAppWindow.CSTM_MSG_SHOW_QUERY_MESSAGES_PANE );
     * MainAppWindow.customMessages_.add(
     * MainAppWindow.CSTM_MSG_SHOW_QUERY_EXECPLAN_PANE );
     */
    /*
     * MainAppWindow.customMsgHandlers_ = new HashMap< String, ArrayList<
     * ICustomMessageListener > >(); for( String s :
     * MainAppWindow.customMessages_ ) { MainAppWindow.customMsgHandlers_.put(
     * s, new ArrayList< ICustomMessageListener >() ); }
     */
  }
  @Override
  public boolean close() {
    if(cQWindowList_ != null){
      for (int i = 0; i < cQWindowList_.size(); i++) {
        CqAppWindow cqAppWindow = cQWindowList_.get(i);
        cqAppWindow.close();
      }
      cQWindowList_.clear();
      cQWindowList_ = null;
    }

    return super.close();
  }

  public void removeCqWindow(CqAppWindow win){
    cQWindowList_.remove(win);
  }

  public void openNewCqWindow(GemFireMember member){
    CqAppWindow cqAppWindow = new CqAppWindow(this, member);
    if(cQWindowList_ == null)
      cQWindowList_ = new ArrayList<CqAppWindow>();

    cQWindowList_.add(cqAppWindow);

    cqAppWindow.open();

  }

  public boolean addCustomMessageListener(String msg,
      ICustomMessageListener lstnrNew) {
    return MainAppWindow.msgDispatcher_.addCustomMessageListener(msg, lstnrNew);
  }

  // TODO MGH - eventually change the code elsewhere in the ui package to
  // directly call the dispatcher
  public void sendCustomMessage(String msg, ArrayList<Object> prms,
      ArrayList<Object> res) {
    if (null != msg) {
      MainAppWindow.msgDispatcher_.sendCustomMessage(msg, prms, res);
    }
    if (msg.equalsIgnoreCase(CustomUIMessages.DS_CONNECTED)
        || msg.equalsIgnoreCase(CustomUIMessages.DS_DISCONNECTED))
      sendMessageToCqWindow(msg, prms, res);
  }
  
  private void sendMessageToCqWindow(String msg, ArrayList<Object> prms,
      ArrayList<Object> res) {
    if(cQWindowList_ == null)
      return;
    if (null != msg) {
      for (int i = 0; i < cQWindowList_.size(); i++) {
        CqAppWindow cqAppWindow = cQWindowList_.get(i);
        cqAppWindow.sendCustomMessage(msg, prms, res);
      }
    }
  }

  private TBMgr_CnxnMsgHndlr hndlrCnxnMsgs = new TBMgr_CnxnMsgHndlr();

  static private class TBMgr_CnxnMsgHndlr implements
      CustomMsgDispatcher.ICustomMessageListener {

    TBMgr_CnxnMsgHndlr() {
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
          if ((true == CustomUIMessages.DS_CONNECTED.equals(msg))) {
            DataBrowserApp instance = DataBrowserApp.getInstance();
            if (instance == null)
              return;
            DataBrowserController controller = instance.getController();
            DSConfiguration currentConnection = controller
                .getCurrentConnection();
            String hostPort = "";
            if (currentConnection != null) {
              hostPort = currentConnection.getHost() + ":"
                  + currentConnection.getPort();
              
              String version = currentConnection.getVersion();
              if (version != null) {
                hostPort += " - GemFire Version: " + version;
              }
            }

            parentShell_.setText(MainAppWindow.STR_TITLE_BASE + hostPort);
          } else if ((true == CustomUIMessages.DS_DISCONNECTED.equals(msg))) {
            parentShell_.setText(MainAppWindow.STR_TITLE_BASE);
          }
        }
      }
    }
  } // class TBMgr_CnxnMsgHndlr
}
