/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.util.ArrayList;

import org.eclipse.jface.action.CoolBarManager;
import org.eclipse.jface.window.ApplicationWindow;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomMsgDispatcher.ICustomMessageListener;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.ExecuteCQuery;

/**
 * @author mghosh
 * 
 */
public class CqAppWindow extends ApplicationWindow {
  private static final String prefix = "CqWindow";
  private static int noOfInstanceOpened = 0;
  final private Shell                parentShell_;
  final private MainAppWindow        mainWindow_;

  final static CustomMsgDispatcher msgDispatcher_     = new CustomMsgDispatcher();

  private  Shell                   shell_             = null;
  private final String                 id ;
  MainWindowMenuManager            menuManager_       = null;
  MainWindowCoolBarManager         coolBarManager_    = null;

  // -- 'control/feedback' elements : [cool/tool]bars, status, menus
  Composite                        parentPane_        = null;
  CQueryView                       queryView_         = null;
  private GemFireMember            gemfireMember_;
  private ExecuteCQuery            cQueryAction_;




  /**
   * @param parentShell
   */
  public CqAppWindow(MainAppWindow mainWin, GemFireMember mem) {
    super(null);
    parentShell_ = mainWin.getShell();
    mainWindow_ = mainWin;
    gemfireMember_ = mem;
    setBlockOnOpen(true);
    addCoolBar(SWT.NONE);
    noOfInstanceOpened++;
    id = prefix + String.valueOf(noOfInstanceOpened);
  }
  
  public String getId(){
    return id;
  }


  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.window.Window#handleShellCloseEvent()
   */
  @Override
  protected void handleShellCloseEvent() {
    mainWindow_.removeCqWindow(this);
    super.handleShellCloseEvent();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.window.ApplicationWindow#createCoolBarManager(int)
   */
  @Override
  protected CoolBarManager createCoolBarManager(int style) {
    if (null == coolBarManager_) {
      coolBarManager_ = new CQWindowCoolBarManager(this, SWT.FLAT);
    }

    return coolBarManager_;
  }
  
  public ExecuteCQuery getExecuteCQueryAction(){
    return cQueryAction_;
  }
  
  public void setExecuteCQueryAction(ExecuteCQuery queryAction){
    cQueryAction_ = queryAction; 
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

    shell_ = shell;

    // -- set title
    shell_.setText("CQ Window -" + gemfireMember_.getRepresentationName());
    int y = parentShell_.getSize().y;
    shell_.setSize(y, 90* y/ 100);
  }
  
  public GemFireMember getAssociatedMember(){
    return gemfireMember_;
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

    createPane(parentPane_);
    return parentPane_;
  }
  
  @Override
  public boolean close() {
    DataBrowserController controller = DataBrowserApp.getInstance().getController();
    controller.closeCq(id);
    return super.close();
  }

  private void createPane(Composite prnt) {
    queryView_ = new CQueryView(this, prnt, SWT.FLAT);
  }
  
  public CQueryView getQueryView(){
    return queryView_;
  }

  public boolean addCustomMessageListener(String msg,
      ICustomMessageListener lstnrNew) {
    return CqAppWindow.msgDispatcher_.addCustomMessageListener(msg + id, lstnrNew);
  }

  // TODO MGH - eventually change the code elsewhere in the ui package to
  // directly call the dispatcher
  public void sendCustomMessage(String msg, ArrayList<Object> prms,
      ArrayList<Object> res) {
    if (null != msg) {
      CqAppWindow.msgDispatcher_.sendCustomMessage(msg + id, prms, res);
    }
  }
  
//  public static void main(String[] args) {
//    CqAppWindow cqAppWindow = new CqAppWindow(null);
//    cqAppWindow.open();
//  }
}
