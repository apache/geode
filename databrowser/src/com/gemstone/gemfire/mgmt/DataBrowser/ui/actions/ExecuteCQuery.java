/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.window.ApplicationWindow;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.CQConfiguarationPrms;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.ICQueryEventListener;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ICQEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CqAppWindow;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomUIMessages;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.MainAppWindow;

/**
 * @author mghosh
 * 
 */
public class ExecuteCQuery extends ExecuteQuery {

  private CqAppWindow parentWindow_;

  private boolean isCqRunning = false;

  private boolean isCqRegistered = true;

  /**
   * 
   */
  public ExecuteCQuery(CqAppWindow win) {
    super();
    parentWindow_ = win;
  }

  // **********************************************************
  //
  // IRunnableWithProgress
  //
  // **********************************************************

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.jface.operation.IRunnableWithProgress#run(org.eclipse.core.
   * runtime.IProgressMonitor)
   */
  @Override
  public void run(IProgressMonitor monitor) throws InvocationTargetException,
      InterruptedException {
    // TODO Auto-generated method stub

  }

  // **********************************************************
  //
  // AbstractDataBrowserAction stuff
  //
  // **********************************************************

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getText()
   */
  @Override
  public String getText() {
    String text;
    if (!isCqRunning)
      text = "Execute Continuous Query";
    else
      text = "Stop Continuous Query";

    return text;
  }

  // **********************************************************
  //
  // IAction stuff
  //
  // **********************************************************

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getDescription()
   */
  @Override
  public String getDescription() {
    return "Executes a GemFire Continuous Query on the selected member";
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    if(this.isCqRunning) {
      return "Stops the specified Continuous Query"; 
    }
    
    return "Executes the specified Continuous Query on the selected member";
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    return isCqRegistered;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#run()
   */
  @Override
  public void run() {
    runWithEvent(new Event());
  }

  private void cqRegistered(boolean registered) {
    isCqRegistered = registered;
    setEnabled(registered);
  }

  private void cqRunning(boolean running) {
    isCqRunning = running;
    if (running)
      setText("Stop Continuous Query");
    else
      setText("Execute Continuous Query");
  }
  
  @Override
  protected ApplicationWindow getApplicationWindow() {
    return this.parentWindow_;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.jface.action.Action#runWithEvent(org.eclipse.swt.widgets.Event)
   */
  @Override
  public void runWithEvent(Event event1) {
    DataBrowserApp app = DataBrowserApp.getInstance();
    if (isCqRunning) {
      app.getController().closeCq(parentWindow_.getId());
      cqRunning(false);
      cqRegistered(true);
      return;
    }
    cqRunning(true);
    cqRegistered(false);

    // super.runWithEvent(event);
   
    if (null != app) {
      MainAppWindow wnd = app.getMainWindow();
      if (wnd != null) {
        ArrayList<Object> p = new ArrayList<Object>();
        // TODO MGH - this might be a default config param?
        p.add(Integer.valueOf(0)); // get the current selection
        ArrayList<Object> r = new ArrayList<Object>();
        parentWindow_.sendCustomMessage(
            CustomUIMessages.QRY_MSG_GET_CQ_QUERY_STR_FOR_EXEC, p, r);

        String strQuery = null;
        if (1 == r.size()) {
          Object rv = r.get(0);
          if (rv instanceof String) {
            strQuery = (String)rv;
          }
        }

        if (strQuery == null) {
          p = new ArrayList<Object>();
          p.add(Integer.valueOf(1)); // get the last query string line
          r = new ArrayList<Object>();
          parentWindow_.sendCustomMessage(
              CustomUIMessages.QRY_MSG_GET_CQ_QUERY_STR_FOR_EXEC, p, r);

          if (1 == r.size()) {
            Object rv = r.get(0);
            if (rv instanceof String) {
              strQuery = (String)rv;
            }
          }
        }

        if (strQuery == null) {
          MessageBox mb = new MessageBox(parentWindow_.getShell(), SWT.OK);
          // MGH This happens only when the pane is empty. If a line is selected 
          mb.setText("DataBrowser: Query Error"); 
          mb.setMessage("No query was provided. Please select a query");
          mb.open();
          cqRunning(false);
          cqRegistered(true);
          return;
        }

        p = new ArrayList<Object>();
        r = new ArrayList<Object>();

        wnd.sendCustomMessage(
            CustomUIMessages.QRY_MEMBER_SELECTED_FOR_QUERY_EXEC, p, r);

        GemFireMember member = parentWindow_.getAssociatedMember();
     
        ICQueryEventListener lstnr = new CQueryEventListener();
        CQConfiguarationPrms qryParams = new CQConfiguarationPrms(parentWindow_.getId(),lstnr);
        qryParams.setQueryString(strQuery);
        qryParams.setMember(member);
        qryParams.setQueryExecutionListener(new CQueryExecListener());
        securityProps = null;
        executeQuery(app, qryParams);
      }
    }
  }
  
  @Override
  protected void errorOccuredWhileQueryExecution(Exception e){
    DataBrowserController controller = DataBrowserApp.getInstance().getController();
    controller.closeCq(parentWindow_.getId()); 
    
    String syntaxError = "";
    if (e.getCause() instanceof QueryInvalidException) {
      QueryInvalidException cause = (QueryInvalidException) e.getCause();
      syntaxError = extractSyntaxErrorInfo(cause, getQueryString());
    }
    
    MessageBox mb = new MessageBox(parentWindow_.getShell(), SWT.OK);
    mb.setText("GF DataBrowser: Query Error");
    String errorMsg = "An error occured on the member while executing the query. The message from the server is:\n" + e.getLocalizedMessage();
    errorMsg = errorMsg + "\n" + syntaxError + "\nPlease check logs for further details.";
    mb.setMessage(errorMsg);
    mb.open();
    cqRunning(false);
    cqRegistered(true);   
  }
  
  @Override
  protected void cancelQuery(){
    cqRunning(false);
    cqRegistered(true);   
  }

  protected final class CQueryExecListener extends QueryExecListener {
    private CqExecutionListener cqExecListener= new CqExecutionListener();
    
    private CqFailedListener cqFailedListener= new CqFailedListener();
    
    CQueryExecListener() {
    }
    
    @Override
    public void queryFailed(IQueryExecutedEvent queryEvent, Throwable ex) {
      DataBrowserController controller = DataBrowserApp.getInstance().getController();
      controller.closeCq(parentWindow_.getId()); 
      if (parentWindow_ != null) {
        if (parentWindow_ != null) {
          cqFailedListener.setData(queryEvent, ex);
          if(cqFailedListener.isProcessed()){
            cqFailedListener.setProcessed(false);
            Display display = parentWindow_.getShell().getDisplay();
            display.asyncExec(cqFailedListener);  
          }
        }
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutionListener
     * #queryExecuted
     * (com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutedEvent)
     */
    @Override
    public void queryExecuted(IQueryExecutedEvent queryEvent) {
      if (parentWindow_ != null) {
        if(cqExecListener.isProcessed()){
          cqExecListener.setProcessed(false);
          Display display = parentWindow_.getShell().getDisplay();
          display.asyncExec(cqExecListener);  
        }
      }
    }
  }
  
  private final class CQueryEventListener implements ICQueryEventListener {

    CQQuery cQuery_;
    private CqEventProcessor cqEventProcessor= new CqEventProcessor();

    public void setQuery(CQQuery query) {
      cQuery_ = query;
      final ArrayList<Object> p = new ArrayList<Object>();
      final ArrayList<Object> r = new ArrayList<Object>();
      p.add(cQuery_);

      parentWindow_.sendCustomMessage(
          CustomUIMessages.QRY_MSG_SET_CQ_QUERY_FOR_DISP, p, r);
    }

    public void close() {
      if (cQuery_ != null) {
        cQuery_.getQueryResult().removeCQEventListener(this);
        cQuery_.getQueryResult().removeTypeListener(this);
      }
    }

    public void onEvent(ICQEvent cqEvent) {
      processEvent(cqEvent);
    }

    public void onNewTypeAdded(IntrospectionResult result) {
      processEvent(result);
    }
    
    private void processEvent(Object object){
      cqEventProcessor.addEvent(object);
      if(cqEventProcessor.isProcessed()){
        cqEventProcessor.setProcessed(false);
        Display display = parentWindow_.getShell().getDisplay();
        display.asyncExec(cqEventProcessor);  
      }
    }
  }

  /**
   * Runnable to process event of running cq
   * @author mjha
   */
  private  class CqEventProcessor implements Runnable{
    private boolean processed = true;
    private ArrayList<Object> cqEvent = new ArrayList<Object>();

    public void run() {
      processed = false;
      ArrayList<Object> res = new ArrayList<Object>();
      ArrayList<Object> prms= new ArrayList<Object>();
      synchronized (cqEvent) {
        prms.addAll(cqEvent);
        cqEvent.clear();
        processed = true;
      }
      if(parentWindow_ != null){
        Shell shell = parentWindow_.getShell();
        if (shell != null && !shell.isDisposed()) {
          parentWindow_.sendCustomMessage(
              CustomUIMessages.QRY_MSG_PROCESS_CQ_QUERY_EVT, prms, res);
        }
      }
    }
    
    public boolean isProcessed(){
      return processed;
    }
    
    public void setProcessed(boolean processed) {
      this.processed = processed;
    }
    
    public void addEvent(Object object) {
      synchronized (cqEvent) {
        cqEvent.add(object);
      }
    }
  }
  
  /**
   * Runnable to be invoke once cq is successfully registered with server
   * @author mjha
   */
  private  class CqExecutionListener implements Runnable{
    private boolean processed = true;

    public void run() {
      processed = false;
      cqRunning(true);
      cqRegistered(true);
      processed = true;
    }
    
    public boolean isProcessed(){
      return processed;
    }
    
    public void setProcessed(boolean processed) {
      this.processed = processed;
    }
    
  }
  
  /**
   * Runnable to be invoke once cq is failed error received
   * @author mjha
   */
  private  class CqFailedListener implements Runnable{
    private boolean processed = true;
    private Throwable ex;
    private IQueryExecutedEvent queryEvent;

    public void run() {
      processed = false;
      synchronized (this) {
        if(queryEvent != null && ex != null){
          errorInQueryExecution(queryEvent, ex);
          queryEvent = null;
          ex = null;
        }
      }
      cqRunning(false);
      cqRegistered(true);
      processed = true;
    }
    
    public boolean isProcessed(){
      return processed;
    }
    
    public void setProcessed(boolean processed) {
      this.processed = processed;
    }
    
    public void setData(IQueryExecutedEvent qEvent, Throwable e){
      synchronized (this) {
        ex= e;
        queryEvent = qEvent;
      }
    }
  }
}
