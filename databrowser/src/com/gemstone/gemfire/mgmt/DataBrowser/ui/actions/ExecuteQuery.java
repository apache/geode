/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.window.ApplicationWindow;
import org.eclipse.jface.window.Window;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.SecurityAttributes;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutionListener;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.InvalidConfigurationException;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.MissingSecuritiesPropException;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.QueryConfigurationPrms;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomUIMessages;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.MainAppWindow;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SpecifySecuritiesDlg;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite.Data;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite.SecurityProp;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;
import com.gemstone.gemfire.security.AuthenticationFailedException;

/**
 * @author mghosh
 *
 */
public class ExecuteQuery extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  private String              queryString_ = null;
  private QueryExecListener   qExecutionListener_;
  protected Data securityProps = null;

  private static final String iconEnabled  = null;
  private static final String iconDisabled = null;

  /**
   *
   */
  public ExecuteQuery() {
    super();
  }

  /**
   * @return the queryString_
   */
  public final String getQueryString() {
    return queryString_;
  }

  /**
   * @param queryString_
   *          the queryString_ to set
   */
  public final void setQueryString(String qs) {
    queryString_ = qs;
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
    return "Execute Query";
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getDisabledIcon()
   */
  @Override
  public String getDisabledIcon() {
    return iconDisabled;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction
   * #getEnabledIcon()
   */
  @Override
  public String getEnabledIcon() {
    return iconEnabled;
  }

  // **********************************************************  //
  // IAction stuff
  //
  // **********************************************************
  //
  // /* (non-Javadoc)
  // * @see org.eclipse.jface.action.Action#getDisabledImageDescriptor()
  // */
  // @Override
  // public ImageDescriptor getDisabledImageDescriptor() {
  // // TODO Auto-generated method stub
  // return super.getDisabledImageDescriptor();
  // }
  //
  // /* (non-Javadoc)
  // * @see org.eclipse.jface.action.Action#getImageDescriptor()
  // */
  // @Override
  // public ImageDescriptor getImageDescriptor() {
  //    final String fqnImage_ = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/ExecuteQuery.ico";
  // InputStream isImage = null;
  // try {
  // isImage = getClass().getResourceAsStream(fqnImage_);
  //
  // if (null != isImage) {
  // imgEnabled_ = new Image(null, isImage);
  // imgDescEnabled_ = ImageDescriptor.createFromImage(imgEnabled_);
  // }
  // } catch (NullPointerException xptn) {
  // // handler for getResourceAsStream
  // LogUtil.warning(
  // "NullPointerException in ExecuteQuery.getImageDescriptor (getResourceAsStream). Continuing...",
  // xptn );
  // } catch (SWTException xptn) {
  // // handler for org.eclipse.swt.graphics.Image ctor
  // // we continue an try to add the other nodes
  // LogUtil.warning(
  // "SWTException in ExecuteQuery.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). Continuing...",
  // xptn );
  // } catch (SWTError err) {
  // // Log this (Image ctor could throw this), and rethrow
  // LogUtil.error(
  // "SWTError in ExecuteQuery.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). This could be due to handles in the underlying widget kit being exhaused. Terminating.",
  // err );
  // throw err;
  // }
  // finally {
  // if( null != isImage ) {
  // try {
  // isImage.close();
  // } catch (IOException e) {
  // LogUtil.warning(
  // "IOException in QueryPrefsPage.getImageDescriptor (isImage.close(..)). Ignoring.",
  // e );
  // }
  // isImage = null;
  // }
  // }
  //
  // return imgDescEnabled_;
  // }
  //

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getDescription()
   */
  @Override
  public String getDescription() {
    return "Executes a GemFire OQL Query";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Execute a GemFire OQL query";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    DataBrowserApp app = DataBrowserApp.getInstance();
    boolean fRet = true;
    if (null != app) {
      DataBrowserController ctrlr = app.getController();
      if (null != ctrlr) {
        fRet = ctrlr.hasConnection();
      }
    }
    return fRet;
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
  
  protected ApplicationWindow getApplicationWindow() {
    DataBrowserApp app = DataBrowserApp.getInstance();
    return app.getMainWindow();
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
    if (null != app) {
      MainAppWindow wnd = (MainAppWindow)getApplicationWindow();
      if (null != wnd) {
        ArrayList<Object> p = new ArrayList<Object>();
        // TODO MGH - this might be a default config param?
        p.add(Integer.valueOf(0)); // get the current selection
        ArrayList<Object> r = new ArrayList<Object>();
        wnd.sendCustomMessage(CustomUIMessages.QRY_MSG_GET_QUERY_STR_FOR_EXEC,
            p, r);

        String strQuery = null;
        if (1 == r.size()) {
          Object rv = r.get(0);
          if (rv instanceof String) {
            strQuery = (String) rv;
          }
        }

        if (strQuery == null) {
          p = new ArrayList<Object>();
          p.add(Integer.valueOf(1)); // get the last query string line
          r = new ArrayList<Object>();
          wnd.sendCustomMessage(
              CustomUIMessages.QRY_MSG_GET_QUERY_STR_FOR_EXEC, p, r);

          if (1 == r.size()) {
            Object rv = r.get(0);
            if (rv instanceof String) {
              strQuery = (String) rv;
            }
          }
        }

        if (strQuery == null || (0 == strQuery.length())) {
          MessageBox mb = new MessageBox(wnd.getShell(), SWT.OK);
          mb.setText("DataBrowser Error");
          mb.setMessage("No query string provided for execution.");
          mb.open();
          return;
        }

        p = new ArrayList<Object>();
        r = new ArrayList<Object>();

        wnd.sendCustomMessage(
            CustomUIMessages.QRY_MEMBER_SELECTED_FOR_QUERY_EXEC, p, r);

        GemFireMember member = null;
        // TODO MGH - is there any possibility that multiple members could be
        // selected?
        if (1 == r.size()) {
          Object rv = r.get(0);
          if (rv instanceof GemFireMember) {
            member = (GemFireMember) rv;
          }
        }
        if (member == null) {
          MessageBox mb = new MessageBox(wnd.getShell(), SWT.OK);
          mb.setText("DataBrowser Error");
          mb
              .setMessage("A GemFire member must be selected to execute a query. Please select a member from the list.");
          mb.open();
          return;
        }

        QueryConfigurationPrms qryParams = new QueryConfigurationPrms();
        qryParams.setQueryString(strQuery);
        qryParams.setMember(member);

        if (qExecutionListener_ == null)
          qExecutionListener_ = new QueryExecListener();

        qryParams.setQueryExecutionListener(qExecutionListener_);
        securityProps = null;
        executeQuery(app, qryParams);
      }
    }
  }

  protected void executeQuery(DataBrowserApp app,
      QueryConfigurationPrms qryParams) {
    boolean retry = false;
    do {
      retry = false;
      try {
        LogUtil.fine("ExecuteQuery.runWithEvent: About to execute.");
        setQueryString(qryParams.getQueryString());
        app.getController().executeQuery(qryParams);
        retry = false;
      }
      catch (InvalidConfigurationException xptn) {
        LogUtil.error("ExecuteQuery.runWithEvent: Could not execute query.",
            xptn);
        retry = false;
        errorOccuredWhileQueryExecution(xptn);
      }
      catch (QueryExecutionException e) {
        LogUtil.error("ExecuteQuery.runWithEvent: Could not execute query.", e);
        retry = false;
        errorOccuredWhileQueryExecution(e);
      }
      catch (MissingSecuritiesPropException e) {
        MessageBox mb = new MessageBox(app.getMainWindow().getShell(), SWT.YES
            | SWT.NO);
        mb.setText("DataBrowser Error");
        mb
            .setMessage("Security is enabled on the server.\nWould you like to specify the security properties and execute the query again?");
        int open = mb.open();
        if (open == SWT.YES) {
          boolean popUpSpecifySecurityDlg = popUpSpecifySecurityDlg();
          if (popUpSpecifySecurityDlg)
            retry = true;
          else{
            retry = false;
            cancelQuery();
          }
        }
      }
      catch (AuthenticationFailedException ex) {
        MessageBox mb = new MessageBox(getApplicationWindow().getShell(),
            SWT.YES | SWT.NO);
        mb.setText("DataBrowser Error");

        String errMsg = ex.getMessage() + " " + qryParams.getQueryString();
        errMsg = errMsg
            + ". \n"
            + "Would you like to specify security properties and execute the query again ?";
        mb.setMessage(errMsg);
        int iRetry = mb.open();
        if (SWT.YES == iRetry) {
          boolean popUpSpecifySecurityDlg = popUpSpecifySecurityDlg();
          if (popUpSpecifySecurityDlg) {
            retry = true;
          }
        }
        if(!retry)
          cancelQuery();
      } finally {
	      /* un-setting query string as currently it's not required further. */
        setQueryString(null); 
      }

    } while (retry);
  }

  /**
   * This is suppose to do clean up, if any, once error happened while query
   * execution
   *
   * @param e
   */
  protected void errorOccuredWhileQueryExecution(Exception e) {

  }
  
  /**
   * cancel the query execution if query already running
   */
  protected void cancelQuery(){
    
  }

  private boolean popUpSpecifySecurityDlg() {
    DataBrowserApp app = DataBrowserApp.getInstance();
    Shell shl = getApplicationWindow().getShell();
    SpecifySecuritiesDlg dlg = new SpecifySecuritiesDlg(shl);

    String errMsg = null;
    boolean fPromptForRetry = false;
    boolean fRetry = false;
    boolean okPressed = false;
    do {
      fRetry = false;
      fPromptForRetry = false;
      okPressed = false;
      dlg.setSecurityData(securityProps);
      int iRetCode = dlg.open();
      fRetry = false;
      if (Window.OK == iRetCode) {
        okPressed = true;
        DataBrowserController cntrlr = DataBrowserApp.getInstance()
            .getController();
        SecurityAttributes sAttrs = new SecurityAttributes();

        securityProps = dlg.getData();

        String pluginJarFQN = securityProps.getSecurityPlugin();
        List<SecurityProp> secProperties = securityProps.getSecurityProperties();

        if ((pluginJarFQN.trim().length() == 0)) {
          fPromptForRetry = true;
          errMsg = "No Security plugin provided. \nWould you like to specify security properties again ?";
        }
        if (true == fPromptForRetry) {
          if (null != errMsg) {
            LogUtil.error(errMsg);
          }
          MessageBox mb = new MessageBox(shl, SWT.YES | SWT.NO);
          mb.setText( "Warning" );
          if (null == errMsg) {
            errMsg = "No Security plugin provided. \nWould you like to specify security properties again ?";
          }

          mb.setMessage(errMsg);
          int iRetry = mb.open();
          fRetry = (SWT.YES == iRetry) ? true : false;

        } else {
          sAttrs.setSecurityPluginPath(pluginJarFQN);
          Map<String, String> props = new HashMap<String, String>();
          for (int i = 0; i < secProperties.size(); i++) {
            SecurityProp securityProp = secProperties.get(i);
            String key = securityProp.getKey();
            String value = securityProp.getValue();
            props.put(key, value);
          }

          sAttrs.setSecurityProperties(props);
          cntrlr.setSecurityAttributes(sAttrs);
        }
      }
    } while (true == fRetry);

    return okPressed;
  }

  protected void errorInQueryExecution(IQueryExecutedEvent queryEvent,
      Throwable ex) {

    DataBrowserApp app = DataBrowserApp.getInstance();
    final ApplicationWindow wnd = getApplicationWindow();
    if (ex instanceof QueryExecutionException) {
      QueryExecutionException qEx = (QueryExecutionException) ex;
      if (qEx.isAuthenticationFailed()) {
        reExecuteQuery(queryEvent, ex);
        return;
      } else if (qEx.isAuthorizationFailed()) {
        MessageBox mb = new MessageBox(wnd.getShell(), SWT.OK);
        mb.setText("DataBrowser Error");
        mb.setMessage("An error occurred during executing the query:\t"
            + queryEvent.getQueryString() + "\n\n" + "Error Details = "
            + ex.getMessage());
        mb.open();
        return;
      }
    }
    if (ex instanceof AuthenticationFailedException) {
      reExecuteQuery(queryEvent, ex);
      return;
    }
    
    MessageBox mb = new MessageBox(wnd.getShell(), SWT.OK);
    mb.setText("DataBrowser Error");
    String errorMessage = "Following error occurred: \n";;

    String syntaxError = "";
    if (ex instanceof QueryInvalidException) {
      syntaxError = extractSyntaxErrorInfo((QueryInvalidException)ex, queryEvent.getQueryString());
    }

    if (ex != null && (ex.getMessage() != null)) {
      errorMessage = errorMessage + ex.getMessage();
    } else {
      String exception = String.valueOf(ex);
      errorMessage = errorMessage + exception;
    }
    mb.setMessage(errorMessage + "\n" + syntaxError + "\nPlease check logs for further details.");

    mb.open();
  }
  
  protected String extractSyntaxErrorInfo(QueryInvalidException ex, String queryString) {
    String syntaxError = "";

    antlr.RecognitionException recogException = null;
    Throwable cause = ex.getCause();
    if (cause instanceof antlr.TokenStreamRecognitionException) {
      recogException = ((antlr.TokenStreamRecognitionException) cause).recog;
    } else if (cause instanceof antlr.RecognitionException) {
      recogException = (antlr.RecognitionException) cause;
    }

    if (recogException != null) {
      int errorAt = recogException.getColumn();
      if (queryString != null) {
        queryString = queryString.trim(); //trims spaces & \r
        int queryLength = queryString.length();
        
        StringBuilder errorPointer = new StringBuilder();
        for (int i = 0; i <= queryLength; i++) {
          if (i == errorAt - 1) {
            errorPointer.append("^");
          } else {
            errorPointer.append("-");
          }
        }
        LogUtil.error(ex.getMessage() + "\n\t" + queryString 
                      + "\n\t" + errorPointer.toString());
      }
      /*
       * Line number information is also available. But it's for the line number
       * for a multi line query and it is not the line number on the UI. Hence,
       * skipping line number for now as it will be confusing to the user
       */
      syntaxError = "In the query string for character at position: " + errorAt;
    }
    return syntaxError;
  }

  private void reExecuteQuery(IQueryExecutedEvent queryEvent, Throwable ex) {

    DataBrowserApp app = DataBrowserApp.getInstance();
    ApplicationWindow wnd = getApplicationWindow();

    MessageBox mb = new MessageBox(wnd.getShell(), SWT.YES | SWT.NO);
    mb.setText("DataBrowser Error");

    String errMsg = ex.getMessage() + " " + queryEvent.getQueryString();
    errMsg = errMsg + ". \n" + "Would you like to specify security properties and execute the query again ?";
    mb.setMessage(errMsg);
    int iRetry = mb.open();

    boolean fPromptForRetry = (SWT.YES == iRetry) ? true : false;
    if (fPromptForRetry) {
      boolean popUpSpecifySecurityDlg = popUpSpecifySecurityDlg();
      if (popUpSpecifySecurityDlg) {
        QueryConfigurationPrms qryParams = new QueryConfigurationPrms();
        qryParams.setQueryString(queryEvent.getQueryString());
        qryParams.setMember(queryEvent.getMember());
        qryParams.setQueryExecutionListener(new QueryExecListener());
        executeQuery(app, qryParams);
      }else{
        cancelQuery();
      }
    }
  }

  protected class QueryExecListener implements IQueryExecutionListener {
    private QueryResultProcessor queryResultProcessor = new QueryResultProcessor();

    QueryExecListener() {
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutionListener
     * #queryExecuted
     * (com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutedEvent)
     */
    public void queryExecuted(IQueryExecutedEvent queryEvent) {
      QueryResult qryres = queryEvent.getQueryResult();

      DataBrowserApp app = DataBrowserApp.getInstance();
      ApplicationWindow wnd = getApplicationWindow();

      queryResultProcessor.addResults(qryres);
      if (queryResultProcessor.isProcessed()) {
        queryResultProcessor.setProcessed(false);
        runTaskInAsyncUIThread(wnd.getShell(), queryResultProcessor);
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutionListener
     * #queryFailed
     * (com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutedEvent,
     * java.lang.Throwable)
     */
    public void queryFailed(final IQueryExecutedEvent queryEvent,
        final Throwable ex) {
      if (ex != null) {
        LogUtil.error("Error while executing query: ", ex);
      }

      DataBrowserApp app = DataBrowserApp.getInstance();
      final ApplicationWindow wnd = getApplicationWindow();
      Runnable runnable = new Runnable() {
        public void run() {
          errorInQueryExecution(queryEvent, ex);
        }
      };

      runTaskInAsyncUIThread(wnd.getShell(), runnable);
    }

    private void runTaskInAsyncUIThread(Shell shell, Runnable runnable) {
      Display display = shell.getDisplay();
      display.asyncExec(runnable);
    }

  }

  private static class QueryResultProcessor implements Runnable {
    private boolean processed    = true;
    QueryResult     queryResults = null;

    public void run() {
      processed = false;
      ArrayList<Object> res = new ArrayList<Object>();
      ArrayList<Object> prms = new ArrayList<Object>();

      if (queryResults == null) {
        processed = true;
        return;
      }

      synchronized (this) {
        prms.add(queryResults);
        queryResults = null;
        processed = true;
      }
      DataBrowserApp app = DataBrowserApp.getInstance();
      if (null != app) {
        final MainAppWindow wnd = app.getMainWindow();
        if (wnd != null) {
          Shell shell = wnd.getShell();
          if (shell != null && !shell.isDisposed()) {
            wnd.sendCustomMessage(
                CustomUIMessages.QRY_MSG_ADD_QUERY_SINGLE_RESULT, prms, res);
          }
        }
      }
    }

    public boolean isProcessed() {
      return processed;
    }

    public void setProcessed(boolean processed) {
      this.processed = processed;
    }

    public void addResults(QueryResult qryres) {
      if (queryResults == null) {
        queryResults = qryres;
        return;
      }
      synchronized (this) {
        queryResults = qryres;
      }
    }
  }
}
