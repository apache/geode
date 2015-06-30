/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.MessageBox;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.export.QueryResultExporter;
import com.gemstone.gemfire.mgmt.DataBrowser.query.export.QuerySchemaExporter;
import com.gemstone.gemfire.mgmt.DataBrowser.query.export.xml.XMLDocExportHandler;
import com.gemstone.gemfire.mgmt.DataBrowser.query.export.xml.XMLSchemaExportHandler;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.MainAppWindow;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 *
 */
public class ExportQueryResults extends AbstractDataBrowserAction implements
    IRunnableWithProgress {

  private static final String iconEnabled = null;
  private static final String iconDisabled = null;

  private static final int BLOCK_SIZE_CONST = 1024 * 64;

  private String queryString_ = null;
  private String xmlFilePath = null;
  private volatile boolean opResult = false;
  private String logMsg = null;
  private Throwable errThrwn = null;
  private ProgressMonitorDialog dlg = null;

  /**
   *
   */
  public ExportQueryResults() {
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

    DataBrowserApp app = DataBrowserApp.getInstance();
    DataBrowserController controller = app.getController();

    if (!controller.hasConnection()
        || (controller.getLastQueryResults() == null))
      return;


    final File doc_file = new File(this.xmlFilePath);
    XMLDocExportHandler doc_handler = null;
    QueryResultExporter export = null;
    XMLSchemaExportHandler schema_handler = null;


    String result = null;
    String schema = null;
    try {
      doc_handler = new XMLDocExportHandler(doc_file);
      export = new QueryResultExporter(doc_handler, -1);
      schema_handler = new XMLSchemaExportHandler();

      monitor.subTask("Exporting the Query results as XML document...");
      export.exportData(controller.getLastQueryResults());
      result = doc_handler.getResultDocument().toString();
      monitor.worked(20);

      monitor.subTask("Exporting the schema of the Query results as XSD document...");
      QuerySchemaExporter.exportSchema(schema_handler, controller
          .getLastQueryResults());
      schema = schema_handler.getSchema();
      monitor.worked(20);
    }
    catch( ColumnNotFoundException xptn ) {
      opResult = true;
      errThrwn = xptn;
      if( null == result ) {
        logMsg = "ColumnNotFoundException thrown transforming results to XML during export.";
      }
      else {
        logMsg = "ColumnNotFoundException thrown transforming schema to XML during export.";
      }
    }
    catch( ColumnValueNotAvailableException xptn ) {
      opResult = true;
      errThrwn = xptn;
      logMsg = "ColumnValueNotAvailableException thrown transforming results to XML during export.";

    } catch (IOException e) {
      opResult = true;
      errThrwn = e;
      logMsg = "IOException thrown transforming results to XML during export.";
    } finally {
      if(doc_handler != null) {
        try {
          doc_handler.close();
        } catch (IOException e) {
          opResult = true;
          errThrwn = e;
          logMsg = "IOException thrown saving the XML result to disk.";
        }
      }
    }

    FileWriter writer = null;

    final File parent_dir = doc_file.getParentFile();
    // Output the schema (XSD) file.
    if( false == opResult ) {
      String fileName = doc_file.getName();
      int index = fileName.lastIndexOf('.');
      if(index > 0) {
    	fileName = fileName.substring(0, index);
      }
      fileName = fileName + ".xsd";

      monitor.subTask("Saving the XSD document to the file system...");
      Thread.sleep(5000);
      File schema_file = new File(parent_dir, fileName);
      try {
        writer = new FileWriter(schema_file);
        int start = 0 ;
        while(start < schema.length()) {
          int end = ((schema.length() - start) > BLOCK_SIZE_CONST ) ? BLOCK_SIZE_CONST : (schema.length() - start);
          writer.write(schema, start, end);
          writer.flush();
          start += end;
         }
      } catch (IOException xptn) {
        opResult = true;
        errThrwn = xptn;
        if (null != writer) {
          logMsg = "Exception thrown while writing to, flushing, or closing the XSD file "
              + schema_file + " during export.";
        } else {
          logMsg = "Unable to create FileWriter for " + schema_file;
        }
      }
      finally {
        if( null != writer ) {
          try {
            writer.close();
          } catch( IOException xptn ) {
            logMsg = "Exception thrown while closing the XSD file: " + schema_file;
            LogUtil.warning( logMsg, xptn );
            logMsg = null;
          }

          writer = null;
        }
      } // for XSD file
    } // if( false == failed )
    monitor.worked(30);
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
    return "Export Results...";
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction#getDisabledIcon()
   */
  @Override
  public String getDisabledIcon() {
    return iconDisabled;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.AbstractDataBrowserAction#getEnabledIcon()
   */
  @Override
  public String getEnabledIcon() {
    return iconEnabled;
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
    return "Export the Query Results into xml";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#getToolTipText()
   */
  @Override
  public String getToolTipText() {
    return "Export the Query Results into xml";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.action.Action#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    DataBrowserApp app = DataBrowserApp.getInstance();
    boolean fRet = false;
    if (null != app) {
      DataBrowserController ctrlr = app.getController();
      if (null != ctrlr)
        fRet = (ctrlr.getLastQueryResults() != null);
    }
    return fRet;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.action.Action#runWithEvent(org.eclipse.swt.widgets.Event)
   */
  @Override
  public void runWithEvent(Event event) {
    DataBrowserApp app = DataBrowserApp.getInstance();
    DataBrowserController controller = app.getController();

    // TODO : Display proper message if there is no query result to export.
    // TODO MGH - Is the table emptied when there is no connection? If data is still displayed
    //            the user should be able to export.
    if (!controller.hasConnection()
        || (controller.getLastQueryResults() == null))
      return;

    final MainAppWindow wnd = app.getMainWindow();
    boolean fRetry = false;
    String path;

    //Step 1 : Fetch the information about the path of File system, where the result is to be saved.
    //Check if we have proper access permissions for saving the file. If not prompt user for some other location.
    do {
      fRetry = false;
      FileDialog dialog = new FileDialog(wnd.getShell(), SWT.SAVE);
      dialog.setOverwrite(true);
      dialog.setFilterExtensions(new String[] { "*.xml" });
      path = dialog.open();

      if(null != path) {
        File temp = new File(path);
        if(temp.exists()) {
          if(!temp.canWrite()) {
            MessageBox mb = new MessageBox(wnd.getShell(), SWT.YES | SWT.NO);
            mb.setText("Error: Unable to export results");
            mb.setMessage( "User permissions do not allow writes to this file." + "\nWould you like to reattempt to export to other location ?");

            int iRetry = mb.open();
            fRetry = (SWT.YES == iRetry) ? true : false;
          }

        } else {
          File parent = temp.getParentFile();
          if(!parent.canWrite()) {
            MessageBox mb = new MessageBox(wnd.getShell(), SWT.YES | SWT.NO);
            mb.setText("Error: Unable to export results");
            mb.setMessage("User permissions do not allow writes to this directory." + "\nWould you like to reattempt to export to other location ?" );

            int iRetry = mb.open();
            fRetry = (SWT.YES == iRetry) ? true : false;
          }
        }
      } else {
        fRetry = false;
      }

    } while( true == fRetry);

    //Step 2 : Perform the actual export operation.
    if (path != null) {
      this.xmlFilePath = path;
      this.opResult = false;
      this.logMsg = null;
      this.errThrwn = null;

      try {
        dlg = new ProgressMonitorDialog(app.getMainWindow().getShell());
        dlg.run(true, false, this);
     } catch (Exception e) {
       this.errThrwn = e;
       this.opResult = true;
     }

      if (true == opResult) {
        LogUtil.error( logMsg, errThrwn );

        MessageBox mb = new MessageBox(wnd.getShell(), SWT.OK);
        mb.setText("Error: Unable to export results");
        // TODO - MGH - will getLogDir return anything sensible; can we get the FQN of the log file itself?
        mb.setMessage( logMsg + "\nPlease check the log file in " + LogUtil.getLogDir() + " for more details");
        mb.open();
      }
    }
  }

}
