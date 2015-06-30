/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.IOException;
import java.io.InputStream;

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;

import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DefaultPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mjha
 *
 */
public class ConnectionPrefsPage extends PreferencePage implements
    IDataBrowserPrefsPage {

  // -- MGH - fixing a default size. Unable to figure out how to dynamically
  // size (or it is possible?)
  // private static final Point ptDef = new Point( 450, 450 );

  private Composite       parent_     = null;
  private Composite       basePane_   = null;
  private Group           grp1_       = null;
  private Group           clientGroup = null;
  private ImageDescriptor imgDesc_;
  private Image           img_;
  //private Text            txtReconnectInterval;
  //private Text            txtNoOfReconnect;
  private Text            txtTimeout;
  private Text            txtMru;
  private Text            socketReadTimeout;

  /**
   *
   */
  public ConnectionPrefsPage() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param title
   */
  public ConnectionPrefsPage(String title) {
    super(title);
  }

  /**
   * @param title
   * @param image
   */
  public ConnectionPrefsPage(String title, ImageDescriptor image) {
    super(title, image);
  }



  /* (non-Javadoc)
   * @see org.eclipse.jface.dialogs.DialogPage#dispose()
   */
  @Override
  public void dispose() {
    if( null != img_ ) {
      img_.dispose();
    }

    imgDesc_ = null;
    super.dispose();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.eclipse.jface.preference.PreferencePage#createContents(org.eclipse.
   * swt.widgets.Composite)
   */
  @Override
  protected Control createContents(Composite prnt) {
    parent_ = prnt;
    basePane_ = new Composite(parent_, SWT.NONE);

    FillLayout lytBase = new FillLayout();
    lytBase.type = SWT.VERTICAL;
    lytBase.marginHeight = 5;
    lytBase.marginWidth = 5;
    basePane_.setLayout(lytBase);

    grp1_ = new Group(basePane_, SWT.SHADOW_ETCHED_IN);
    grp1_.setText("Locator/JMX Manager Connection");

    GridLayout lytGridGrp1 = new GridLayout();
    lytGridGrp1.numColumns = 2;
    lytGridGrp1.verticalSpacing = 20;
    lytGridGrp1.marginHeight = 20;
    lytGridGrp1.makeColumnsEqualWidth = true;
    grp1_.setLayout(lytGridGrp1);

    GridData gridData = new GridData(SWT.BEGINNING, SWT.TOP, false, false);
    gridData.verticalSpan = 2;

    Label lblMru = new Label(grp1_, SWT.BOLD);
    lblMru.setLayoutData(gridData);
    String text = "Number of Locator/JMX Manager entries, to keep in history: ";
    lblMru.setText(text);

    txtMru = new Text(grp1_, SWT.BORDER);
    txtMru.setToolTipText("Number of host/port MRU entries for Locator/JMX Manager to store");
    txtMru.addModifyListener(new PrefModifyListenerAdapter(this));

    gridData = new GridData();
    gridData.horizontalAlignment = SWT.FILL;
    gridData.verticalAlignment = SWT.BOTTOM;
    gridData.grabExcessHorizontalSpace = true;
    gridData.verticalSpan = 2;
    txtMru.setLayoutData(gridData);


    Label lblTimeout = new Label(grp1_, SWT.BOLD);
    lblTimeout.setText("Timeout on connection with Locator/JMX Manager (ms): ");

    txtTimeout = new Text(grp1_, SWT.BORDER);
    gridData = new GridData();
    gridData.horizontalAlignment = SWT.FILL;
    gridData.grabExcessHorizontalSpace = true;
    txtTimeout.setLayoutData(gridData);
    txtTimeout.setToolTipText("Timeout to set on the connection attempts to the Locator/JMX Manager in milliseconds");
    txtTimeout.addModifyListener(new PrefModifyListenerAdapter(this));
    
    clientGroup = new Group(basePane_, SWT.SHADOW_ETCHED_IN);
    clientGroup.setText("Client Connection");

    GridLayout lytGridGrp2 = new GridLayout();
    lytGridGrp2.numColumns = 2;
    lytGridGrp2.verticalSpacing = 20;
    lytGridGrp2.marginHeight = 20;
    lytGridGrp2.makeColumnsEqualWidth = true;
    clientGroup.setLayout(lytGridGrp2);
    
    Label lblsockTimeout = new Label(clientGroup, SWT.BOLD);
    lblsockTimeout.setText("Server Response Timeout (ms): ");

    socketReadTimeout = new Text(clientGroup, SWT.BORDER);
    gridData = new GridData();
    gridData.horizontalAlignment = SWT.FILL;
    gridData.grabExcessHorizontalSpace = true;
    socketReadTimeout.setLayoutData(gridData);
    socketReadTimeout.setToolTipText("The amount of time, in milliseconds, to wait for a response from a server");    
    socketReadTimeout.addModifyListener(new PrefModifyListenerAdapter(this));

    /*Label reconnectLabel = new Label(grp1_, SWT.NONE);
    reconnectLabel.setText( "Retry attempts to connect to AdminAgent:" );
    gridData = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_BEGINNING,
        false, false);
    reconnectLabel.setLayoutData(gridData);

    txtNoOfReconnect = new Text(grp1_, SWT.BORDER);
    gridData = new GridData(SWT.FILL, GridData.VERTICAL_ALIGN_BEGINNING,
        true, false);
    txtNoOfReconnect.setLayoutData(gridData);
    txtNoOfReconnect.setEditable(true);
    txtNoOfReconnect.setToolTipText("Number of attemtps to connect to the AdminAgent");
    txtNoOfReconnect.addModifyListener(new PrefModifyListenerAdapter(this));

    Label reconnectIntervalLabel = new Label(grp1_, SWT.NONE);
    reconnectIntervalLabel.setText("Reconnect attempt interval (ms): ");
    gridData = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_BEGINNING,
        false, false);
    reconnectIntervalLabel.setLayoutData(gridData);

    txtReconnectInterval = new Text(grp1_, SWT.BORDER);
    gridData = new GridData(SWT.FILL, GridData.VERTICAL_ALIGN_BEGINNING,
        true, false);
    txtReconnectInterval.setLayoutData(gridData);
    txtReconnectInterval.setEditable(true);
    txtReconnectInterval.setToolTipText("Wait time between reconnect attemtps in milliseconds");
    txtReconnectInterval.addModifyListener(new PrefModifyListenerAdapter(this)); */
    setPreferences(false);

    return basePane_;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.preference.PreferencePage#performHelp()
   */
  @Override
  public void performHelp() {
    // TODO MGH - hook to display Help window with relevant information
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.preference.PreferencePage#performApply()
   */
  @Override
  protected void performApply() {
    String mruStr = txtMru.getText();
    String timeoutStr = txtTimeout.getText();
    String socketReadTimeoutStr = socketReadTimeout.getText();
//    String noOfReconnectStr = txtNoOfReconnect.getText();
//    String reconnectintervalStr = txtReconnectInterval.getText();

    long reconnectinterval;
    long timeout;
    int readTimeout;
    int mru;
    int noOfReconnect;
//    try {
//      reconnectinterval = Long.valueOf(reconnectintervalStr);
//    }
//    catch (NumberFormatException e) {
//      MessageBox mb = new MessageBox(getShell());
//      mb.setText("DataBrowser Error");
//      mb.setMessage("Please enter a valid ( greater than zero ) value for the reconnect interval.");
//      mb.open();
//      txtReconnectInterval.setFocus();
//      return ;
//    }
//    
//    if(reconnectinterval <= 0) {
//      MessageBox mb = new MessageBox(getShell());
//      mb.setText("DataBrowser Error");
//      mb.setMessage("Please enter a valid ( greater than zero ) value for the reconnect interval."); 
//      mb.open();
//      this.txtReconnectInterval.setFocus();
//      return;
//    }


    try {
      timeout = Long.valueOf(timeoutStr);
    }
    catch (NumberFormatException e) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Please enter a valid ( greater than zero ) value for the connection timeout interval (in milliseconds).");
      mb.open();
      txtTimeout.setFocus();
      return;
    }
    
    if(timeout <= 0) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Please enter a valid ( greater than zero ) value for the connection timeout interval (in milliseconds)."); 
      mb.open();
      this.txtTimeout.setFocus();
      return;
    }


    try {
      mru = Integer.valueOf(mruStr);
    }
    catch (NumberFormatException e) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Please enter a valid ( non negative ) value for the number of Locator/JMX Manager entries to keep in the history");
      mb.open();
      txtMru.setFocus();
      return ;
    }
    
    if(mru < 0) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Please enter a valid ( non negative ) value for the number of Locator/JMX Manager entries to keep in the history"); 
      mb.open();
      this.txtMru.setFocus();
      return;
    }
    
    try {
      readTimeout = Integer.valueOf(socketReadTimeoutStr);
    } catch (NumberFormatException e) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Please enter a valid ( greater than zero ) numeric value for server response timeout interval (in milliseconds).");
      mb.open();
      socketReadTimeout.setFocus();
      return;
    }
    
    if(readTimeout <= 0) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Please enter a valid ( greater than zero ) value for server response timeout interval (in milliseconds)."); 
      mb.open();
      this.socketReadTimeout.setFocus();
      return;
    }


//    try {
//      noOfReconnect = Integer.valueOf(noOfReconnectStr);
//    }
//    catch (NumberFormatException e) {
//      MessageBox mb = new MessageBox(getShell());
//      mb.setText("DataBrowser Error");
//      mb.setMessage( "Please enter a valid ( non negative ) value for the number of retry attempts." );
//      mb.open();
//      txtNoOfReconnect.setFocus();
//      return;
//    }
//    
//    if(noOfReconnect < 0) {
//      MessageBox mb = new MessageBox(getShell());
//      mb.setText("DataBrowser Error");
//      mb.setMessage("Please enter a valid ( non negative ) value for the number of retry attempts."); 
//      mb.open();
//      this.txtNoOfReconnect.setFocus();
//      return;
//    }


    DataBrowserPreferences.setConnectionMRUHListLimit(mru);
    DataBrowserPreferences.setConnectionTimeoutInterval(timeout);
    DataBrowserPreferences.setSocketReadTimeoutInterval(readTimeout);
//    DataBrowserPreferences.setConRetryInterval(reconnectinterval);
//    DataBrowserPreferences.setConRetryAttempts(noOfReconnect);
  }
  
  @Override
  public boolean performOk() {
    performApply();
    return super.performOk();
  }
  
  public boolean isValid() {
    String mruStr = txtMru.getText();
    String timeoutStr = txtTimeout.getText();
    String readTimeoutStr = socketReadTimeout.getText();
//    String noOfReconnectStr = txtNoOfReconnect.getText();
//    String reconnectintervalStr = txtReconnectInterval.getText();

    long temp1;
    int temp2;
    long readTimoutLong;

    try {
      temp2 = Integer.valueOf(mruStr);
    }
    catch (NumberFormatException e) {
      temp2 = -1;
    }
    
    if(temp2 < 0) {
      setErrorMessage("Please enter a valid ( non negative ) value for the number of Locator/JMX Manager entries to keep in the history"); 
      txtTimeout.setEnabled(false);
      socketReadTimeout.setEnabled(false);
//      txtNoOfReconnect.setEnabled(false);
//      txtReconnectInterval.setEnabled(false);
      return false;
    }

    try {
      temp1 = Long.valueOf(timeoutStr);
    }
    catch (NumberFormatException e) {
      temp1 = -1;
    }
    
    if(temp1 <= 0) {
      setErrorMessage("Please enter a valid ( greater than zero ) value for the connection timeout interval (in milliseconds)."); 
      txtMru.setEnabled(false);
      socketReadTimeout.setEnabled(false);
//      txtNoOfReconnect.setEnabled(false);
//      txtReconnectInterval.setEnabled(false);
      return false;
    }
    
    try {
      readTimoutLong = Long.valueOf(readTimeoutStr);
    }
    catch (NumberFormatException e) {
      readTimoutLong = -1;
    }
    
    if(readTimoutLong <= 0) {
      setErrorMessage("Please enter a valid ( greater than zero ) numeric value for server response timeout interval (in milliseconds)."); 
      txtMru.setEnabled(false);
      txtTimeout.setEnabled(false);
//      txtNoOfReconnect.setEnabled(false);
//      txtReconnectInterval.setEnabled(false);
      return false;
    }

//    try {
//      temp2 = Integer.valueOf(noOfReconnectStr);
//    }
//    catch (NumberFormatException e) {
//      temp2 = -1;
//    }
//    
//    if(temp2 < 0) {
//      setErrorMessage("Please enter a valid ( non negative ) value for the number of retry attempts."); 
//      txtMru.setEnabled(false);
//      txtTimeout.setEnabled(false);
//      txtReconnectInterval.setEnabled(false);
//      return false;
//    }

//    try {
//      temp1 = Long.valueOf(reconnectintervalStr);
//    }
//    catch (NumberFormatException e) {
//      temp1 = -1;
//    }
//    
//    if(temp1 <= 0) {
//      setErrorMessage("Please enter a valid ( greater than zero ) value for the reconnect interval.");
//      txtTimeout.setEnabled(false);
//      txtNoOfReconnect.setEnabled(false);
//      txtMru.setEnabled(false);
//      return false;
//    }
    
    setErrorMessage(null);
    txtMru.setEnabled(true);  
    txtTimeout.setEnabled(true);
    socketReadTimeout.setEnabled(true);
//    txtNoOfReconnect.setEnabled(true);
//    txtReconnectInterval.setEnabled(true);
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.preference.PreferencePage#performDefaults()
   */
  @Override
  protected void performDefaults() {
    setPreferences(true);
    super.performDefaults();
  }

  private void setPreferences(boolean isDefault){
    String temp1 ;
    String temp2 ;
    String readTimeoutStr ;
    
    if(isDefault) {
      temp1 = String.valueOf(DefaultPreferences.DEFAULT_MRU_HOST_PORT_LIST_LIMIT);
      temp2 = String.valueOf(DefaultPreferences.DEFAULT_CONNECTION_TIMEOUT);
      readTimeoutStr = String.valueOf(DefaultPreferences.DEFAULT_SOCKET_READ_TIMEOUT);
    
    } else {
      temp1 = String.valueOf(DataBrowserPreferences.getConnectionMRUHListLimit());
      temp2 = String.valueOf(DataBrowserPreferences.getConnectionTimeoutInterval());
      readTimeoutStr = String.valueOf(DataBrowserPreferences.getSocketReadTimeoutInterval());
    }
    
    txtMru.setText(temp1);
    txtTimeout.setText(temp2);
    socketReadTimeout.setText(readTimeoutStr);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.preference.PreferencePage#setTitle(java.lang.String)
   */
  @Override
  public void setTitle(String title) {
    super.setTitle("Connection Options");
  }

  // ----------------------------------------------------------------------------
  // IDataBrowserPrefsPage methods
  // ----------------------------------------------------------------------------

  public String getID() {
    return "Connection";
  }

  public String getLabel() {
    return "Connection";
  }

  public ImageDescriptor getImageDescriptor() {
    final String fqnImage_ = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/QueryPrefsPageIcon.png";
    InputStream isImage = null;
    try {
      isImage = getClass().getResourceAsStream(fqnImage_);

      if (null != isImage) {
        img_ = new Image(null, isImage);
        imgDesc_ = ImageDescriptor.createFromImage(img_);
      }
    } catch (NullPointerException xptn) {
      // handler for getResourceAsStream
      LogUtil.warning( "NullPointedException in QueryPrefsPage.getImageDescriptor (getResourceAsStream). Continuing...", xptn );
    } catch (SWTException xptn) {
      // handler for org.eclipse.swt.graphics.Image ctor
      // we continue an try to add the other nodes
      LogUtil.warning( "SWTException in QueryPrefsPage.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). Continuing...", xptn );
    } catch (SWTError err) {
      // Log this (Image ctor could throw this), and rethrow
      LogUtil.error( "SWTError in QueryPrefsPage.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). This could be due to handles in the underlying widget kit being exhaused. Terminating.", err );
      throw err;
    }
    finally {
      if( null != isImage ) {
        try {
          isImage.close();
        } catch (IOException e) {
          LogUtil.warning( "IOException in QueryPrefsPage.getImageDescriptor (isImage.close(..)). Ignoring.", e );
        }
        isImage = null;
      }
    }

    return imgDesc_;
  }
}
