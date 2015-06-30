/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
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
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Widget;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DefaultPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mjha
 *
 */
public class MiscellaneousPrefsPage extends PreferencePage implements IDataBrowserPrefsPage {
  private ImageDescriptor       imgDesc_;
  private Image                 img_;
  private Composite             parent_;
  private Composite             basePane_;
  private Group                 grp1_;
  private Group                 grp2_;
  private Group                 grp3_;
  private Text                  logDirectory;
  private Combo                 logLevelCombo;
  private Text                  logFileSize;
  private Text                  logFileCount;
  private Text                  timeIntervalText;
  private List                  jarList;
  /**
   *
   */
  public MiscellaneousPrefsPage() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param title
   */
  public MiscellaneousPrefsPage(String title) {
    super(title);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param title
   * @param image
   */
  public MiscellaneousPrefsPage(String title, ImageDescriptor image) {
    super(title, image);
    // TODO Auto-generated constructor stub
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


  /* (non-Javadoc)
   * @see org.eclipse.jface.preference.PreferencePage#createContents(org.eclipse.swt.widgets.Composite)
   */
  @Override
  protected Control createContents(Composite parent) {
    parent_ = parent;
    basePane_ = new Composite(parent_, SWT.NONE);

    GridLayout lytBase = new GridLayout();
    lytBase.marginHeight = 5;
    lytBase.marginWidth = 5;
    lytBase.verticalSpacing = 30;
    basePane_.setLayout(lytBase);

    grp1_ = new Group(basePane_, SWT.SHADOW_ETCHED_IN);
    grp1_.setText("Logging");
    grp1_.setLayoutData(new GridData(SWT.FILL, SWT.BEGINNING, true, false));
    GridLayout lytGridGrp1 = new GridLayout();
    lytGridGrp1.numColumns = 4;
    lytGridGrp1.makeColumnsEqualWidth = true;
    lytGridGrp1.marginHeight =20;
    grp1_.setLayout(lytGridGrp1);

    createLogPane(grp1_);

    grp2_ = new Group(basePane_, SWT.SHADOW_ETCHED_IN);
    grp2_.setLayoutData(new GridData(SWT.FILL, SWT.BEGINNING, true, false));
    grp2_.setText("DS Update");

    GridLayout lytGridGrp2 = new GridLayout();
    lytGridGrp2.numColumns = 2;
    lytGridGrp2.makeColumnsEqualWidth = false;
    lytGridGrp2.marginHeight =20;
    grp2_.setLayout(lytGridGrp2);

    createDsInformationPane(grp2_);


    grp3_ = new Group(basePane_, SWT.SHADOW_ETCHED_IN);
    grp3_.setLayoutData(new GridData(SWT.FILL, SWT.BEGINNING, true, false));
    grp3_.setText("Add Application Class Classpath");

    GridLayout lytGridGrp3 = new GridLayout();
    lytGridGrp3.numColumns = 3;
    lytGridGrp3.makeColumnsEqualWidth = true;
    lytGridGrp3.marginHeight =20;
    grp3_.setLayout(lytGridGrp3);

    createApplicationClassPane(grp3_);

    setPreferences(false);

    return basePane_;
  }

  private void createLogPane(final Composite parent){
    GridData gd = null;
    
    StringBuffer logUpdateInfo = new StringBuffer();
    logUpdateInfo.append("\nAny changes in logging configuration would only be effective after DataBrowser restart.\n");
    
    Label logLabel = new Label(parent, SWT.NONE);
    logLabel.setText("Log File Directory");
    gd = new GridData(SWT.BEGINNING,
        GridData.VERTICAL_ALIGN_BEGINNING, false, false);
    logLabel.setLayoutData(gd);

    logDirectory = new Text(parent, SWT.BORDER);

    gd = new GridData(SWT.FILL, GridData.VERTICAL_ALIGN_BEGINNING, true,
        false);
    gd.horizontalSpan = 2;
    gd.widthHint = 100;
    logDirectory.setLayoutData(gd);
    logDirectory.setEditable(false);
    logDirectory.setToolTipText("Select the directory where the log files will be stored.");

    Button changeButton = new Button(parent,SWT.PUSH);
    changeButton.setText("Browse...");
    gd = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_BEGINNING, false,
        false);
    changeButton.setLayoutData(gd);

    changeButton.addSelectionListener(new SelectionListener() {

      public void widgetDefaultSelected(SelectionEvent e) {
      }

      public void widgetSelected(SelectionEvent e) {

        DirectoryDialog directoryDialog = new DirectoryDialog(
            parent.getShell());
        // TODO get filter path from pref
//        directoryDialog.setFilterPath(selectedDir);
        directoryDialog.setMessage("Please select a directory and click OK");

        String dir = directoryDialog.open();
        if (dir != null) {
          java.io.File newLogDir = new java.io.File(dir);
          if (newLogDir.canWrite()) {
            logDirectory.setText(dir);
            logDirectory.setToolTipText(dir);
          } else {
            MessageBox mb = new MessageBox(parent.getShell(), SWT.OK);
            mb.setText("GemFire DataBrowser Warning");
            mb.setMessage("User permissions do not allow\n writes to this directory");
          }
        }
      }
    });

    Label logFileSizeLabel = new Label(parent, SWT.NONE);
    logFileSizeLabel.setText("Maximum log file size (MB)");
    gd = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_BEGINNING, false, false);
    logFileSizeLabel.setLayoutData(gd);
    
    this.logFileSize = new Text(parent, SWT.BORDER);
    gd = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_BEGINNING, false, false);
    gd.widthHint = 100;
    gd.horizontalSpan = 3;
    logFileSize.setLayoutData(gd);
    logFileSize.setEditable(true);
    logFileSize.setToolTipText("Set the maximum log file size in MB. Once the log file reaches this size, it is rolled over, renamed, and a new log file is created.");
    logFileSize.addModifyListener(new PrefModifyListenerAdapter(this));
    
    Label logFileCountLabel = new Label(parent, SWT.NONE);
    logFileCountLabel.setText("Number of rolled over \nlog files to store");
    gd = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_BEGINNING, false, false);
    gd.verticalSpan = 2;
    logFileCountLabel.setLayoutData(gd);

    logFileCount = new Text(parent, SWT.BORDER);
    gd = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_END, false, false);
    gd.widthHint = 100;
    gd.horizontalSpan = 3;
    gd.verticalSpan = 2;
    logFileCount.setLayoutData(gd);
    logFileCount.setEditable(true);
    logFileCount.setToolTipText("Maximum number of rolled over log files to store. Once this limit is reached, the oldest log file is deleted.");
    logFileCount.addModifyListener(new PrefModifyListenerAdapter(this)); 
    
    Label loggingLabel = new Label(parent, SWT.NONE);
    loggingLabel
        .setToolTipText("Logging level for DataBrowser");
    loggingLabel.setText("Logging level: ");
    gd = new GridData(SWT.BEGINNING,
        GridData.VERTICAL_ALIGN_BEGINNING, false, false);
    loggingLabel.setLayoutData(gd);

    logLevelCombo = new Combo(parent, SWT.DROP_DOWN | SWT.READ_ONLY);
    gd = new GridData(SWT.FILL, GridData.VERTICAL_ALIGN_BEGINNING, true,
        false);
    logLevelCombo.setLayoutData(gd);
    logLevelCombo.setToolTipText("Select the logging level");

    String[] allLevels = LogUtil.getLoglevelStrings();
    for (int i = 0; i < allLevels.length; i++) {
      logLevelCombo.add(allLevels[i]);
    }
    
    FontData[] data = parent.getFont().getFontData();
    for(int i =0 ; i < data.length ; i++) {
      int style = data[i].getStyle();
      data[i].setStyle(style | SWT.ITALIC); 
    }
    Font font = new Font(parent.getDisplay(), data);      
    
    Label updateInfoLabel = new Label(parent, SWT.NONE);
    updateInfoLabel.setFont(font);
    updateInfoLabel.setText(logUpdateInfo.toString());
    gd = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_BEGINNING, true, false);
    gd.horizontalSpan = 4;
    gd.verticalSpan = 3;
    updateInfoLabel.setLayoutData(gd);

    
  }

  private void createDsInformationPane(Composite parent) {
    Label timeIntervalLabel = new Label(parent, SWT.NONE);
    timeIntervalLabel.setText( "GemFire System View Refresh Interval (ms): " );
    GridData gd = new GridData(SWT.BEGINNING,
        GridData.VERTICAL_ALIGN_BEGINNING, false, false);
    timeIntervalLabel.setLayoutData(gd);

    timeIntervalText = new Text(parent, SWT.BORDER);
    gd = new GridData(SWT.BEGINNING, GridData.VERTICAL_ALIGN_BEGINNING, true,
        false);
    gd.widthHint = 100;
    timeIntervalText.setLayoutData(gd);
    timeIntervalText.setEditable(true);
    timeIntervalText.setToolTipText( "Refresh time interval to update system members in milliseconds" );
    
    timeIntervalText.addModifyListener(new PrefModifyListenerAdapter(this));    
  }

  private void createApplicationClassPane(final Composite parent){
    jarList = new List(parent, SWT.BORDER | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    GridData gridData = new GridData(SWT.FILL, SWT.FILL, true, true);
    gridData.horizontalSpan = 2;
    gridData.verticalSpan = 6;
    gridData.widthHint = 300;
    gridData.heightHint = 75;
    jarList.setLayoutData(gridData);

    Button jarButton = new Button(parent, SWT.PUSH);
    gridData = new GridData(SWT.FILL, SWT.FILL, true, true);
    jarButton.setLayoutData(gridData);
    jarButton.setText("Add Jar...");


    final Button removeButton = new Button(parent, SWT.PUSH);
    gridData = new GridData(SWT.FILL, SWT.FILL, true, true);
    removeButton.setLayoutData(gridData);
    removeButton.setText("Remove");
    removeButton.setEnabled(false);
    removeButton.addSelectionListener(new SelectionListener(){

      public void widgetDefaultSelected(SelectionEvent e) {
        widgetSelected(e);
      }

      public void widgetSelected(SelectionEvent e) {
        int[] selectionIndices = jarList.getSelectionIndices();
        jarList.remove(selectionIndices);
        removeButton.setEnabled(false);
      }

    });


    jarList.addSelectionListener(new SelectionListener(){

      public void widgetDefaultSelected(SelectionEvent e) {
        widgetSelected(e);
      }

      public void widgetSelected(SelectionEvent e) {
        int[] selectionIndices = jarList.getSelectionIndices();
        boolean enable = (selectionIndices.length != 0)? true: false;
        removeButton.setEnabled(enable);
      }

    });

    jarButton.addSelectionListener(new SelectionListener() {

      public void widgetDefaultSelected(SelectionEvent e) {
        widgetSelected(e);
      }

      public void widgetSelected(SelectionEvent e) {
        Widget w = e.widget;
        FileDialog dlgBrowse = new FileDialog(w.getDisplay().getActiveShell(),
            SWT.MULTI);

        String sCurrFQN = null;
        dlgBrowse.setFileName(sCurrFQN);
        dlgBrowse.setFilterExtensions(new String[] { "*.jar" });
        String sPluginJar = dlgBrowse.open();
        if (sPluginJar != null) {
          jarList.add(sPluginJar);
        }
      }
    });
  }




  /* (non-Javadoc)
   * @see org.eclipse.jface.preference.PreferencePage#performHelp()
   */
  @Override
  public void performHelp() {
    // TODO Manish - hook to display Help window with relevant information
  }

  /* (non-Javadoc)
   * @see org.eclipse.jface.preference.PreferencePage#performApply()
   */
  @Override
  protected void performApply() {
    String[] jars = jarList.getItems();
    String logDir = logDirectory.getText();
    String logFileSize = this.logFileSize.getText();
    String logFileCount = this.logFileCount.getText();
    String logLevel = logLevelCombo.getText();
    String timeIntervalStr = timeIntervalText.getText();

    
    long timeInterval;
    try {
      timeInterval = Integer.valueOf(timeIntervalStr);
    }
    catch (NumberFormatException e) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("The refresh interval entered is not valid. \n Please enter a valid ( greater than zero ) number."); 
      mb.open();
      timeIntervalText.setFocus();
      return;
    }
    
    if(timeInterval <= 0) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("The refresh interval entered is not valid. \n Please enter a valid ( greater than zero ) number."); 
      mb.open();
      this.timeIntervalText.setFocus();
      return;
    }
    
    
    int logfilesize = 0;
    try {
      logfilesize = Integer.valueOf(logFileSize);      
    } catch (NumberFormatException e) {
      logfilesize = 0;
    }
    
    if(logfilesize <= 0) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Enter a valid size (greater than zero) as the maximum size of the log file in MB"); 
      mb.open();
      this.logFileSize.setFocus();
      return;
    }
    
    int logfilecount = 0;
    try {
      logfilecount = Integer.valueOf(logFileCount);      
    } catch (NumberFormatException e) {
      logfilecount = 0;
    }
    
    if(logfilecount <= 0) {
      MessageBox mb = new MessageBox(getShell());
      mb.setText("DataBrowser Error");
      mb.setMessage("Enter the maximum number of times the log filed is rolled over. Once this count is reached, the oldest log file is deleted."); 
      mb.open();
      this.logFileCount.setFocus();
      return;
    }
    
    if(logDir.length() != 0)
      DataBrowserPreferences.setLogDirectory(logDir);
    DataBrowserPreferences.setLogFileSize(logfilesize);
    DataBrowserPreferences.setLogFileCount(logfilecount);
    DataBrowserPreferences.setLoggingLevel(logLevel);
    DataBrowserPreferences.setDSRefreshInterval(timeInterval);
    DataBrowserPreferences.setApplicationClasses(jars);

    DataBrowserController controller = DataBrowserApp.getInstance()
        .getController();
    try {
      controller.setApplicationClassJars(jars);
    } catch (Exception e) {
      LogUtil.warning("Exception during loading application classes", e);
    }
    
    controller.setConnectionRefreshInterval(timeInterval);
  }
  
  @Override
  public boolean performOk() {
    performApply();
    return super.performOk();
  }
  
  public boolean isValid() {
    String logFileSize = this.logFileSize.getText();
    String logFileCount = this.logFileCount.getText();
    String timeIntervalStr = timeIntervalText.getText();
    
    int temp;
    
    try {
      temp = Integer.valueOf(logFileSize);
    }catch (NumberFormatException ex) {
      temp = -1;
    }
    
    if(temp <= 0) {
      setErrorMessage("Enter a valid size (greater than zero) as the maximum size of the log file in MB");
      this.logFileCount.setEnabled(false);
      this.timeIntervalText.setEnabled(false);
      return false;
    }
    
    
    try {
      temp = Integer.valueOf(logFileCount);
    }catch (NumberFormatException ex) {
      temp = -1;
    }
    
    if(temp <= 0) {
      setErrorMessage("Enter a valid count (greater than zero) as the maximum number of times the log filed is rolled over");
      this.logFileSize.setEnabled(false);
      this.timeIntervalText.setEnabled(false);        
      return false;
    }
    
    long timeInterval;
    try {
      timeInterval = Integer.valueOf(timeIntervalStr);
    }catch (NumberFormatException ex) {
      timeInterval = -1;
    }
    
    if(timeInterval <= 0) {
      setErrorMessage("The refresh interval entered is not valid. Please enter a valid ( greater than zero ) number.");
      this.logFileSize.setEnabled(false);
      this.logFileCount.setEnabled(false);
      return false;
    }

    setErrorMessage(null);
    this.logFileSize.setEnabled(true);
    this.logFileCount.setEnabled(true);
    this.timeIntervalText.setEnabled(true);   
    
    return true; 
  }
  

  /* (non-Javadoc)
   * @see org.eclipse.jface.preference.PreferencePage#performDefaults()
   */
  @Override
  protected void performDefaults() {
    setPreferences(true);
    super.performDefaults();
  }

  private void setPreferences(boolean isDefault){
    String temp1;
    String temp2;
    String temp3;
    String temp4;
    String temp5;
    String[] temp6;   
    
    if(isDefault) {
      temp1 = DefaultPreferences.DEFAULT_LOG_DIR;
      temp2 = DefaultPreferences.DEFAULT_LOGGING_LEVEL;
      temp3 = String.valueOf(DefaultPreferences.DEFAULT_LOG_FILE_SIZE);
      temp4 = String.valueOf(DefaultPreferences.DEFAULT_LOG_FILE_COUNT);
      temp5 = String.valueOf(DefaultPreferences.DEFAULT_DS_REFRESH_INTERVAL);
      temp6 = DefaultPreferences.DEFAULT_APPLICATION_CLASSES;   
      
    } else {
      temp1 = DataBrowserPreferences.getLogDirectoryPref();
      temp2 = DataBrowserPreferences.getLoggingLevelPref();
      temp3 = String.valueOf(DataBrowserPreferences.getLogFileSizePref());
      temp4 = String.valueOf(DataBrowserPreferences.getLogFileCountPref());
      temp5 = String.valueOf(DataBrowserPreferences.getDSRefreshInterval());
      temp6 = DataBrowserPreferences.getApplicationClasses();
    }
    
    logDirectory.setText(temp1);
    logLevelCombo.setText(temp2);
    logFileSize.setText(temp3);
    logFileCount.setText(temp4);
    timeIntervalText.setText(temp5);
    jarList.setItems(temp6);
  }

  /* (non-Javadoc)
   * @see org.eclipse.jface.preference.PreferencePage#setTitle(java.lang.String)
   */
  @Override
  public void setTitle(String title) {
    super.setTitle("Miscellaneous");
  }



  /* (non-Javadoc)
   * @see org.eclipse.jface.preference.PreferencePage#createControl(org.eclipse.swt.widgets.Composite)
   */
  @Override
  public void createControl(Composite parent) {
    super.createControl(parent);
  }


  // ----------------------------------------------------------------------------
  // IDataBrowserPrefsPage methods
  // ----------------------------------------------------------------------------

  public String getID() {
    return "Miscellaneous";
  }

  public String getLabel() {
    return "Miscellaneous";
  }

  public ImageDescriptor getImageDescriptor() {
    final String fqnImage_ = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/MiscellaneousPrefsPageIcon.png";
    InputStream isImage = null;
    try {
      isImage = getClass().getResourceAsStream(fqnImage_);

      if (null != isImage) {
        img_ = new Image(null, isImage);
        imgDesc_ = ImageDescriptor.createFromImage(img_);
      }
    } catch (NullPointerException xptn) {
      // handler for getResourceAsStream
      LogUtil.warning( "NullPointedException in MiscellaneousPrefsPage.getImageDescriptor (getResourceAsStream). Continuing...", xptn );
    } catch (SWTException xptn) {
      // handler for org.eclipse.swt.graphics.Image ctor
      // we continue an try to add the other nodes
      LogUtil.warning( "SWTException in MiscellaneousPrefsPage.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). Continuing...", xptn );
    } catch (SWTError err) {
      // Log this (Image ctor could throw this), and rethrow
      LogUtil.error( "SWTError in MiscellaneousPrefsPage.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). This could be due to handles in the underlying widget kit being exhaused. Terminating.", err );
      throw err;
    }
    finally {
      if( null != isImage ) {
        try {
          isImage.close();
        } catch (IOException e) {
          LogUtil.warning( "IOException in MiscellaneousPrefsPage.getImageDescriptor (isImage.close(..)). Ignoring.", e );
        }
        isImage = null;
      }
    }

    return imgDesc_;
  }
}
