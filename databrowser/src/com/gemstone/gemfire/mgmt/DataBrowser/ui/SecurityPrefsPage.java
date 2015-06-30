/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite.Data;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite.SecurityProp;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 *
 */
public class SecurityPrefsPage extends PreferencePage implements IDataBrowserPrefsPage {

  private Composite parent_ = null;
  private Composite basePane_ = null;
  private Image img_;
  private ImageDescriptor imgDesc_;
  private SecurityPropComposite securityPropComposite_;

  /**
   *
   */
  public SecurityPrefsPage() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param title
   */
  public SecurityPrefsPage(String title) {
    super(title);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param title
   * @param image
   */
  public SecurityPrefsPage(String title, ImageDescriptor image) {
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
  protected Control createContents(Composite prnt) {
    parent_ = prnt;
    basePane_ = new Composite( parent_, SWT.NONE );

    FillLayout lytBase = new FillLayout();
    lytBase.type = SWT.VERTICAL;
    lytBase.marginHeight = 5;
    lytBase.marginWidth = 5;
    basePane_.setLayout( lytBase );

    securityPropComposite_ = new SecurityPropComposite( basePane_, SWT.NONE );

    return basePane_;
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
    securityPropComposite_.populateData();
    Data data = securityPropComposite_.getSecurityPropsData();
    String plugin = data.getSecurityPlugin();
    List<SecurityProp> props = data.getSecurityProperties();

    DataBrowserPreferences.setSecurityPlugin(plugin);
    DataBrowserPreferences.setSecurityProperties(props);
    boolean hidden = securityPropComposite_.isHidden();
    DataBrowserPreferences.setSecurityPropsHidden(hidden);
  }
  
  @Override
  public boolean performOk() {
    performApply();
    return super.performOk();
  }

  /* (non-Javadoc)
   * @see org.eclipse.jface.preference.PreferencePage#performDefaults()
   */
  @Override
  protected void performDefaults() {
    securityPropComposite_.setDefaultSecurityProp();
    super.performDefaults();
  }

  /* (non-Javadoc)
   * @see org.eclipse.jface.preference.PreferencePage#setTitle(java.lang.String)
   */
  @Override
  public void setTitle(String title) {
    super.setTitle("Security Options");
  }


  // ----------------------------------------------------------------------------
  // IDataBrowserPrefsPage methods
  // ----------------------------------------------------------------------------

  public String getID() {
    return "Security";
  }

  public String getLabel() {
    return "Security";
  }

  public ImageDescriptor getImageDescriptor() {
    final String fqnImage_ = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/SecurityPrefsPageIcon.png";
    InputStream isImage = null;
    try {
      isImage = getClass().getResourceAsStream(fqnImage_);

      if (null != isImage) {
        img_ = new Image(null, isImage);
        imgDesc_ = ImageDescriptor.createFromImage(img_);
      }
    } catch (NullPointerException xptn) {
      // handler for getResourceAsStream
      LogUtil.warning( "NullPointedException in SecurityPrefsPage.getImageDescriptor (getResourceAsStream). Continuing...", xptn );
    } catch (SWTException xptn) {
      // handler for org.eclipse.swt.graphics.Image ctor
      // we continue an try to add the other nodes
      LogUtil.warning( "SWTException in SecurityPrefsPage.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). Continuing...", xptn );
    } catch (SWTError err) {
      // Log this (Image ctor could throw this), and rethrow
      LogUtil.error( "SWTError in SecurityPrefsPage.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). This could be due to handles in the underlying widget kit being exhaused. Terminating.", err );
      throw err;
    }
    finally {
      if( null != isImage ) {
        try {
          isImage.close();
        } catch (IOException e) {
          // TODO MGH - Log and continue
          LogUtil.warning( "IOException in SecurityPrefsPage.getImageDescriptor (isImage.close(..)). Ignoring.", e );
        }
        isImage = null;
      }
    }

    return imgDesc_;
  }


  /**
   * @param args
   *
   * Test hook
   */
  static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
