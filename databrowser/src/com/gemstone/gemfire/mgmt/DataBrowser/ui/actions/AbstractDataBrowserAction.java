/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.actions;

import java.io.IOException;
import java.io.InputStream;

import org.eclipse.jface.action.Action;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.graphics.Image;

import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 * 
 */
public abstract class AbstractDataBrowserAction extends Action {

  private ImageDescriptor imgDescEnabled_;
  private Image           imgEnabled_;

  private ImageDescriptor imgDescDisabled_;
  private Image           imgDisabled_;

  /**
	 * 
	 */
  public AbstractDataBrowserAction() {
    // TODO Auto-generated constructor stub
  }

  
  public AbstractDataBrowserAction(String text, int style) {
    super(text, style);
    // TODO Auto-generated constructor stub
  }


  /**
   * 
   * Called at runtime to allow derived action classes to provide 
   * their own specific 'enabled' icon.
   * This should be fully qualified, especially if the derived class
   * is defined in a different package
   * 
   * @return The [fully (package) qualified] name of the icon resource to display when the action is enabled.  
   */
  abstract public String getEnabledIcon();
  
  /**
   * 
   * Called at runtime to allow derived action classes to provide 
   * their own specific 'disabled' icon.
   * This should be fully qualified, especially if the derived class
   * is defined in a different package
   * 
   * @return The [fully (package) qualified] name of the icon resource to display when the action is disabled.  
   */
  abstract public String getDisabledIcon();  

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.IAction#getId()
   */
  @Override
  public String getId() {
    return this.getClass().getCanonicalName();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.IAction#getText()
   */
  @Override
  public String getText() {
    return this.getClass().getName();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getDisabledImageDescriptor()
   */
  @Override
  public ImageDescriptor getDisabledImageDescriptor() {
    String fqnImage_ = getDisabledIcon();
    if( null != fqnImage_ && ( 0 != ( fqnImage_ = fqnImage_.trim() ).length() )) {      
      InputStream isImage = null;
      try {
        // MGH - Findbugs points out this as a bad practice, since if the derived class
        //       is in a different package, and the icon's path is not fully qualified, 
        //       an incorrect icon (or none) could get loaded. 
        //       This is addressed by the java doc comments for getDisabledIcon()
        isImage = getClass().getResourceAsStream(fqnImage_);
  
        if (null != isImage) {
          imgDisabled_ = new Image(null, isImage);
          imgDescDisabled_ = ImageDescriptor.createFromImage(imgDisabled_);
        }
      } catch (NullPointerException xptn) {
        // handler for getResourceAsStream
        LogUtil
            .warning(
                "NullPointerException in AbstractDataBrowser.getDisabledImageDescriptor (getResourceAsStream). Continuing...",
                xptn);
      } catch (SWTException xptn) {
        // handler for org.eclipse.swt.graphics.Image ctor
        // we continue an try to add the other nodes
        LogUtil
            .warning(
                "SWTException in AbstractDataBrowser.getDisabledImageDescriptor (org.eclipse.swt.graphics.Image ctor). Continuing...",
                xptn);
      } catch (SWTError err) {
        // Log this (Image ctor could throw this), and rethrow
        LogUtil
            .error(
                "SWTError in AbstractDataBrowser.getDisabledImageDescriptor (org.eclipse.swt.graphics.Image ctor). This could be due to handles in the underlying widget kit being exhaused. Terminating.",
                err);
        throw err;
      } finally {
        if (null != isImage) {
          try {
            isImage.close();
          } catch (IOException e) {
            LogUtil
                .warning(
                    "IOException in AbstractDataBrowser.getDisabledImageDescriptor (isImage.close(..)). Ignoring.",
                    e);
          }
          isImage = null;
        }
      }
    }
  
    return ( null != imgDescDisabled_ ) ? imgDescDisabled_ : super.getDisabledImageDescriptor();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.action.Action#getImageDescriptor()
   */
  @Override
  public ImageDescriptor getImageDescriptor() {
    String fqnImage_ = getEnabledIcon();
    if( null != fqnImage_ && ( 0 != ( fqnImage_ = fqnImage_.trim() ).length() )) {      
      InputStream isImage = null;
      try {
        // MGH - Findbugs points out this as a bad practice, since if the derived class
        //       is in a different package, and the icon's path is not fully qualified, 
        //       an incorrect icon (or none) could get loaded. 
        //       This is addressed by the java doc comments for getEnabledIcon()
        isImage = getClass().getResourceAsStream(fqnImage_);
  
        if (null != isImage) {
          imgEnabled_ = new Image(null, isImage);
          imgDescEnabled_ = ImageDescriptor.createFromImage(imgEnabled_);
        }
      } catch (NullPointerException xptn) {
        // handler for getResourceAsStream
        LogUtil
            .warning(
                "NullPointerException in AbstractDataBrowser.getImageDescriptor (getResourceAsStream). Continuing...",
                xptn);
      } catch (SWTException xptn) {
        // handler for org.eclipse.swt.graphics.Image ctor
        // we continue an try to add the other nodes
        LogUtil
            .warning(
                "SWTException in AbstractDataBrowser.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). Continuing...",
                xptn);
      } catch (SWTError err) {
        // Log this (Image ctor could throw this), and rethrow
        LogUtil
            .error(
                "SWTError in AbstractDataBrowser.getImageDescriptor (org.eclipse.swt.graphics.Image ctor). This could be due to handles in the underlying widget kit being exhaused. Terminating.",
                err);
        throw err;
      } finally {
        if (null != isImage) {
          try {
            isImage.close();
          } catch (IOException e) {
            LogUtil
                .warning(
                    "IOException in AbstractDataBrowser.getImageDescriptor (isImage.close(..)). Ignoring.",
                    e);
          }
          isImage = null;
        }
      }
    }
    
    return ( null != imgDescEnabled_ ) ? imgDescEnabled_ : super.getImageDescriptor();
  }

}
