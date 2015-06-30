/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.InputStream;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;

/**
 * @author mghosh
 *
 */
public class SplashScreen {

  private final static String           fqnSplashImage_  = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/splash.bmp";
  boolean                               fDoneWithSplash_ = false;
  private SplashScreenProgressCallback  callback_        = null;

  private Image         image_           = null;
  private ProgressBar   progBar_         = null;
  private Label         label_           = null;
  private Shell         shell_           = null;
  private Display       display_         = null;


  /**
	 *
	 */
  public SplashScreen(Display dsply, SplashScreenProgressCallback clbkObj) {
    this.callback_ = clbkObj;
    init(dsply);
  }


  /* (non-Javadoc)
   * @see java.lang.Object#finalize()
   */
  @Override
  protected void finalize() throws Throwable {
    // MGH - Being paranoid; just in case it does not get cleaned up
    if( null != image_ ) {
      image_.dispose();
    }

    super.finalize();
  }




  private void init(Display dsply) {
    display_ = dsply;

    // TODO MGH:read the image from some resource bundle or property.
    // ImageDescriptor id = new ImageDescriptor( );
    InputStream isImage = getClass().getResourceAsStream(
        fqnSplashImage_);
    if (null != isImage) {
      image_ = new Image(display_, isImage);
    }

    shell_ = new Shell(SWT.ON_TOP);
    Rectangle rcBounds = null;

    // TODO MGH: Should the window be bound to the image size with a max?.
    // Currently it is bound to the image size
    if (null != image_) {
      rcBounds = image_.getBounds();
    } else {
      // TODO MGH - set these to some sane default
      rcBounds = new Rectangle(0, 0, 350, 250);
    }

    shell_.setBounds(0, 0, rcBounds.width, rcBounds.height + 20); // -- 20

    rcBounds = this.shell_.getBounds();
    label_ = new Label(this.shell_, SWT.None);
    label_.setBounds(0, 0, rcBounds.width, rcBounds.height - 20);
    label_.setAlignment(SWT.CENTER);
    progBar_ = new ProgressBar(shell_, SWT.NONE);
    progBar_.setBounds(0, rcBounds.height - 20, rcBounds.width, 20);

    label_.setImage(image_);

    display_.asyncExec(new DisplayRunnable(this));
  } // init

  public void show() {
    Rectangle splashRect = shell_.getBounds();
    Rectangle displayRect = display_.getBounds();
    int x = (displayRect.width - splashRect.width) / 2;
    int y = (displayRect.height - splashRect.height) / 2;
    shell_.setLocation(x, y);
    shell_.open();

    while (false == fDoneWithSplash_) {
      if (!display_.readAndDispatch()) {
        display_.sleep();
      }
    }

    shell_.close();
    if (null != image_) {
      image_.dispose();
      image_ = null;
    }
  } // show()

  // --------------------------------------------------------------
  //
  // Internal helper types
  //
  // --------------------------------------------------------------

  // TODO MGH Document this
  private static class DisplayRunnable implements Runnable {
    private final SplashScreen ssParent_;

    DisplayRunnable(SplashScreen ss) {
      this.ssParent_ = ss;
    }

    public void run() {
      Integer[] oPercent = { Integer.valueOf(-1) };
      SplashScreenProgressCallback.ActionCode codeAction = SplashScreenProgressCallback.ActionCode.CONTINUE;
      do {
        if (null != ssParent_.callback_)
          codeAction = ssParent_.callback_.handleProgress(oPercent);

        int iProgLevel = (-1 != oPercent[0].intValue()) ? oPercent[0]
            .intValue() : 0;
        ssParent_.progBar_.setSelection(iProgLevel);
      } while (SplashScreenProgressCallback.ActionCode.CONTINUE == codeAction);

      ssParent_.fDoneWithSplash_ = true;
    } // run
  } // END: private static class DisplayRunnable implements Runnable

}
