/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.app;

import java.util.ArrayList;
import java.util.Collection;

import com.gemstone.gemfire.mgmt.DataBrowser.controller.DSSnapShot;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;

/**
 * @author mghosh
 *
 */
public class State {

  static private State           stateInst_      = new State();

  private Collection<DSSnapShot> distribSystems_ = new ArrayList<DSSnapShot>();

  private DSSnapShot             activeDS_       = null;

  /**
   *
   */
  private State() {
    // TODO Auto-generated constructor stub
  }

  static State create() {
    return State.stateInst_;
  }

  public final Collection<DSSnapShot> getDSs() {
    return distribSystems_;
  }

  public void setDSs(Collection<DSSnapShot> dss) {
    distribSystems_ = dss;
  }

  public void addDS(DSSnapShot ds) {
    distribSystems_.add(ds);
  }

  public boolean switchToDS(DSSnapShot ds) {
    boolean fRet = false;
    if (true == distribSystems_.contains(ds)) {
      activeDS_ = ds;
      fRet = true;
    }

    return fRet;
  }

  public boolean hasConnectionToSomeDS() {
    /*
     * boolean fRet = false;
     *
     * if (distribSystems_.isEmpty()) return fRet;
     *
     * for (DSSnapShot ds : distribSystems_) { if (ds.isConnected()) { fRet =
     * true; } }
     *
     * return fRet;
     */
    DataBrowserApp app = DataBrowserApp.getInstance();
    boolean fRet = false;
    if (null != app) {
      DataBrowserController ctrlr = app.getController();
      if (null != ctrlr) {
        fRet = ctrlr.hasConnection();
      }
    }

    return fRet;
  }

  public void disconnect(DSSnapShot ds) {
    /*
     * ds.disconnect();
     */
    DataBrowserApp app = DataBrowserApp.getInstance();
    if (null != app) {
      DataBrowserController ctrlr = app.getController();
      if (null != ctrlr) {
        ctrlr.disconnect();
      }
    }

  }

  public DSSnapShot disconnect() {
    DataBrowserApp app = DataBrowserApp.getInstance();
    DSSnapShot dsss = null;
    if (null != app) {
      DataBrowserController ctrlr = app.getController();
      if (null != ctrlr) {
        ctrlr.disconnect();

        dsss = activeDS_;
        if( null != activeDS_  ) {
          ArrayList< DSSnapShot > lstDS = ( ArrayList< DSSnapShot > )this.distribSystems_;
          int idxNextActiveDS = lstDS.indexOf( activeDS_);
          if( idxNextActiveDS == ( lstDS.size() - 1 )) {
            idxNextActiveDS = 0;
          }
          else {
            idxNextActiveDS++;
          }

          this.distribSystems_.remove( dsss );

          try {
            activeDS_ = lstDS.get( idxNextActiveDS );
          }
          catch( IndexOutOfBoundsException xtpn ) {
            if( lstDS.size() > 0 ) {
              activeDS_ = lstDS.get( 0 );
            }
            else {
              activeDS_ = null;
            }
          }

        }
      }
    }

    return dsss;
  }

  public DSSnapShot getCurrDS() {
    return activeDS_;
  }

  // ------------------------
  // for initial testing before we develop the back-end portions.
  protected static class DummyState extends State {
    int iCount = 0;

    DummyState(int iCnt) {
      init(iCnt);
    }

    private void init(int iCnt) {
      // I am commenting this out, since I have removed setName operation from
      // DistributedSystem,
      // Application is not supposed to set any name here. We will pickup the
      // name from GFE itself.
      // - Hrishikesh Gadre.
      /*
       * iCount = iCnt; Collection<DistributedSystem> colDss = new
       * ArrayList<DistributedSystem>(); for (int i = 0; i < iCnt; i++) {
       * DistributedSystem ds = new DistributedSystem();
       * ds.setName("DummyData_DS_" + i);
       *
       * Collection<DSMember> collDSM = new ArrayList<DSMember>(); for (int j =
       * 0; j < iCnt; j++) { DSMember dsm = new DSMember();
       * dsm.setName(ds.getName() + "_Mmbr_" + j);
       *  dsm.setDS(ds); Collection<Region> collRgns = new
       * ArrayList<Region>(); for (int k = 0; k < iCnt; k++) { Region rgn = new
       * Region(); rgn.setName(dsm.getName() + "_Rgn_" +
       * k); rgn.setDsMember(dsm); collRgns.add(rgn); }
       *
       * dsm.setRegions(collRgns);
       *
       * collDSM.add(dsm); }
       *
       * ds.setMembers(collDSM); colDss.add(ds); }
       *
       * setDSs(colDss);
       */
    }
  }
}
