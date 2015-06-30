/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui.internal;

import org.eclipse.jface.preference.IPreferenceNode;
import org.eclipse.jface.preference.PreferenceManager;
import org.eclipse.jface.preference.PreferenceNode;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.graphics.Image;

import com.gemstone.gemfire.mgmt.DataBrowser.ui.IDataBrowserPrefsPage;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 * 
 */
public class PrefsMgr extends PreferenceManager {

  private NodeData[] ndData_;
  IPreferenceNode[]  nodes_;

  /**
   * 
   */
  public PrefsMgr() {
    init();
  }

  /**
   * @param separatorChar
   */
  public PrefsMgr(char separatorChar) {
    super(separatorChar);
    init();
  }

  /**
   * @param separatorChar
   * @param rootNode
   */
  public PrefsMgr(char separatorChar, PreferenceNode rootNode) {
    super(separatorChar, rootNode);
    init();
  }

  /**
   * @param none
   * 
   *          Add the nodes in the tree of the preference dialog TODO Change
   *          this to get label, id, image descriptor from each of the
   *          preferences pages This is quick / dirty method.
   */
  private void init() {
    updateNodeData();
    
    nodes_ = new IPreferenceNode[4];
    int iNumNodes = ndData_.length;
    for (int i = 0; i < iNumNodes; i++) {
      nodes_[i] = new PreferenceNode(ndData_[i].id_, ndData_[i].label_, ndData_[i].imgDesc_, ndData_[i].clsNameImplementor_ );
      this.getRoot().add(nodes_[i]);
    }
  }

  private void updateNodeData() {
    // TODO MGH - Perhaps these could be written from config?
    String[] pageClsNms = {
        "com.gemstone.gemfire.mgmt.DataBrowser.ui.ConnectionPrefsPage",
        "com.gemstone.gemfire.mgmt.DataBrowser.ui.QueryPrefsPage",
        "com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPrefsPage",
        "com.gemstone.gemfire.mgmt.DataBrowser.ui.MiscellaneousPrefsPage" };

    Class<?> kls = null;
    ndData_ = new NodeData[pageClsNms.length];
    int idxNodeData = 0;
    for (int idxClsNms = 0; idxClsNms < pageClsNms.length; idxClsNms++) {
      try {
        kls = Class.forName(pageClsNms[idxClsNms]);
        IDataBrowserPrefsPage pg = (IDataBrowserPrefsPage) kls.newInstance();

        ndData_[idxNodeData] = new NodeData();
        
        ndData_[idxNodeData].clsNameImplementor_ = pageClsNms[idxClsNms];
        ndData_[idxNodeData].id_ = pg.getID();
        ndData_[idxNodeData].label_ = pg.getLabel();
        ndData_[idxNodeData].imgDesc_ = pg.getImageDescriptor();
        ++idxNodeData;
      } catch (ClassNotFoundException xptn) {

      } catch (InstantiationException e) {
        LogUtil.warning( "InstantiationException in PrefsMgr.updateNodeData(..)\n", e );
      } catch (IllegalAccessException e) {
        LogUtil.warning( "IllegalAccessException in PrefsMgr.updateNodeData(..)\n", e );
      } finally {
        /*
         * ndData_[idxNodeData].clsNameImplementor_ = null;
         * ndData_[idxNodeData].id_ = null; ndData_[idxNodeData].label_ = null;
         * ndData_[idxNodeData].imgDesc_ = null;
         */
      }
    } // for(...)

  }

  private static class NodeData {
    ImageDescriptor imgDesc_;
    String          label_;
    String          id_;
    String          clsNameImplementor_;

    NodeData() {
      imgDesc_ = null;
      label_ = null;
      id_ = null;
      clsNameImplementor_ = null;
    }
  } // NodeData

  /**
   * @param args
   * 
   *          Testing hook
   */
  static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
