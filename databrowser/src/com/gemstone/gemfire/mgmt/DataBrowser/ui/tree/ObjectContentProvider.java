package com.gemstone.gemfire.mgmt.DataBrowser.ui.tree;

import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.jface.viewers.Viewer;

public class ObjectContentProvider implements ITreeContentProvider {

  private static Object[] EMPTY_ARRAY = new Object[0];
  protected ObjectImage root;
  protected TreeViewer viewer;
 
  public Object[] getChildren(Object parentElement) {
    if(parentElement instanceof ObjectImage) {
     ObjectImage image = (ObjectImage)parentElement;
     return image.getChildren().toArray();
    }
    
    return EMPTY_ARRAY;
  }

  public Object getParent(Object element) {
    if(element instanceof ObjectImage) {
      ObjectImage image = (ObjectImage)element;
      return image.getParent();
      
    }
    return null;
  }

  public boolean hasChildren(Object element) {
    if(element instanceof ObjectImage) {
      ObjectImage image = (ObjectImage)element;
      return !image.getChildren().isEmpty();      
    }  
    
    return false;
  }

  public Object[] getElements(Object inputElement) {
    ObjectImage[] result = new ObjectImage[1];
    result[0] = root;       
    return result;
  }
  
  public void setRoot(ObjectImage rt) {
    this.root = rt;
  }

  public void dispose() {
    
  }

  public void inputChanged(Viewer vwr, Object oldInput, Object newInput) {
    
  }

}
