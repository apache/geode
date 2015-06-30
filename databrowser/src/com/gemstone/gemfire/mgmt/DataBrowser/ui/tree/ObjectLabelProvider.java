package com.gemstone.gemfire.mgmt.DataBrowser.ui.tree;

import org.eclipse.jface.viewers.ColumnLabelProvider;
import org.eclipse.swt.graphics.Image;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;

public class ObjectLabelProvider extends ColumnLabelProvider {

  private int index;

  public ObjectLabelProvider(int idx) {
    index = idx;
  }

  @Override
  public Image getImage(Object element) {
    return null;
  }

  @Override
  public String getText(Object element) {
    String sRet = "Unknown type";
    
    if(element instanceof ObjectImage) {
      ObjectImage temp = (ObjectImage)element;

      switch(index) {
        case 0: { 
          sRet = temp.getName();
          break;
        }
  
        case 2: {
          sRet = temp.getTypeName();
          break;
        }
        
        // TODO : MGH - check the logic for return value ("null" vs empty string)
        case 1: { 
          if(temp.getType() == IntrospectionResult.PRIMITIVE_TYPE_RESULT)
            sRet = (temp.getValue() != null) ? temp.getValue().toString() : "null";
          else
            sRet = "";
          break;
        }
          
        default: {
          sRet = "Unknown type";
          break;
        }
      }
    }   

    return sRet;
  }

  @Override
  public void dispose() {
  }
}
