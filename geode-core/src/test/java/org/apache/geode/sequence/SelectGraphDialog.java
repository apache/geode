/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.sequence;

import org.apache.geode.internal.sequencelog.model.GraphID;
import org.apache.geode.internal.sequencelog.model.GraphSet;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.*;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: dsmith
 * Date: Dec 9, 2010
 * Time: 3:34:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class SelectGraphDialog extends JDialog {
    private List<GraphID> selectedIds = new ArrayList<GraphID>();
    private Set<SelectionListener> listeners = new HashSet<SelectionListener>();

    public SelectGraphDialog(final GraphSet graphs) {
        
        final List<GraphID> ids = new ArrayList<GraphID>(graphs.getMap().keySet());
        Collections.sort(ids);
        final FilterableListModel listModel = new FilterableListModel(ids);
        final JList list = new JList(listModel);

        JScrollPane selectGraphPane = new JScrollPane(list);
        selectGraphPane.setPreferredSize(new Dimension(500, 500));

        JButton apply = new JButton("Apply");
        apply.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                selectedIds = (List) Arrays.asList(list.getSelectedValues());
                fireSelectionChanged();
                setVisible(false);
            }
        });

        JButton cancel= new JButton("Cancel");
        cancel.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                setVisible(false);
            }
        });

        JPanel buttonPane = new JPanel();
        buttonPane.setLayout(new BoxLayout(buttonPane, BoxLayout.LINE_AXIS));
        buttonPane.setBorder(BorderFactory.createEmptyBorder(0, 10, 10, 10));
        buttonPane.add(Box.createHorizontalGlue());
        buttonPane.add(apply);
        buttonPane.add(cancel);
        
        final JTextField searchField = new JTextField(10);
        searchField.getDocument().addDocumentListener(new DocumentListener() {
          public void removeUpdate(DocumentEvent e) {
            doUpdate();
          }
          
          public void insertUpdate(DocumentEvent e) {
            doUpdate();
          }
          
          public void changedUpdate(DocumentEvent e) {
            doUpdate();
          }
          
          private void doUpdate() {
            listModel.updateFilter(searchField.getText());
          }
        });
        

        Container contentPane = getContentPane();
        contentPane.add(searchField, BorderLayout.PAGE_START);
        contentPane.add(selectGraphPane, BorderLayout.CENTER);
        contentPane.add(buttonPane, BorderLayout.PAGE_END);
    }

    private void fireSelectionChanged() {
        for(SelectionListener listener : listeners) {
            listener.selectionChanged(selectedIds);
        }
    }

    public void addSelectionListener(SelectionListener listener) {
        listeners.add(listener);

    }

    public void removeSelectionListener(SelectionListener listener) {
        listeners.remove(listener);
    }

    /**
     * A listener for changes to the graph selections
     */
    public static interface SelectionListener {
        void selectionChanged(List<GraphID> selectedIds);
    }
    
    private static class FilterableListModel extends AbstractListModel {
      private final List<?> allElements;
      private List<Object> filteredElements;
      
      public FilterableListModel(List<?> elements) {
        this.allElements = elements;
        this.filteredElements = new ArrayList<Object>(elements);
      }

      public int getSize() {
        return filteredElements.size();
      }

      public Object getElementAt(int index) {
        return filteredElements.get(index);
      }
      
      public void updateFilter(String filter) {
        Pattern pattern = Pattern.compile(filter);
        filteredElements = new ArrayList<Object>();
        for(Object element : allElements) {
          if(pattern.matcher(element.toString()).find()) {
            filteredElements.add(element);
          }
        }
        
        fireContentsChanged(this, 0, filteredElements.size());
      }
      
    }
}
