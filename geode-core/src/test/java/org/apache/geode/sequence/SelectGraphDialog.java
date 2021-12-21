/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.sequence;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import javax.swing.AbstractListModel;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.apache.geode.internal.sequencelog.model.GraphID;
import org.apache.geode.internal.sequencelog.model.GraphSet;

/**
 * Created by IntelliJ IDEA. User: dsmith Date: Dec 9, 2010 Time: 3:34:38 PM To change this template
 * use File | Settings | File Templates.
 */
public class SelectGraphDialog extends JDialog {
  private List<GraphID> selectedIds = new ArrayList<>();
  private final Set<SelectionListener> listeners = new HashSet<>();

  public SelectGraphDialog(final GraphSet graphs) {

    final List<GraphID> ids = new ArrayList<>(graphs.getMap().keySet());
    Collections.sort(ids);
    final FilterableListModel listModel = new FilterableListModel(ids);
    final JList list = new JList(listModel);

    JScrollPane selectGraphPane = new JScrollPane(list);
    selectGraphPane.setPreferredSize(new Dimension(500, 500));

    JButton apply = new JButton("Apply");
    apply.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        selectedIds = (List) Arrays.asList(list.getSelectedValues());
        fireSelectionChanged();
        setVisible(false);
      }
    });

    JButton cancel = new JButton("Cancel");
    cancel.addActionListener(new ActionListener() {
      @Override
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
      @Override
      public void removeUpdate(DocumentEvent e) {
        doUpdate();
      }

      @Override
      public void insertUpdate(DocumentEvent e) {
        doUpdate();
      }

      @Override
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
    for (SelectionListener listener : listeners) {
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
  public interface SelectionListener {
    void selectionChanged(List<GraphID> selectedIds);
  }

  private static class FilterableListModel extends AbstractListModel {
    private final List<?> allElements;
    private List<Object> filteredElements;

    public FilterableListModel(List<?> elements) {
      allElements = elements;
      filteredElements = new ArrayList<>(elements);
    }

    @Override
    public int getSize() {
      return filteredElements.size();
    }

    @Override
    public Object getElementAt(int index) {
      return filteredElements.get(index);
    }

    public void updateFilter(String filter) {
      Pattern pattern = Pattern.compile(filter);
      filteredElements = new ArrayList<>();
      for (Object element : allElements) {
        if (pattern.matcher(element.toString()).find()) {
          filteredElements.add(element);
        }
      }

      fireContentsChanged(this, 0, filteredElements.size());
    }

  }
}
