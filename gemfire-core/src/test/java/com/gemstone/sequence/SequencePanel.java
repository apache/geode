/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.sequence;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;

/**
 * Created by IntelliJ IDEA.
 * User: dan
 * Date: Oct 28, 2010
 * Time: 10:29:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class SequencePanel extends JPanel {

    public SequencePanel(SequenceDiagram sequenceDiagram) {
         //Set up the drawing area.
        ZoomingPanel drawingPane = new ZoomingPanel();
        drawingPane.setBackground(Color.white);
        drawingPane.setSequenceDiagram(sequenceDiagram);

        //Put the drawing area in a scroll pane.
        final JScrollPane scroller = new JScrollPane(drawingPane);
        scroller.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
        scroller.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);
        final TimeAxis timeAxis = new TimeAxis(TimeAxis.VERTICAL, 
                sequenceDiagram.getMinTime(),
                sequenceDiagram.getMaxTime());
        timeAxis.setPreferredHeight(drawingPane.getHeight());
        scroller.setRowHeaderView(timeAxis);
        scroller.setColumnHeaderView(sequenceDiagram.createMemberAxis());
        int preferredWidth = (int) (Toolkit.getDefaultToolkit().getScreenSize().getWidth() - 100);
        int preferredHeight = (int) (Toolkit.getDefaultToolkit().getScreenSize().getHeight() - 100);
        scroller.setPreferredSize(new Dimension(preferredWidth, preferredHeight));
        scroller.setAutoscrolls(true);
//        scroller.setPreferredSize(new Dimension(200,200));

        sequenceDiagram.addComponentListener(new ComponentAdapter() {
            @Override
            public void componentResized(ComponentEvent e) {
                int height = e.getComponent().getHeight();
                timeAxis.setPreferredHeight(height);
                timeAxis.revalidate();
            }
        });

        BorderLayout layout = new BorderLayout();
//        layout.setHgap(0);
//        layout.setVgap(0);
        setLayout(layout);
        //Lay out this demo.
//        add(instructionPanel, BorderLayout.PAGE_START);
        add(scroller, BorderLayout.CENTER);

        addComponentListener(new ComponentAdapter() {
            @Override
            public void componentResized(ComponentEvent e) {
                Component source =e.getComponent();
                scroller.setSize(source.getSize());
                scroller.revalidate();
            }
        });

        
    }
}
