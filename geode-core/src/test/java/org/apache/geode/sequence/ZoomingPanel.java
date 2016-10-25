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

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionAdapter;

/**
 * Created by IntelliJ IDEA.
 * User: dan
 * Date: Oct 28, 2010
 * Time: 10:30:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class ZoomingPanel extends JPanel {
    private int zoomBoxStartX;
    private int zoomBoxStartY;
    private int zoomBoxWidth;
    private int zoomBoxHeight;
    private SequenceDiagram child;


    public ZoomingPanel() {
        super();
        addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent e) {
                startBox(e.getX(), e.getY());
            }

            @Override
            public void mouseReleased(MouseEvent e) {
                endBox(e.getX(), e.getY());
            }

            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getButton() != MouseEvent.BUTTON1) {
                    unzoom();    
                } else {
                  child.selectState(e.getX(), e.getY());
                }
                
            }
        });

        addMouseMotionListener(new MouseMotionAdapter() {
            @Override
            public void mouseDragged(MouseEvent e) {
                Rectangle r = new Rectangle(e.getX(), e.getY(), 1, 1);
                ((JPanel)e.getSource()).scrollRectToVisible(r);
                showBox(e.getX(), e.getY());
            }

            @Override
            public void mouseMoved(MouseEvent e) {
              int popupX = ZoomingPanel.this.getLocationOnScreen().x + e.getX();
              int popupY = ZoomingPanel.this.getLocationOnScreen().y + e.getY();
              child.showPopupText(e.getX(), e.getY(), popupX, popupY);
            }
        });
        BorderLayout layout = new BorderLayout();
        layout.setHgap(0);
        layout.setVgap(0);
        this.setLayout(layout);
    }

    private void unzoom() {
        resizeMe(0, 0, getWidth(), getHeight());
    }

    void resizeMe(int zoomBoxX, int zoomBoxY, int zoomBoxWidth, int zoomBoxHeight) {
        Dimension viewSize = getParent().getSize();
        double windowWidth = viewSize.getWidth();
        double  windowHeight = viewSize.getHeight();
        double scaleX = getWidth() / ((double) zoomBoxWidth);
        double scaleY = getHeight() / ((double) zoomBoxHeight);
        int oldWidth = getWidth();
        int oldHeight = getHeight();
        int width = (int) (scaleX * windowWidth);
        int height = (int) (scaleY * windowHeight);
//        this.setPreferredSize(new Dimension(width, height));
        child.resizeMe(width, height);
        //TODO not sure this one is needed
        this.revalidate();

        //scroll to the new rectangle
//        int scrollX = (int) (zoomBoxX * scaleX);
//        int scrollY = (int) (zoomBoxY * scaleY);
//        int scrollWidth= (int) (zoomBoxWidth * scaleX);
//        int scrollHeight = (int) (zoomBoxHeight * scaleY);
        int scrollX = (int) (zoomBoxX  *  (width / (double) oldWidth));
        int scrollY = (int) (zoomBoxY *  (height / (double) oldHeight));
        int scrollWidth= (int) (zoomBoxWidth *  (width / (double) oldWidth));
        int scrollHeight = (int) (zoomBoxHeight *  (height / (double) oldHeight));        
        Rectangle r = new Rectangle(scrollX, scrollY, scrollWidth, scrollHeight);
        ((JViewport)getParent()).scrollRectToVisible(r);
        repaint();

    }

    public void setSequenceDiagram(SequenceDiagram diag) {
        this.child = diag;
        this.add(child, BorderLayout.CENTER);
    }

    private void showBox(int x, int y) {
        if(zoomBoxWidth != -1) {
            repaint(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
        }

        this.zoomBoxWidth = x - zoomBoxStartX;
        this.zoomBoxHeight = y - zoomBoxStartY;

        repaint(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
    }

    private void startBox(int x, int y) {
        this.zoomBoxStartX = x;
        this.zoomBoxStartY = y;
    }

    private void endBox(int x, int y) {
        if(zoomBoxStartX != -1 && zoomBoxStartY != -1
                && zoomBoxWidth != -1 && zoomBoxHeight != -1
                && zoomBoxWidth != 0 && zoomBoxHeight != 0) {
            resizeMe(getBoxX() , getBoxY(), getBoxWidth(), getBoxHeight());
            repaint(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
            this.zoomBoxStartX = -1;
            this.zoomBoxStartY = -1;
            this.zoomBoxWidth = -1;
            this.zoomBoxHeight = -1;
        }
    }

    public int getBoxX() {
        return zoomBoxWidth >= 0 ? zoomBoxStartX : zoomBoxStartX + zoomBoxWidth;
    }

    public int getBoxY() {
        return zoomBoxHeight >= 0 ? zoomBoxStartY : zoomBoxStartY + zoomBoxHeight;
    }

    public int getBoxHeight() {
        return zoomBoxHeight >= 0 ? zoomBoxHeight : -zoomBoxHeight;
    }

    public int getBoxWidth() {
        return zoomBoxWidth >= 0 ? zoomBoxWidth : -zoomBoxWidth;
    }



    @Override
    public void paint(Graphics g) {
        super.paint(g);

        if(zoomBoxStartX != -1 && zoomBoxStartY != -1 && zoomBoxWidth != -1 && zoomBoxHeight != -1) {
            Graphics2D g2 = (Graphics2D) g.create();

            Composite old = g2.getComposite();
            g2.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER, 0.5f));
            g2.setColor(Color.BLUE);

//            g2.drawRect(zoomBoxStartX, zoomBoxStartY, zoomBoxWidth, zoomBoxHeight);
//            g2.setBackground(Color.BLUE);
            g2.fillRect(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
            g2.setComposite(old);
        }
    }
}
