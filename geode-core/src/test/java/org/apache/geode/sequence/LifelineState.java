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

import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.swing.JLabel;

/**
 * Created by IntelliJ IDEA.
 * User: dsmith
 * Date: Oct 29, 2010
 * Time: 3:45:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class LifelineState {
    private final long startTime;
    private final long endTime;
    private final String stateName;
    private int startY;
    private int height;
    private final Lifeline line;
    private final Set<Arrow> inboundArrows = new HashSet<Arrow>();
    private static final int ARC_SIZE = 10;

    public int getStartY() {
        return startY;
    }

    public int getHeight() {
        return height;
    }


    public LifelineState(Lifeline line, String stateName, long startTime, long endTime) {
        this.line = line;
        this.stateName = stateName;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public void paint(Graphics2D g, StateColorMap colorMap) {
        Rectangle bounds = g.getClipBounds();
        if(startY > bounds.getMaxY()  || startY+height <bounds.getMinY()) {
            return;
        }
        
        int x = line.getX();
        int width  = line.getWidth();

        Color color = colorMap.getColor(stateName);
        g.setColor(color);
        g.fillRoundRect(x, startY, width, height, ARC_SIZE, ARC_SIZE);
        g.setColor(Color.BLACK);
    }
    
    public void highlight(Graphics2D g) {
      Rectangle bounds = g.getClipBounds();
      if(startY > bounds.getMaxY()  || startY+height <bounds.getMinY()) {
          return;
      }
      
      int x = line.getX();
      int width  = line.getWidth();

      g.drawRoundRect(x, startY, width, height, ARC_SIZE, ARC_SIZE);
    }

    public void resize(double scale, long base) {
        startY = (int)  ((startTime - base) * scale);
        height = (int) ((endTime - startTime) * scale);
    }

    public Component getPopup() {
        return new JLabel("<html>Object: " + line.getDiagramName() + "<br>Member: " + line.getName() + "<br>State: " + stateName + "<br>Time:" + new Date(startTime) + "</html>");
    }

    public Lifeline getLine() {
      return line;
    }

    public void addInboundArrow(Arrow arrow) {
      inboundArrows.add(arrow);
    }

    public Set<Arrow> getInboundArrows() {
      return inboundArrows;
    }
    
    @Override
    public String toString() {
      return line.getName() + "@" + startTime + ":" + stateName;
    }
}
