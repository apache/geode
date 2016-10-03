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
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: dsmith
 * Date: Oct 29, 2010
 * Time: 4:18:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class SequenceDiagram extends JPanel {

    /**
   * 
   */
  private static final Color HIGHLIGHT_COLOR = Color.RED;

    private final List<String> lineNames;
    private final Map<String, List<String>> shortLineNames;
    private final SortedMap<Comparable, SubDiagram> subDiagrams = new TreeMap<Comparable, SubDiagram>();
    private final StateColorMap colorMap = new StateColorMap();
    private final long minTime;
    private final long maxTime;

    private static int PADDING_BETWEEN_LINES = 100;
    private static int Y_PADDING = 20;
    private static final int STATE_WIDTH = 20;
    private static final int LINE_LABEL_BOUNDARY = 5;
    private static final int AXIS_SIZE=35;

    private int lineStep;
    private int lineWidth;
    private Popup mouseover;
    private LifelineState mouseoverState;
    private LifelineState selectedState;

    public SequenceDiagram(long minTime, long maxTime, List<String> lineNames, LineMapper lineMapper) {
        this.lineNames = lineNames;
        this.shortLineNames = parseShortNames(lineNames, lineMapper);
        this.minTime = minTime;
        this.maxTime = maxTime;
        int width = getInitialWidth();
        int height = 500;
        super.setPreferredSize(new Dimension(width, height));
        resizeMe(width, height);
        addComponentListener(new ComponentAdapter() {
            @Override
            public void componentResized(ComponentEvent e) {
                Component source = (Component) e.getSource();
                resizeMe(source.getWidth(), source.getHeight());
            }
        });
        setBackground(Color.WHITE);
    }

    private Map<String, List<String>> parseShortNames(List<String> lineNames, LineMapper lineMapper) {
        Map<String, List<String>> shortNames = new LinkedHashMap<String, List<String>>(lineNames.size());
        for(String name : lineNames) {
          String shortName = lineMapper.getShortNameForLine(name);
          List<String> list = shortNames.get(shortName);
          if(list == null) {
            list = new ArrayList<String>();
            shortNames.put(shortName, list);
          }
          list.add(name);
        }
          
        return shortNames;
    }

    public List<Comparable> getSubDiagramsNames() {
        return new ArrayList<Comparable>(subDiagrams.keySet());

    }

    public void addSubDiagram(Comparable name, Map<String, Lifeline> lines, List<Arrow> arrows) {
        subDiagrams.put(name, new SubDiagram(name, lines, arrows));
    }

    public void removeSubDiagram(Comparable name) {
        this.subDiagrams.remove(name);
    }

    private int getInitialWidth() {
        return (this.shortLineNames.size()) * PADDING_BETWEEN_LINES;
    }

    public void resizeMe(int width, int height) {
        this.setPreferredSize(new Dimension(width, height));
        float xZoom = width / (float) getInitialWidth();


        long elapsedTime = maxTime - minTime;
        double yScale = height / (double) elapsedTime;
//        long yBase = (long) (minTime - ((long) Y_PADDING / yScale));
        long yBase = minTime;
        lineStep = (int) (PADDING_BETWEEN_LINES * xZoom);
        lineWidth = (int) (STATE_WIDTH * xZoom);

        if(subDiagrams.size() <= 0) {
            return;
        }
        
        int sublineWidth = lineWidth / subDiagrams.size();
        int sublineIndex = 0;
        for(SubDiagram diagram : subDiagrams.values()) {
            int lineIndex = 0;
            for(List<String> fullNames : shortLineNames.values()) {
              for(String name : fullNames) {
                Lifeline line = diagram.lifelines.get(name);
                if(line != null) {
                    int lineX = lineIndex * lineStep + sublineIndex * sublineWidth;
                    line.resize(lineX, sublineWidth, yBase, yScale);
                }
              }
              lineIndex++;
            }
            sublineIndex++;
        }
    }

    public void showPopupText(int x, int y, int xOnScreen, int yOnScreen) {
      LifelineState state = getStateAt(x, y);
      if(state == mouseoverState) {
        return;
      }
      if(mouseover != null) {
        mouseover.hide();
      }
      if(state == null) {
        mouseover = null;
        mouseoverState = null;
      } else {
        Component popupContents = state.getPopup();        
        mouseoverState = state;
        mouseover = PopupFactory.getSharedInstance().getPopup(this, popupContents, xOnScreen + 20, yOnScreen);
        mouseover.show();
      }
    }
    
    public void selectState(int x, int y) {
      LifelineState state = getStateAt(x, y);
      if(state == selectedState) {
        return;
      }
      
      if(selectedState != null) {
        fireRepaintOfDependencies(selectedState);
      }
      
      selectedState = state;
      
      if(state != null) {
        fireRepaintOfDependencies(state);
      }
    }
    
    private LifelineState getStateAt(int x, int y) {
    //TODO - this needs some
      //serious optimization to go straight to the right state
      // I think we need a tree map of of lines, keyed by x offset
      //and a keymap of states keyed by y offset.
      //That could make painting faster as well.
      List<String> reverseList = new ArrayList<String>(lineNames);
      Collections.reverse(reverseList);
      for(SubDiagram diagram : subDiagrams.values()) {
        for(String name : reverseList) {
            Lifeline line = diagram.lifelines.get(name);
            if(line != null) {
                if(line.getX() < x && line.getX() + line.getWidth() > x) {
                    LifelineState state = line.getStateAt(y);
                    if(state != null) {
                      return state;
                    }
                }
            }
        }
      }
      return null;
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        Graphics2D g2 = (Graphics2D) g.create();
        //TODO - we should clip these to the visible lines
        for(SubDiagram subDiagram : subDiagrams.values()) {
            subDiagram.paintStates(g2, colorMap);
        }

        for(SubDiagram subDiagram : subDiagrams.values()) {
            subDiagram.paintArrows(g2, colorMap);
        }
        paintHighlightedComponents(g2, selectedState, new HashSet<LifelineState>());
    }
    
    private void fireRepaintOfDependencies(LifelineState state) {
      //TODO - it might be more efficient to repaint just the changed 
      //areas, but right now this will do.
      repaint();
    }

    private void paintHighlightedComponents(Graphics2D g2, LifelineState endingState, Set<LifelineState> visited) {
      if(!visited.add(endingState)) {
        //Prevent cycles
        return;
      }
      g2.setColor(HIGHLIGHT_COLOR);
      if(endingState != null) {
        endingState.highlight(g2);
        
        for(Arrow arrow : endingState.getInboundArrows()) {
          arrow.paint(g2);
          paintHighlightedComponents(g2, arrow.getStartingState(), visited);
        }
      }
      
      
    }

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public JComponent createMemberAxis() {
        return new MemberAxis();
    }

    private class MemberAxis extends JComponent {
        public MemberAxis() {
            setPreferredSize(new Dimension(getWidth(), AXIS_SIZE));
            SequenceDiagram.this.addComponentListener(new ComponentAdapter() {
                @Override
                public void componentResized(ComponentEvent e) {
                    int newWidth = e.getComponent().getWidth();
                    setPreferredSize(new Dimension(newWidth, AXIS_SIZE));
                    revalidate();
                }
            });
        }
        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Rectangle bounds = g.getClipBounds();
            int index = 0;
            for(String name : shortLineNames.keySet()) {
                int nameWidth = g.getFontMetrics().stringWidth(name);
                int lineX = lineStep * index;
                index++;
                if(bounds.getMaxX() < lineX
                        || bounds.getMinX() > lineX + nameWidth) {
                    continue;
                }
                g.setClip(lineX + LINE_LABEL_BOUNDARY, 0, lineStep - + LINE_LABEL_BOUNDARY * 2, getHeight());
                g.drawString(name, lineX + LINE_LABEL_BOUNDARY, AXIS_SIZE / 3);
                g.setClip(null);
            }
        }
    }

    public static class SubDiagram {
        private final Map<String, Lifeline> lifelines;
        private final List<Arrow> arrows;
        private final Comparable name;

        public SubDiagram(Comparable name, Map<String, Lifeline> lines, List<Arrow> arrows) {
            this.name = name;
            this.lifelines = lines;
            this.arrows = arrows;
        }

        public void paintStates(Graphics2D g2, StateColorMap colorMap) {
            for(Lifeline line: lifelines.values()) {
                line.paint(g2, colorMap);
            }
        }
        public void paintArrows(Graphics2D g2, StateColorMap colorMap) {
            Color lineColor = colorMap.getColor(name);
            g2.setColor(lineColor);
            for(Arrow arrow: arrows) {
                arrow.paint(g2);
            }
        }
    }
}
