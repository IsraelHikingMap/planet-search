package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class LineMerger {

    List<Geometry> lines = new ArrayList<>();

    public void add(Geometry line) {
        lines.add(line);
    }

    public Coordinate getFirstPoint() {
        List<List<Coordinate>> nodesGroups = new ArrayList<>();
        List<Geometry> waysToGroup = lines.stream()
            .filter(w -> !w.isEmpty())
            .collect(Collectors.toList());
            
        while (!waysToGroup.isEmpty()) {
            Geometry wayToGroup = waysToGroup.stream()
                .filter(w -> nodesGroups.stream()
                    .anyMatch(g -> canBeMerged(Arrays.asList(w.getCoordinates()), g)))
                .findFirst()
                .orElse(null);
                
            if (wayToGroup == null) {
                nodesGroups.add(new LinkedList<>(Arrays.asList((waysToGroup.getFirst().getCoordinates()))));
                waysToGroup.removeFirst();
                continue;
            }
            
            List<Coordinate> currentNodes = new LinkedList<>(Arrays.asList(wayToGroup.getCoordinates()));
            waysToGroup.remove(wayToGroup);
            List<Coordinate> group = nodesGroups.stream()
                .filter(g -> canBeMerged(currentNodes, g))
                .findFirst()
                .get();
                
            if (canBeReverseMerged(group, currentNodes)) {
                Collections.reverse(currentNodes);
            }
            
            if (currentNodes.getFirst().equals2D(group.getLast())) {
                currentNodes.removeFirst();
                group.addAll(currentNodes);
                continue;
            }
            
            currentNodes.removeLast();
            group.addAll(0, currentNodes);
        }
        
        List<List<Coordinate>> nodes = rearrangeInCaseOfCircleAndLine(nodesGroups);
            
        return nodes.getFirst().getFirst();
    }

    private boolean canBeMerged(List<Coordinate> nodes1, List<Coordinate> nodes2)
    {
        return nodes1.getLast().equals2D(nodes2.getFirst()) ||
               nodes1.getFirst().equals2D(nodes2.getLast()) ||
               canBeReverseMerged(nodes1, nodes2);
    }

    private boolean canBeReverseMerged(List<Coordinate> nodes1, List<Coordinate> nodes2)
    {
        return nodes1.getFirst().equals2D(nodes2.getFirst()) ||
               nodes1.getLast().equals2D(nodes2.getLast());
    }
    
    /**
     * The purpose of this method is to take grouped results that grouped into "O" shape and lines that
     * touches this "O" shape and turn them into a "Q" shape.
     * This should only be applied to multiline strings
     * It does so by going over all the circles, finding lines that are not circles that touches those
     * and reorder the points, adding a new line and removes the circle and the line from the original list
     * @param coordinateGroups original list of list of nodes to alter
     * @returns A new list of list of nodes after the changes
     */
    private List<List<Coordinate>> rearrangeInCaseOfCircleAndLine(List<List<Coordinate>> coordinateGroups) {
        if (coordinateGroups.size() == 1) {
            return coordinateGroups;
        }

        // Find circles (groups where first and last coordinates are the same)
        List<List<Coordinate>> circles = coordinateGroups.stream()
            .filter(g -> g.getFirst().equals2D(g.getLast()))
            .collect(Collectors.toList());

        if (circles.isEmpty()) {
            return coordinateGroups;
        }

        for (List<Coordinate> circle : circles) {
            // Find a line that touches the circle
            List<Coordinate> lineThatTouchesTheCircle = coordinateGroups.stream()
                .filter(g -> !circles.contains(g))
                .filter(g -> circle.stream().anyMatch(n -> 
                    n.equals2D(g.getFirst()) || n.equals2D(g.getLast())))
                .findFirst()
                .orElse(null);

            if (lineThatTouchesTheCircle == null) {
                continue;
            }

            coordinateGroups.remove(circle);
            coordinateGroups.remove(lineThatTouchesTheCircle);

            // Find the coordinate in circle that touches the end of the line
            Coordinate nodeInCircleThatTouches = circle.stream()
                .filter(n -> n.equals2D(lineThatTouchesTheCircle.getLast()))
                .findFirst()
                .orElse(null);

            if (nodeInCircleThatTouches != null) {
                int indexInCircle = circle.indexOf(nodeInCircleThatTouches);
                List<Coordinate> newList = new ArrayList<>(lineThatTouchesTheCircle);
                
                // Add remaining coordinates after the touch point
                newList.addAll(circle.subList(indexInCircle + 1, circle.size()));
                
                // Add coordinates from start up to touch point
                newList.addAll(circle.subList(1, indexInCircle));
                
                coordinateGroups.add(newList);
                continue;
            }

            // Find the coordinate in circle that touches the start of the line
            nodeInCircleThatTouches = circle.stream()
                .filter(n -> n.equals2D(lineThatTouchesTheCircle.getFirst()))
                .findFirst()
                .orElse(null);

            if (nodeInCircleThatTouches != null) {
                int indexInCircle = circle.indexOf(nodeInCircleThatTouches);
                List<Coordinate> newList = new ArrayList<>();
                
                // Add coordinates from circle between start and touch point
                newList.addAll(circle.subList(1, indexInCircle));
                
                // Add the line
                newList.addAll(lineThatTouchesTheCircle);
                
                // Add remaining coordinates from circle
                newList.addAll(0, circle.subList(indexInCircle, circle.size()));
                
                coordinateGroups.add(newList);
            }
        }

        return coordinateGroups;
    }
    
}
