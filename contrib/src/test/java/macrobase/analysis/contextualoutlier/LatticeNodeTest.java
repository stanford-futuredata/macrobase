package macrobase.analysis.contextualoutlier;

import macrobase.datamodel.Datum;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class LatticeNodeTest {

    @Test
    public void latticeNodeTest() {
        LatticeNode node0 = new LatticeNode(0);
        LatticeNode node1 = new LatticeNode(1);
        LatticeNode node2 = new LatticeNode(2);
        
        List<ContextualDatum> data = new ArrayList<>();
        
        LatticeNode node01 = node0.join(node1, data, 1.0);
        LatticeNode node02 = node0.join(node2, data, 1.0);
        LatticeNode node12 = node1.join(node2, data, 1.0);

        assertEquals(node01.getDimensions().contains(0),true);
        assertEquals(node01.getDimensions().contains(1),true);
        
        assertEquals(node02.getDimensions().contains(0),true);
        assertEquals(node02.getDimensions().contains(2),true);
        
        assertEquals(node12.getDimensions().contains(1),true);
        assertEquals(node12.getDimensions().contains(2),true);
        
        LatticeNode node012 = node01.join(node02, data, 1.0);
        assertEquals(node012.getDimensions().contains(0),true);
        assertEquals(node012.getDimensions().contains(1),true);
        assertEquals(node012.getDimensions().contains(2),true);
        node012.clear();
        assertEquals(node012.getDimensions().size(),0);

        LatticeNode node012Null = node12.join(node02, data, 1.0);
        assertEquals(node012Null,null);
        
        List<LatticeNode> latticeNodeByDimensions = new ArrayList<LatticeNode>();
        latticeNodeByDimensions.add(node12);
        latticeNodeByDimensions.add(node02);
        latticeNodeByDimensions.add(node01);
        assertEquals(latticeNodeByDimensions.get(0),node12);
        assertEquals(latticeNodeByDimensions.get(1),node02);
        assertEquals(latticeNodeByDimensions.get(2),node01);
        Collections.sort(latticeNodeByDimensions, new LatticeNode.DimensionComparator());
        assertEquals(latticeNodeByDimensions.get(0),node01);
        assertEquals(latticeNodeByDimensions.get(1),node02);
        assertEquals(latticeNodeByDimensions.get(2),node12);
        
        LatticeNode node01Copy = node1.join(node0, data, 1.0);
        latticeNodeByDimensions.add(node01Copy);
        Collections.sort(latticeNodeByDimensions, new LatticeNode.DimensionComparator());
        assertEquals(latticeNodeByDimensions.get(0),node01);
        assertEquals(latticeNodeByDimensions.get(1),node01Copy);

        LatticeNode node00 = node0.join(node0, data, 1.0);
        assertEquals(node00,null);
        
        latticeNodeByDimensions.remove(node01Copy);
        latticeNodeByDimensions.add(node01);
        Collections.sort(latticeNodeByDimensions, new LatticeNode.DimensionComparator());
        assertEquals(latticeNodeByDimensions.get(0),node01);
        assertEquals(latticeNodeByDimensions.get(1),node01);
        //assertEquals(latticeNodeByDimensions.get(2),node01Copy);
    }
}
