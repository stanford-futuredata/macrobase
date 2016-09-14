package macrobase.analysis.contextualoutlier;

import java.util.*;

public class ContextIndexTree {

    private Map<Interval, ContextIndexTree> interval2SubTree = new HashMap<Interval, ContextIndexTree>();
    private Context leafContext; //the context this tree represents if  interval2SubTree is empty
    
    /**
     * Add a to the tree
     * @param c
     */
    public void addContext(Context c) {
        List<Interval> intervals = c.getIntervals();
        addContext(c, intervals);
    }
    
    private void addContext(Context c, List<Interval> intervals) {
        Interval interval = intervals.get(0);
        if (interval2SubTree.containsKey(interval)) {
        } else {
            ContextIndexTree cit = new ContextIndexTree();
            interval2SubTree.put(interval, cit);
        }
        if (intervals.size() == 1) {
            interval2SubTree.get(interval).leafContext = c;
            return;
        }
        interval2SubTree.get(interval).addContext(c, intervals.subList(1, intervals.size()));
        
    }
    
    public Context getContext(List<Interval> intervals) {
        if (!interval2SubTree.containsKey(intervals.get(0))) {
            return null;
        }
        
        if (intervals.size() == 1) {
            return interval2SubTree.get(intervals.get(0)).leafContext;
        }
        
        ContextIndexTree subTree = interval2SubTree.get(intervals.get(0));
        return subTree.getContext(intervals.subList(1, intervals.size()));
    }
    

}
