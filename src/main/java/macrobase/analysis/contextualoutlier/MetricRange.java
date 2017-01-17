package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.List;

public class MetricRange {

    //denote a range [left, right]
    private double left;
    private double right;
    private boolean leftClosed;
    private boolean rightClosed;
    
    public MetricRange(double left, double right) {
        this.left = left;
        this.right = right;
        this.leftClosed = true;
        this.rightClosed = true;
    }
    
    public MetricRange(double left, double right, boolean leftClosed, boolean rightClosed) {
        this.left = left;
        this.right = right;
        this.leftClosed = leftClosed;
        this.rightClosed = rightClosed;
    }
    
    public double getLeft() {
        return left;
    }
    
    public boolean getLeftClosed() {
        return leftClosed;
    }
    
    public double getRight() {
        return right;
    }
    
    public boolean getRightClosed() {
        return rightClosed;
    }
    
    /**
     * Intersect the current MR with other MR
     * @param other
     * @return
     */
    public boolean intersect(MetricRange other) {
        if (left < other.left) {
            left = other.left;
            leftClosed = other.leftClosed;
        } else if (left == other.left) {
            leftClosed = leftClosed && other.leftClosed;
        }
        
        if (other.right < right) {
            right = other.right;
            rightClosed = other.rightClosed;
        } else if (other.right == right) {
            rightClosed = rightClosed && other.rightClosed;
        } 
       
        if (right < left) {
            return false;
        } else if (right == left) {
            return leftClosed && rightClosed;
        } else {
            return true;
        }
           
    }
    
    /**
     * Is the current MR contained in the other
     * @param other
     * @return
     */
    public boolean contained(MetricRange other) {
        if (other.left > left)
            return false;
        else if (other.left == left && other.leftClosed == false && leftClosed == true)
            return false;
        
        if (other.right < right)
            return false;
        else if(other.right == right && other.rightClosed == false && rightClosed == true)
            return false;
        
        return true;
    }
    
    /**
     * 
     * @param value
     * @return
     */
    public boolean isInRange(double value) {
        if (value < left)
            return false;
        if (value == left && leftClosed == false)
            return false;
        if (value > right)
            return false;
        if (value == right && rightClosed == false)
            return false;
        
        return true;
    }
    
   /**
    * Return the metric ranges that is the current MR, but not in the other MR
    * @param other
    * @return
    */
    public List<MetricRange> minus(MetricRange other) {
        List<MetricRange> result = new ArrayList<MetricRange>();
        
        if (other.left < left || 
                (other.left == left && other.leftClosed == leftClosed) || 
                (other.left == left && other.leftClosed == true &&  leftClosed == false)) {
            if (other.right < left) {
                MetricRange mr1 = new MetricRange(this.left, this.right, this.leftClosed, this.rightClosed);
                result.add(mr1);
                return result;
            } else if (other.right == left) {
                if (other.rightClosed && leftClosed) {
                    MetricRange mr1 = new MetricRange(this.left, this.right, false, this.rightClosed);
                    result.add(mr1);
                    return result;
                } else {
                    MetricRange mr1 = new MetricRange(this.left, this.right, this.leftClosed, this.rightClosed);
                    result.add(mr1);
                    return result;
                }
            } else {
                //other.right > left
                if (other.right < right) {
                    MetricRange mr1 = new MetricRange(other.right, right, !other.rightClosed, this.rightClosed);
                    result.add(mr1);
                    return result;
                } else if (other.right == right) {
                    if (rightClosed == true && other.rightClosed == false) {
                        MetricRange mr1 = new MetricRange(right, right, this.rightClosed, this.rightClosed);
                        result.add(mr1);
                        return result;
                    } else {
                        return result;
                    }
                } else {
                    return result;
                }
            }
                
        } else {
            //other.left > left or other.left == left && leftClosed == true && other.leftClosed == false
            if (other.left < right) {
                if (other.right < right ||
                        (other.right == right && rightClosed == true && other.rightClosed == false)) {
                    MetricRange mr1 = new MetricRange(this.left, other.left, this.leftClosed, !other.leftClosed);
                    MetricRange mr2 = new MetricRange(other.right, this.right, !other.rightClosed, rightClosed);
                    result.add(mr1);
                    result.add(mr2);
                    return result;
                } else {
                    MetricRange mr1 = new MetricRange(this.left, other.left, this.leftClosed, !other.leftClosed);
                    result.add(mr1);
                    return result;
                }
            }
        }
        
        return result;
    }
    
    public boolean isSame(MetricRange mr) {
        return left == mr.left && right == mr.right && leftClosed == mr.leftClosed && rightClosed == mr.rightClosed;
    }
    
    @Override
    public String toString() {
        return  ( (leftClosed)?"[ ":"( " ) + left + " , " + right + (  (rightClosed)?" ]": " )" ) ; 
    }
}
