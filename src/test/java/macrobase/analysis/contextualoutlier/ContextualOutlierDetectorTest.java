
package macrobase.analysis.contextualoutlier;

import static junit.framework.TestCase.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import macrobase.analysis.stats.BatchTrainScore;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import macrobase.analysis.stats.MAD;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

public class ContextualOutlierDetectorTest {

	
	public ContextualOutlierDetectorTest() {
		
	}
	
	@Test
    public void testContextualDiscreteAttribute() {
	   //construct a contextual outlier detector
	   MAD mad = new MAD(new MacroBaseConf());
	   List<String> contextualDiscreteAttributes = new ArrayList<String>();
	   contextualDiscreteAttributes.add("C1_Discrete");
	   List<String> contextualDoubleAttributes = new ArrayList<String>();
       ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(
    		   mad,
    		   contextualDiscreteAttributes,
    		   contextualDoubleAttributes,
    		   0.4, 10);
		
		
		List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            
            Integer[] c1 = new Integer[1];
            if(i < 5){
            	c1[0] = 1;
            }
            else if( i >=5 && i < 50){
            	c1[0] = 2;
            }else{
            	c1[0] = 1;
            }
            
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample),
            		new ArrayList<Integer>(Arrays.asList(c1)),
            		new ArrayRealVector()));
        }

        
        contextualDetector.searchContextualOutliers(data, 3);
        Map<Context,BatchTrainScore.BatchResult> context2Outliers = contextualDetector.getContextualOutliers();
        assertEquals(context2Outliers.size(), 1);
        
        for(Context context: context2Outliers.keySet()){
        	System.out.println("Context: " + context.toString());
        	System.out.println("Number of Outliers: " + context2Outliers.get(context).getOutliers().size());
        }
        
    }
	
	
	@Test
    public void testContextualDoubleAttribute() {
	   //construct a contextual outlier detector
	   MAD mad = new MAD(new MacroBaseConf());
	   List<String> contextualDiscreteAttributes = new ArrayList<String>();
	   List<String> contextualDoubleAttributes = new ArrayList<String>();
	   contextualDoubleAttributes.add("C1_Double");
       ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(
    		   mad,
    		   contextualDiscreteAttributes,
    		   contextualDoubleAttributes,
    		   0.4, 10);
		
		
		List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            
            double[] c1 = new double[1];
            if(i < 5){
            	c1[0] = 1;
            }
            else if( i >=5 && i < 50){
            	c1[0] = 100;
            }else{
            	c1[0] = 1;
            }
            
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample),
            		new ArrayList<Integer>(),
            		new ArrayRealVector(c1)));
        }

        
        contextualDetector.searchContextualOutliers(data, 3);
        Map<Context,BatchTrainScore.BatchResult> context2Outliers = contextualDetector.getContextualOutliers();
        assertEquals(context2Outliers.size(), 1);
        
        for(Context context: context2Outliers.keySet()){
        	System.out.println("Context: " + context.toString());
        	System.out.println("Number of Outliers: " + context2Outliers.get(context).getOutliers().size());
        }
    
    }
	
	@Test
    public void testTwoAttributesContext() {
	   //construct a contextual outlier detector
	   MAD mad = new MAD(new MacroBaseConf());
	   List<String> contextualDiscreteAttributes = new ArrayList<String>();
	   contextualDiscreteAttributes.add("C1_Discrete");
	   
	   List<String> contextualDoubleAttributes = new ArrayList<String>();
	   contextualDoubleAttributes.add("C2_Double");
      
	   ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(
    		   mad,
    		   contextualDiscreteAttributes,
    		   contextualDoubleAttributes,
    		   0.3, 10);
		
	   
		
		List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 120; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            
            Integer[] c1 = new Integer[1];
            if(i < 80){
            	c1[0] = 1;
            }else{
            	c1[0] = 2;
            }
            
            double[] c2 = new double[1];
             if(i < 40){
            	c2[0] = 1.0;
            }
            else if( i >=40 && i < 79){
            	c2[0] = 100.0;
            }else if( i >= 79 && i < 80){
            	c2[0] = 1.0;
            }else{
            	c2[0] = 1.0;
            }
            
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample),
            		new ArrayList<Integer>(Arrays.asList(c1)),
            		new ArrayRealVector(c2)));
        }

        
        contextualDetector.searchContextualOutliers(data, 3);
        Map<Context,BatchTrainScore.BatchResult> context2Outliers = contextualDetector.getContextualOutliers();
        assertEquals(context2Outliers.size(), 1);
        
        for(Context context: context2Outliers.keySet()){
        	System.out.println("Context: " + context.toString());
        	System.out.println("Number of Outliers: " + context2Outliers.get(context).getOutliers().size());
        }
    
    }
}