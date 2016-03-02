package macrobase.analysis.contextualoutlier;

import java.util.*;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.datamodel.Datum;

public class ContextPruning {

	public static List<Datum> data;

	public static OutlierDetector detector;
	
	public static boolean pdfPruning(Context c1, Context c2){
		return false;
	}
	
	public static boolean detectorSpecificPruning(Context c1, Context c2){
		return false;
	}
}
