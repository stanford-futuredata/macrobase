package macrobase.analysis.contextualoutlier;

import macrobase.MacroBase;
import macrobase.analysis.contextualoutlier.conf.ContextualConf;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;

public class ContextualDatum extends Datum {
    private List<Integer> contextualDiscreteAttributesValues;
    private RealVector contextualDoubleAttributes;

    public ContextualDatum(List<Integer> attributes, double... doubleMetrics) {
        super(attributes, doubleMetrics);
    }

    public ContextualDatum(Datum d, MacroBaseConf conf) throws ConfigurationException {
        super(d);

        List<String> attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        List<String> contextualDiscreteNames = conf.getStringList(ContextualConf.CONTEXTUAL_DISCRETE_ATTRIBUTES,
                                                                  null);

        if(contextualDiscreteNames != null) {
            contextualDiscreteAttributesValues = new ArrayList<>(contextualDiscreteNames.size());

            for (String attr : contextualDiscreteNames) {
                int pos = attributes.indexOf(attr);
                contextualDiscreteAttributesValues.add(d.attributes().get(pos));
            }
        }

        List<String> contextualDoubleNames = conf.getStringList(ContextualConf.CONTEXTUAL_DOUBLE_ATTRIBUTES,
                                                                  null);


        if(contextualDoubleNames != null) {
            contextualDoubleAttributes = new ArrayRealVector(contextualDoubleNames.size());

            int vecPos = 0;
            for (String attr : contextualDoubleNames) {
                int pos = attributes.indexOf(attr);
                contextualDoubleAttributes.setEntry(vecPos,
                                                    Double.parseDouble(conf.getEncoder()
                                                                               .getAttribute(d.attributes().get(pos))
                                                                               .getValue()));
                vecPos += 1;
            }
        }




    }

    public List<Integer> getContextualDiscreteAttributes() {
        if (contextualDiscreteAttributesValues != null) {
            return contextualDiscreteAttributesValues;
        } else {
            return new ArrayList<>();
        }
    }

    public RealVector getContextualDoubleAttributes() {
        if (contextualDoubleAttributes != null) {
            return contextualDoubleAttributes;
        } else {
            return new ArrayRealVector(0);
        }
    }
}
