package macrobase.ingest;

/**
 * Slightly inaccurate fast floating point parser from char array range
 */
class CharDoubleParser {
    private final double[] invPow10 = new double[50];

    CharDoubleParser() {
        double curPower = 1.0;
        for (int i = 0; i < invPow10.length; i++) {
            invPow10[i] = curPower;
            curPower /= 10.0;
        }
    }

    double parseDouble(char[] c, int i, int j) {
        double val = 0.0;
        double sign = 1;
        boolean seenDecimal = false;
        int digitsAfterDecimal = 0;
        int totalDigits = 0;

        if (i >= c.length) {
            return Double.NaN;
        }

        if (c[i] == '-') {
            sign = -1;
            i++;
        }
        while (i < j) {
            char curChar = c[i];
            int intVal = (int)curChar - 48;
            if (intVal >= 0 && intVal <= 9) {
                if (seenDecimal) {
                    digitsAfterDecimal++;
                }
                totalDigits++;
                val = 10*val+intVal;
            } else if (curChar == '.') {
                seenDecimal = true;
            }
            i++;
        }
        if (totalDigits > 0) {
            return sign * val * invPow10[digitsAfterDecimal];
        } else {
            return Double.NaN;
        }
    }
}
