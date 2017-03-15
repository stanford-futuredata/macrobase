package macrobase.util.data;

import java.util.ArrayList;
import java.util.List;

public class TinyDataSource implements DataSource {
    @Override
    public List<double[]> get() {
        double[][] values = {
                {2,3,3},
                {5,4,2},
                {9,6,7},
                {4,7,9},
                {8,1,5},
                {7,2,6},
                {9,4,1},
                {8,4,2},
                {9,7,8},
                {6,3,1},
                {3,4,5},
                {1,6,8},
                {11,5,3},
                {2,1,3},
                {8,7,6}
        };

        ArrayList<double[]> l = new ArrayList<>();
        for (double[] row : values) {
            l.add(row);
        }

        return l;
    }
}
