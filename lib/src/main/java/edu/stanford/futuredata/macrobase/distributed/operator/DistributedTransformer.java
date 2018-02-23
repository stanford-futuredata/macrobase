package edu.stanford.futuredata.macrobase.distributed.operator;

import edu.stanford.futuredata.macrobase.distributed.datamodel.DistributedDataFrame;
import edu.stanford.futuredata.macrobase.operator.Operator;

import java.io.Serializable;

public interface DistributedTransformer extends Serializable {
    DistributedDataFrame process(DistributedDataFrame input) throws Exception;
}
