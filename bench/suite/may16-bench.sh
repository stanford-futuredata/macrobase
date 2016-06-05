BGCMD=""
ITERATIONS=1

# run all workflows first

eval python execute_workflows.py \
   --number-of-runs $ITERATIONS \
   $BGCMD

# comparing heavy hitters
eval python execute_workflows.py \
    --number-of-runs $ITERATIONS \
    --diagnostic macrobase.bench.HeavyHittersComparePipeline \
    --target_workflows telematicsComplex \
    $BGCMD

# comparing various summary comparison algorithms

eval python execute_workflows.py \
    --number-of-runs 1 \
    --diagnostic macrobase.bench.SummaryComparePipeline \
     $BGCMD

# runtime versus num summaries varying ratio, support

eval python execute_workflows.py \
    --number-of-runs 1 \
    --diagnostic macrobase.bench.RatioSupportSweepPipeline \
    --target_workflows campaignExpendituresComplex, telematicsComplex \
    $BGCMD

# accuracy training on samples

eval python execute_workflows.py \
    --number-of-runs $ITERATIONS \
    --diagnostic macrobase.bench.SampleComparePipeline \
    --target_workflows campaignExpendituresComplex, telematicsComplex \
    $BGCMD

# compute score CDFs

eval python execute_workflows.py \
    --number-of-runs 1 \
    --diagnostic macrobase.bench.ScoreCDFPipeline \
     $BGCMD

wait

# scaleout

for threads in 1 2 4 8 16 32 64;
do
    python execute_workflows.py \
        --number-of-runs $ITERATIONS \
        --diagnostic macrobase.bench.NaiveOneShotScaleoutStreamingPipeline \
        --target_workflows telematicsSimpleStreaming, telematicsComplexStreaming, fedDisbursementsSimpleStreaming, fedDisbursementsComplexStreaming \
        --extra_env_args " -Dmacrobase.naive.scaleout.factor=$threads"
done


# compare CPS

eval python execute_workflows.py \
    --number-of-runs 1 \
    --diagnostic macrobase.bench.FullCPSStreamingPipeline \
    --target_workflows accidentsComplexStreaming, milanTelecomComplexStreaming, campaignExpendituresComplexStreaming, fedDisbursementsComplexStreaming, accidentsComplexStreaming, liquorComplexStreaming, telematicsComplexStreaming \
     $BGCMD
