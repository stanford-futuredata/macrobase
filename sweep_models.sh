#!/bin/zsh

set -x 

for model in MEAN_FIELD_GMM MEAN_FIELD_DPGMM EM_GMM SVI_GMM SVI_DPGMM
do
  java ${JAVA_OPTS} -Dmacrobase.analysis.transformType=$model -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*:target/test-classes" macrobase.MacroBase pipeline conf/custom/3gaussians-100k.yaml > 100k_gaussian_logs/$model-100k.txt
done
