package macrobase.analysis.contextualoutlier;

import macrobase.conf.MacroBaseDefaults;

public class ContextPruningOptions {

	private boolean densityPruning;
	private boolean dependencyPruning;
	
	private boolean distributionPruningForTraining;
	private boolean distributionPruningForScoring;
	
	public ContextPruningOptions(String contextPruningOptions) {
		if(contextPruningOptions.equals(MacroBaseDefaults.CONTEXTUAL_PRUNING)){
			//By default, all prunings are enabled
			setDensityPruning(true);
			setDependencyPruning(true);
			setDistributionPruningForTraining(true);
			setDistributionPruningForScoring(true);
		}else{
			if(contextPruningOptions.contains("densityPruningEnabled")){
				setDensityPruning(true);
			}
			if(contextPruningOptions.contains("dependencyPruningEnabled")){
				setDependencyPruning(true);
			}
			if(contextPruningOptions.contains("distributionPruningForTrainingEnabled")){
				setDistributionPruningForTraining(true);
			}
			if(contextPruningOptions.contains("distributionPruningForScoringEnabled")){
				setDistributionPruningForScoring(true);
			}
		}
	}

	public boolean isDensityPruning() {
		return densityPruning;
	}

	public void setDensityPruning(boolean densityPruning) {
		this.densityPruning = densityPruning;
	}

	public boolean isDependencyPruning() {
		return dependencyPruning;
	}

	public void setDependencyPruning(boolean dependencyPruning) {
		this.dependencyPruning = dependencyPruning;
	}

	public boolean isDistributionPruningForTraining() {
		return distributionPruningForTraining;
	}

	public void setDistributionPruningForTraining(boolean distributionPruningForTraining) {
		this.distributionPruningForTraining = distributionPruningForTraining;
	}

	public boolean isDistributionPruningForScoring() {
		return distributionPruningForScoring;
	}

	public void setDistributionPruningForScoring(boolean distributionPruningForScoring) {
		this.distributionPruningForScoring = distributionPruningForScoring;
	}

}
