#!/usr/bin/python
import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import subprocess, datetime, os, time, signal
import datetime
from threading import Timer

groud_truth = ["ALL", "OUTLIER_CONTEXT_RATIO_AT_LEAST_0.01", "OUTLIER_CONTEXT_RATIO_AT_LEAST_0.005"]

keysList = ["Query Name",
            "ResultDir", 
                "exit_status",
                "macrobase.analysis.transformType",
                "macrobase.loader.loaderType",
                "macrobase.analysis.contextual.denseContextTau",
                "macrobase.analysis.classify.outlierStaticThreshold",
                "macrobase.analysis.contextual.numTuples",
                #"macrobase.analysis.contextual.pruning.density", 
                #"macrobase.analysis.contextual.pruning.dependency",
                "macrobase.analysis.contextual.pruning.triviality",
                "macrobase.analysis.contextual.pruning.contextContainedInOutliers",
                "macrobase.analysis.contextual.pruning.mad.noOutliers",
                "macrobase.analysis.contextual.pruning.mad.containedOutliers",
                #"macrobase.analysis.contextual.pruning.triviality.approximation",
                #"macrobase.analysis.contextual.pruning.triviality.approximation.propagate",
                #"macrobase.analysis.contextual.pruning.distribution",
                #"macrobase.analysis.contextual.pruning.distribution.alpha",
                #"macrobase.analysis.contextual.pruning.distribution.minsamplesize",
                #"macrobase.analysis.contextual.pruning.distribution.testmean",
                #"macrobase.analysis.contextual.pruning.distribution.testvariance",
                #"macrobase.analysis.contextual.pruning.distribution.testks",
                #"loadMs",
                #"executeMs",
                "timeTotal",
                "timeBuildLattice",
                #"timeBuildLatticeByLevel",
                #"timeDependencyPruning",
                #"timeTrivialityPruning",
                #"timeDistributionPruning",
                "timeDetectContextualOutliers",
                "timeMADNoOutliersContainedOutliersPruned",
                #"analyzeMs",
                #"summarizeMs",
                #"numDensityPruningsUsingAll",
                #"numDependencyPruningsUsingAll",
                #"numApproximateDependencyPruningsUsingAll",
                "numDensityPruning",
                "numTrivialityPruning",
                "numContextContainedInOutliersPruning",
                
                "numContextsGenerated",
                "numMadNoOutliers",
                "numMadContainedOutliers",
                "numContextsGeneratedWithMaximalOutliers",

                #"numDistributionPrunings",
                #"numContextsGeneratedWithOutliers",
                #"numContextsGeneratedWithOutOutliers",
                "precision",
                "recall",
                "precisionRelaxed",
                "recallRelaxed"]

context_desc_to_context = dict()

def read_contextual_outliers_multiple_runs(dir1, print_max_contextual_outliers = False):
    result_list = list()
    for i in range(0,10):
        output1 = os.path.join(dir1, "output%d.txt" % i)
        if os.path.isfile(output1):
             result = read_contextual_outliers(dir1, i, print_max_contextual_outliers)
             result_list.append(result)
    return result_list
    
def read_contextual_outliers(dir1, run_number, print_max_contextual_outliers = False):
    output1 = os.path.join(dir1, "output%d.txt" % run_number)
    f1 = open(output1,'r')
    lines1 = f1.read().splitlines()
    f1.close()
    
    lenght = len(lines1)
    
    #a dictionary from tuple to a list of contexts
    result = dict()
    for i in range(0,len(lines1)/5):
        context_desc = lines1[i*5]
        context_line = lines1[i*5 + 3]
        outlier_line = lines1[i*5 + 4]
        temp = context_line[1:len(context_line)-1].split(",")
        context = [x.strip() for x in context_line[1:len(context_line)-1].split(",")  ]
        outliers = [x.strip() for x in outlier_line[1: len(outlier_line) -1].split(",") ]
        
        context_desc_to_context[context_desc] = context
        for outlier in outliers:
            if result.has_key(outlier):
                result[outlier].append(context_desc)
            else :
                temp = list()
                temp.append(context_desc)
                result[outlier] = temp
    print "done reading all contexts outliers from " + output1  
    if print_max_contextual_outliers:
        print_ground_truth_contextual_outliers(result, os.path.join(dir1, "print_contextual_outliers.txt"));          
    # get the kernel of the dictionary
    for t in result.keys():
        # for every tuple, get the maximum contexts
        contextDescs = result[t]
        contextDescscontextsSorted = sorted(contextDescs, key=lambda x: len(context_desc_to_context[x]))
        
        #print "Tuple " + t + " has contexts before removed " + str(len(contexts))  
        toBeRemoved = list()
        for i in range(0,len(contextDescscontextsSorted)):
            removeC = False
            for j in range(i+1, len(contextDescscontextsSorted)):
                if subContexts(contextDescscontextsSorted[i], contextDescscontextsSorted[j]):
                    removeC = True
                    break
            if removeC:
                toBeRemoved.append(contextDescscontextsSorted[i])
            
        for re in toBeRemoved:
            result[t].remove(re)      
         
        #print "Tuple " + t + " has contexts after removed " + str(len(contexts))         
    print "done extracting kernel outliers from " + output1            
    if print_max_contextual_outliers:
        print_ground_truth_contextual_outliers(result, os.path.join(dir1, "print_max_contextual_outliers.txt"));          
    
    return result


def print_ground_truth_contextual_outliers(result_reference, output_file_path):
    #collect context that has outliers
    max_context_2_outliers = dict()
    for t in result_reference.keys():
        for context_reference in result_reference[t]:
            if max_context_2_outliers.has_key(context_reference):
                max_context_2_outliers[context_reference].append(t)
            else:
                temp = list()
                temp.append(t)
                max_context_2_outliers[context_reference] = temp
    #collect context that has outliers
    out = open(output_file_path, 'w')
    for context_reference in max_context_2_outliers.keys():
        temp = max_context_2_outliers[context_reference]
        out.write(context_reference + "\t" + str(len(context_desc_to_context[context_reference])) + "\t" + str(len(temp)) + "\n")
    out.close()
    return;
   
def compare_quality(result_current, result_reference):
   #compute precision
   count_current = 0
   count_current_correct = 0
   for t in result_current.keys():
       for context in result_current[t]:
           count_current = count_current + 1
           correct = False
           if result_reference.has_key(t):
               for context_reference in result_reference[t]:
                   if subContexts(context_reference, context):
                       correct = True
                       break
           if correct:
               count_current_correct = count_current_correct + 1
           '''
           else:
               print "Lost in Precision Tuple: " + t + " \nCurrent Context: " + context + " \nReference Context: " + "||\n".join(result_reference[t])
               for context_reference in result_reference[t]:
                   print str(subContexts(context, context_reference))
               
               print "--------------"
           '''
           
   print "count_current_correct: " + str(count_current_correct)
   print "count_current: " + str(count_current)
   precision = float(count_current_correct) / float(count_current)
   print "Precision: " + str(precision)
   #compute recall
   
   count_reference = 0
   count_reference_covered = 0
   for t in result_reference.keys():
       for context_reference in result_reference[t]:
           count_reference = count_reference + 1
           covered = False
           if result_current.has_key(t):
               for context in result_current[t]:
                   if subContexts(context_reference, context):
                       covered= True
                       break
           if covered:
               count_reference_covered = count_reference_covered + 1
   recall = float(count_reference_covered) / float(count_reference)
   print "count_reference_covered: " + str(count_reference_covered)
   print "count_reference: " + str(count_reference)
   print "Recall: " + str(recall)
   return precision, recall
    
def compare_quality_relaxed(result_current, result_reference):
   #compute precision
   count_current = 0
   count_current_correct = 0
   for t in result_current.keys():
       for context in result_current[t]:
           count_current = count_current + 1
           correct_score = 0
           if result_reference.has_key(t):
               for context_reference in result_reference[t]:
                   correct_score_this = subContextsRelax(context_reference, context)
                   if correct_score_this > correct_score:
                       correct_score = correct_score_this
                       if correct_score == 1.0:
                           break
           count_current_correct = count_current_correct + correct_score
           
   print "relaxed_count_current_correct: " + str(count_current_correct)
   print "relaxed_count_current: " + str(count_current)
   precision = float(count_current_correct) / float(count_current)
   print "relaxed_Precision: " + str(precision)
   #compute recall
   
   count_reference = 0
   count_reference_covered = 0
   for t in result_reference.keys():
       for context_reference in result_reference[t]:
           count_reference = count_reference + 1
           covered_score = 0
           if result_current.has_key(t):
               for context in result_current[t]:
                   covered_score_this = subContextsRelax(context_reference, context)
                   if covered_score_this > covered_score:
                       covered_score = covered_score_this
                       if covered_score == 1.0:
                            break
           
           count_reference_covered = count_reference_covered + covered_score
   recall = float(count_reference_covered) / float(count_reference)
   print "relaxed_count_reference_covered: " + str(count_reference_covered)
   print "relaxed_count_reference: " + str(count_reference)
   print "relaxed_Recall: " + str(recall)
   return precision, recall
# is list1 a subset of list2
# both list1 and list2 are ordered, and have distinct elements
sub_contexts_dict = dict()

def subContextsRelax(context_desc_1, context_desc_2):
    
    context1 = context_desc_to_context[context_desc_1]
    context2 = context_desc_to_context[context_desc_2]
    set1 = set(context1)
    set2 = set(context2)
    
    set1_and_set2 = set1 & set2
    set1_or_set2 = set1 | set2
    
    overlap =  float(len(set1_and_set2)) / float(len(set1_or_set2))
    if overlap > 0.9:
        return overlap
    else:
        return 0
    

def subContexts(context_desc_1, context_desc_2):
    
    if sub_contexts_dict.has_key(context_desc_1) == False:
        inner_dict = dict()
        sub_contexts_dict[context_desc_1] = inner_dict
    
    inner_dict = sub_contexts_dict[context_desc_1]    
    if inner_dict.has_key(context_desc_2):
            return sub_contexts_dict[context_desc_1][context_desc_2]
    else:
        context1 = context_desc_to_context[context_desc_1]
        context2 = context_desc_to_context[context_desc_2]
    
        contained = set(context1) <= set(context2)
        inner_dict[context_desc_2] = contained
        return contained
        

def compute_precision_recall(records, query_name, transform_type,
                                   exit_status, loader_type, 
                                   density_tau,
                                   pruning_dependency, pruning_triviality, pruning_triviality_approximation,pruning_triviality_approximation_propagation,
                         pruning_distribution,
                         pruning_distribution_test_mean,pruning_distribution_test_variance,pruning_distribution_test_ks):
    #first step is identify reference record
    reference_record = None
    for record in records:
        if record["Query Name"] != query_name:
            continue
        if record["macrobase.analysis.transformType"] != transform_type:
            continue;
        if exit_status not in record["exit_status"]:
            continue
        if record["macrobase.loader.loaderType"] != loader_type:
            continue
        if record["macrobase.analysis.contextual.denseContextTau"] != density_tau:
            continue
        if record["macrobase.analysis.contextual.pruning.dependency"] != pruning_dependency:
            continue
        if record["macrobase.analysis.contextual.pruning.triviality"] != pruning_triviality:
            continue
        if record["macrobase.analysis.contextual.pruning.triviality.approximation"] != pruning_triviality_approximation:
            continue
        if record["macrobase.analysis.contextual.pruning.triviality.approximation.propagate"] != pruning_triviality_approximation_propagation:
            continue;
        if record["macrobase.analysis.contextual.pruning.distribution"] != pruning_distribution:
            continue
        if record["macrobase.analysis.contextual.pruning.distribution.testmean"] != pruning_distribution_test_mean:
            continue
        if record["macrobase.analysis.contextual.pruning.distribution.testvariance"] != pruning_distribution_test_variance:
            continue
        if record["macrobase.analysis.contextual.pruning.distribution.testks"] != pruning_distribution_test_ks:
            continue
        reference_record = record
        break
    reference_result_list = read_contextual_outliers_multiple_runs(reference_record["ResultDir"], True)
    #refernece_result should be the same
    reference_result = reference_result_list[0]
    for record in records:
        if record["Query Name"] != query_name:
            continue
        if record["macrobase.analysis.transformType"] != transform_type:
            continue;
        if exit_status not in record["exit_status"]:
            continue
        if record["macrobase.analysis.contextual.denseContextTau"] != density_tau:
            continue
        current_result_list = read_contextual_outliers_multiple_runs(record["ResultDir"])
        if "07-13-09:36:41" in record["ResultDir"]:
            current_result_list = read_contextual_outliers_multiple_runs(record["ResultDir"],True)
        precision_list = list()
        recall_list = list()
        for current_result in current_result_list:
            precision, recall = compare_quality(current_result, reference_result)
            precision_list.append(str(precision))
            recall_list.append(str(recall))
        record["precision"] = "|".join(precision_list) 
        record["recall"] = "|".join(recall_list)
        '''
        precision_relaxed, recall_relaxed = compare_quality_relaxed(current_result, reference_result)
        record["precisionRelaxed"] = str(precision_relaxed)
        record["recallRelaxed"] = str(recall_relaxed)
        '''
def plot_time_precision_recall_vs_x_axix(records, 
                                              query_name,transform_type,exit_status,loader_type, 
                                              density_tau,
                                              pruning_dependency, 
                                              pruning_triviality,
                                              pruning_triviality_approximation,
                                              pruning_triviality_approximation_propagate,
                                              pruning_distribution,
                                              pruning_distribution_samplesize,
                                              pruning_distribution_alpha,
                                              pruning_distribution_test_mean,
                                              pruning_distribution_test_variance,
                                              pruning_distribution_test_ks,
                                              which_x_axix,
                                              output_file_path):
    
    x_list, execute_time_list, time_lattice_building_list,  time_dependency_pruning_list, time_distribution_pruning_list, time_detection_outliers_list, precision_list, recall_list = get_time_precision_recall_vs_x_axix(records, 
                                              query_name,transform_type,exit_status,loader_type, 
                                              density_tau,
                                              pruning_dependency, 
                                              pruning_triviality,
                                              pruning_triviality_approximation,
                                              pruning_triviality_approximation_propagate,
                                              pruning_distribution,
                                              pruning_distribution_samplesize,
                                              pruning_distribution_alpha,
                                              pruning_distribution_test_mean,
                                              pruning_distribution_test_variance,
                                              pruning_distribution_test_ks,
                                              which_x_axix,
                                              output_file_path)
    
    if which_x_axix == "macrobase.analysis.contextual.pruning.distribution.test":
        out = open(output_file_path, 'w')
        out.write("test_mean, test_variance, test_ks, precision, recall, execution time, lattice building time, parent triviality time,  distribution pruning time, outlier detection time\n")
        for x, precision, recall, execute_time, lattice_time, parent_triviality_time, distribution_pruning_time, outlier_detection_time in zip(x_list, precision_list, recall_list, execute_time_list, time_lattice_building_list, time_dependency_pruning_list, time_distribution_pruning_list, time_detection_outliers_list):
            out.write(x  + "," + str(precision) + "," + str(recall) + "," + str(execute_time) + "," + str(lattice_time) +"," +str(parent_triviality_time) + "," +  str(distribution_pruning_time) + "," +  str(outlier_detection_time) +  "\n")
        out.close()
        return
    
    
    y_values_list = list()
    y_values_list.append(execute_time_list)
    y_values_list.append(list())
    y_values_list.append(precision_list)
    y_values_list.append(recall_list)
    y_labels = list()
    y_labels.append("execution time")
    y_labels.append("time breakdown")
    y_labels.append("precision")
    y_labels.append("recall")
    
    fig, axes = plt.subplots(nrows=4)
    colors = ('c', 'r', 'b', 'g')
    
    ind = np.arange(len(x_list))  # the x locations for the groups
   
    
    for ax, color, y_values,y_label in zip(axes, colors, y_values_list,y_labels):
        if y_label == "time breakdown":
            ax.bar(ind, time_lattice_building_list, width = 0.35, color='b')
            ax.bar(ind, time_dependency_pruning_list, width = 0.35, bottom=time_lattice_building_list, color='g')
            ax.bar(ind, time_distribution_pruning_list, width = 0.35, bottom=[sum(x) for x in zip(time_lattice_building_list, time_dependency_pruning_list)], color='r')
            ax.bar(ind, time_detection_outliers_list, width = 0.35, bottom=[sum(x) for x in zip(time_lattice_building_list, time_dependency_pruning_list,time_distribution_pruning_list)], color='c')
            ax.set_xticklabels(["" for x in ind])
        else:
            ax.plot(x_list,y_values, marker='o', linestyle='-', color=color)
            
        ax.set_ylabel(y_label)
        if y_label == "recall":
            ax.set_xlabel(which_x_axix)
    
    plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    plt.savefig(output_file_path , bbox_inches='tight')
    plt.close()
    
def plot_time_precision_recall_vs_x_axix_witho_approximation_propagate(records, 
                                              query_name,transform_type,exit_status,loader_type, 
                                              density_tau,
                                              pruning_dependency, 
                                              pruning_triviality,
                                              pruning_triviality_approximation,
                                              #pruning_triviality_approximation_propagate,
                                              pruning_distribution,
                                              pruning_distribution_samplesize,
                                              pruning_distribution_alpha,
                                              pruning_distribution_test_mean,
                                              pruning_distribution_test_variance,
                                              pruning_distribution_test_ks,
                                              which_x_axix,
                                              output_file_path):
    
    (true_x_list, 
    true_execute_time_list, 
    true_time_lattice_building_list,  
    true_time_dependency_pruning_list, 
    true_time_distribution_pruning_list, 
    true_time_detection_outliers_list, 
    true_precision_list, 
    true_recall_list) = get_time_precision_recall_vs_x_axix(records, 
                                              query_name,transform_type,exit_status,loader_type, 
                                              density_tau,
                                              pruning_dependency, 
                                              pruning_triviality,
                                              pruning_triviality_approximation,
                                              "True",
                                              pruning_distribution,
                                              pruning_distribution_samplesize,
                                              pruning_distribution_alpha,
                                              pruning_distribution_test_mean,
                                              pruning_distribution_test_variance,
                                              pruning_distribution_test_ks,
                                              which_x_axix,
                                              output_file_path)
    (false_x_list, 
    false_execute_time_list, 
    false_time_lattice_building_list,  
    false_time_dependency_pruning_list, 
    false_time_distribution_pruning_list, 
    false_time_detection_outliers_list, 
    false_precision_list, 
    false_recall_list) = get_time_precision_recall_vs_x_axix(records, 
                                              query_name,transform_type,exit_status,loader_type, 
                                              density_tau,
                                              pruning_dependency, 
                                              pruning_triviality,
                                              pruning_triviality_approximation,
                                              "False",
                                              pruning_distribution,
                                              pruning_distribution_samplesize,
                                              pruning_distribution_alpha,
                                              pruning_distribution_test_mean,
                                              pruning_distribution_test_variance,
                                              pruning_distribution_test_ks,
                                              which_x_axix,
                                              output_file_path)
    
    true_y_values_list = list()
    true_y_values_list.append(true_execute_time_list)
    true_y_values_list.append(true_precision_list)
    true_y_values_list.append(true_recall_list)
    
    false_y_values_list = list()
    false_y_values_list.append(false_execute_time_list)
    false_y_values_list.append(false_precision_list)
    false_y_values_list.append(false_recall_list)
   
    y_labels = list()
    y_labels.append("execution time")
    y_labels.append("precision")
    y_labels.append("recall")
    
    fig, axes = plt.subplots(nrows=3)
    colors = ('c', 'b', 'g')
    
    
    
    for ax, color, true_y_values, false_y_values, y_label in zip(axes, colors, true_y_values_list,false_y_values_list ,y_labels):
       
        ax.plot(true_x_list,true_y_values, marker='o', linestyle='-', color=color)
        ax.plot(false_x_list,false_y_values, marker='s', linestyle='--', color=color)
            
        ax.set_ylabel(y_label)
        if y_label == "recall":
            ax.set_xlabel(which_x_axix)
    
    plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    plt.savefig(output_file_path , bbox_inches='tight')
    plt.close()
        
def get_time_precision_recall_vs_x_axix(records, 
                                              query_name,transform_type,exit_status,loader_type, 
                                              density_tau,
                                              pruning_dependency, 
                                              pruning_triviality,
                                              pruning_triviality_approximation,
                                              pruning_triviality_approximation_propagate,
                                              pruning_distribution,
                                              pruning_distribution_samplesize,
                                              pruning_distribution_alpha,
                                              pruning_distribution_test_mean,
                                              pruning_distribution_test_variance,
                                              pruning_distribution_test_ks,
                                              which_x_axix,
                                              output_file_path):
    
    if which_x_axix != "macrobase.analysis.contextual.pruning.distribution.test":
        records.sort(key=lambda record: float(record[which_x_axix]))
    
    x_list = list()
    execute_time_list = list()
    time_lattice_building_list = list()
    time_dependency_pruning_list = list()
    time_distribution_pruning_list = list()
    time_detection_outliers_list = list()
    precision_list = list()
    recall_list = list()
    
    for record in records:
            if record["Query Name"] != query_name:
                continue
            if record["macrobase.analysis.transformType"] != transform_type:
                continue
            if exit_status not in record["exit_status"]:
                continue
            if record["macrobase.loader.loaderType"] != loader_type:
                continue
            if record["macrobase.analysis.contextual.denseContextTau"] != density_tau:
                continue
            if record["macrobase.analysis.contextual.pruning.dependency"] != pruning_dependency:
                continue
            if record["macrobase.analysis.contextual.pruning.triviality"] != pruning_triviality:
                continue
            if record["macrobase.analysis.contextual.pruning.triviality.approximation"] != pruning_triviality_approximation:
                if which_x_axix != "macrobase.analysis.contextual.pruning.triviality.approximation":
                    continue
            
            if record["macrobase.analysis.contextual.pruning.triviality.approximation.propagate"] != pruning_triviality_approximation_propagate:
                continue
                
            if record["macrobase.analysis.contextual.pruning.distribution"] != pruning_distribution:
                continue
           
            if record["macrobase.analysis.contextual.pruning.distribution.minsamplesize"] != pruning_distribution_samplesize:
                 if which_x_axix != "macrobase.analysis.contextual.pruning.distribution.minsamplesize":
                    continue
            
            if record["macrobase.analysis.contextual.pruning.distribution.alpha"] != pruning_distribution_alpha:
                 if which_x_axix != "macrobase.analysis.contextual.pruning.distribution.alpha":
                    continue
                
            if record["macrobase.analysis.contextual.pruning.distribution.testmean"] != pruning_distribution_test_mean:
                if which_x_axix != "macrobase.analysis.contextual.pruning.distribution.test":
                    continue
            if record["macrobase.analysis.contextual.pruning.distribution.testvariance"] != pruning_distribution_test_variance:
                if which_x_axix != "macrobase.analysis.contextual.pruning.distribution.test":
                    continue
            if record["macrobase.analysis.contextual.pruning.distribution.testks"] != pruning_distribution_test_ks:
                if which_x_axix != "macrobase.analysis.contextual.pruning.distribution.test":
                    continue
            
            
            
            if which_x_axix == "macrobase.analysis.contextual.pruning.distribution.test":
                x_point = record["macrobase.analysis.contextual.pruning.distribution.testmean"] + "," + record["macrobase.analysis.contextual.pruning.distribution.testvariance"] + "," + record["macrobase.analysis.contextual.pruning.distribution.testks"]
            else :
                x_point = record[which_x_axix]
            
            execute_time = get_mean(record["executeMs"])
            time_lattice_building = get_mean(record["timeBuildLattice"])
            time_dependency_pruning = get_mean(record["timeDependencyPruning"])
            time_distribution_pruning = get_mean(record["timeDistributionPruning"])
            time_detection_outliers = get_mean(record["timeDetectContextualOutliers"])
            precision = get_mean( record["precision"] )
            recall = get_mean(record["recall"])
            
            x_list.append(x_point)
            execute_time_list.append(execute_time)
            time_lattice_building_list.append(time_lattice_building)
            time_dependency_pruning_list.append(time_dependency_pruning)
            time_distribution_pruning_list.append(time_distribution_pruning)
            time_detection_outliers_list.append(time_detection_outliers)
            precision_list.append(precision)
            recall_list.append(recall)
    
    return (x_list, execute_time_list, time_lattice_building_list,  time_dependency_pruning_list, time_distribution_pruning_list, time_detection_outliers_list, precision_list, recall_list)


def get_mean(field):
    value_list = [float(x.strip()) for x in field.split('|') ]   
    mean = sum(value_list) / len(value_list)
    stddev = (sum([(value - mean)**2 for value in value_list]) /
              len(value_list)) ** 0.5
    return mean

def assemble_all_records(*files):
    records = list()
    for file in files:
        f = open(file,'r')
        lines = f.read().splitlines()
        
        key_value_dict = dict.fromkeys(keysList,"")
        
        for line in lines:
            if "--------------" in line:
                print "--------------"
                    
                values = [key_value_dict[x] for x in keysList]
                value_print = ",".join(values)
                print value_print
                
                records.append(key_value_dict)
                
                key_value_dict = dict.fromkeys(keysList,"")

            else : 
                splits = line.split(":")
                #keys.append(splits[0])
                #values.append(splits[1])
                keys = [x for x in keysList if x == splits[0].strip()]
                if len(keys) > 1:
                    raise Exception("There are more than one key matching")
                if len(keys) == 0:
                    continue
                    #raise Exception("There are no keys matching")
                #key_value_dict[keys[0]] = splits[1].strip()
                key_value_dict[keys[0]] = ":".join(splits[1:len(splits)]).strip()
        f.close()
    return records
def write_out_all_records(output_file_path, records):
    out = open(output_file_path, 'w')
    out.write(",".join(keysList) + "\n")
    for record in records:
        values = [record[x] for x in keysList]
        value_print = ",".join(values)
        out.write(value_print + "\n")
    out.close()
    
def read_in_all_records(output_file_path):
    records = list()
    
    f = open(output_file_path,'r')
    lines = f.read().splitlines()
    f.close()
    for i in range(0,len(lines)):
        if i == 0:
            continue
        line = lines[i]
        splits = line.split(",")
        key_value_dict = dict.fromkeys(keysList,"")
        for j in range(0,len(keysList)):
            key_value_dict[keysList[j]] = splits[j]
            if splits[j] == "TRUE":
                key_value_dict[keysList[j]] = "True"
            if splits[j] == "FALSE":
                key_value_dict[keysList[j]] = "False"
        records.append(key_value_dict)
        
    return records


def experiments_triviality(transform_type, density_tau):
    query_name_list = ["cmt100000", "marketing", "uk_road_accident", "fed_disbursements100000", "campaign_expenditures100000"]

    
    for i in range(0,len(query_name_list)):
        output_file = None
        if i == 0:
            output_file = "contextual_workflow_output_istc3.txt"
        else:
            output_file = "contextual_workflow_output_husky0" + str(i) + ".txt"
            
        if os.path.isfile(output_file) == False:
            continue
        
        records = assemble_all_records(output_file)
        
        query_name = query_name_list[i]
        write_out_all_records(str(i) + "_" + query_name + "_" + transform_type + "_approximate_triviality_output_table.csv", records)
        compute_precision_recall(records, 
                                       query_name = query_name, 
                                       transform_type = transform_type,
                                       exit_status = "Completed", 
                                       loader_type = "POSTGRES_LOADER", 
                                       density_tau = density_tau,
                                       #following parameters are for reference record
                                       pruning_dependency = "True", 
                                       pruning_triviality = "False", 
                                       pruning_triviality_approximation = "1.0", 
                                       pruning_triviality_approximation_propagation = "False", 
                                       pruning_distribution = "True",
                                       pruning_distribution_test_mean = "False",
                                       pruning_distribution_test_variance="False",
                                       pruning_distribution_test_ks="False")
        write_out_all_records(str(i) + "_" + query_name + "_" + transform_type + "_approximate_triviality_output_table_p_r.csv", records)
    
        
    
    for i in range(0,len(query_name_list)):
        query_name = query_name_list[i]

        if os.path.isfile(str(i) + "_" + query_name + "_" + transform_type + "_approximate_triviality_output_table_p_r.csv") == False:
            continue
        
        
        records = read_in_all_records(str(i) + "_" + query_name + "_" + transform_type + "_approximate_triviality_output_table_p_r.csv")
       
        plot_time_precision_recall_vs_x_axix(records, 
                                                  query_name=query_name,
                                                  transform_type = transform_type,
                                                  exit_status = "Completed", 
                                                   loader_type = "POSTGRES_LOADER", 
                                                   density_tau = density_tau,
                                                   pruning_dependency = "True", 
                                                   pruning_triviality = "False", 
                                                   pruning_triviality_approximation = None,
                                                   pruning_triviality_approximation_propagate = "False",
                                                   pruning_distribution = "True",
                                                   pruning_distribution_samplesize = "500", 
                                                   pruning_distribution_alpha = "0.05",
                                                   pruning_distribution_test_mean = "False",
                                                   pruning_distribution_test_variance="False",
                                                   pruning_distribution_test_ks="False",
                                                   which_x_axix = "macrobase.analysis.contextual.pruning.triviality.approximation",
                                                   output_file_path =  str(i) + "_" + query_name + "_"  + transform_type + "_approximate_triviality_no_propagate.pdf")
        plot_time_precision_recall_vs_x_axix(records, 
                                                  query_name=query_name,
                                                  transform_type = transform_type,
                                                  exit_status = "Completed", 
                                                   loader_type = "POSTGRES_LOADER", 
                                                   density_tau = density_tau,
                                                   pruning_dependency = "True", 
                                                   pruning_triviality = "False", 
                                                   pruning_triviality_approximation = None,
                                                   pruning_triviality_approximation_propagate = "True",
                                                   pruning_distribution = "True",
                                                   pruning_distribution_samplesize = "500", 
                                                   pruning_distribution_alpha = "0.05",
                                                   pruning_distribution_test_mean = "False",
                                                   pruning_distribution_test_variance="False",
                                                   pruning_distribution_test_ks="False",
                                                   which_x_axix = "macrobase.analysis.contextual.pruning.triviality.approximation",
                                                   output_file_path = str(i) + "_" + query_name + "_"  + transform_type + "_approximate_triviality_propagate.pdf")
        plot_time_precision_recall_vs_x_axix_witho_approximation_propagate(records, 
                                                  query_name=query_name,
                                                  transform_type = transform_type,
                                                  exit_status = "Completed", 
                                                   loader_type = "POSTGRES_LOADER", 
                                                   density_tau = density_tau,
                                                   pruning_dependency = "True", 
                                                   pruning_triviality = "False", 
                                                   pruning_triviality_approximation = None,
                                                   pruning_distribution = "True",
                                                   pruning_distribution_samplesize = "500", 
                                                   pruning_distribution_alpha = "0.05",
                                                   pruning_distribution_test_mean = "False",
                                                   pruning_distribution_test_variance="False",
                                                   pruning_distribution_test_ks="False",
                                                   which_x_axix = "macrobase.analysis.contextual.pruning.triviality.approximation",
                                                   output_file_path = str(i) + "_" + query_name + "_"  + transform_type + "_approximate_triviality_compare_propagate.pdf")
 
 
def experiments_distribution(transform_type,density_tau):
    query_name_list = ["cmt100000", "marketing", "uk_road_accident", "fed_disbursements100000", "campaign_expenditures100000"]

    
    for i in range(0,len(query_name_list)):
        query_name = query_name_list[i]
        output_file = None
        
        if i == 0:
            output_file = "contextual_workflow_output_istc3.txt"
        else:
            output_file = "contextual_workflow_output_husky0" + str(i) + ".txt"
            
        if os.path.isfile(output_file) == False:
            continue
        
        records = assemble_all_records(output_file)
        write_out_all_records(str(i) + "_" + query_name + "_" + transform_type + "_distribution_output_table.csv", records)
        compute_precision_recall(records, 
                                       query_name = query_name, 
                                       transform_type = transform_type,
                                       exit_status = "Completed", 
                                       loader_type = "POSTGRES_LOADER", 
                                       density_tau = density_tau,
                                       #following parameters are for reference record
                                       pruning_dependency = "True", 
                                       pruning_triviality = "False", 
                                       pruning_triviality_approximation = "1.0", 
                                       pruning_triviality_approximation_propagation = "False", 
                                       pruning_distribution = "True",
                                       pruning_distribution_test_mean = "False",
                                       pruning_distribution_test_variance="False",
                                       pruning_distribution_test_ks="False")
        write_out_all_records(str(i) + "_" + query_name + "_" + transform_type + "_distribution_output_table_p_r.csv", records)
    
        
    
    for i in range(0,len(query_name_list)):
        query_name = query_name_list[i]
        
        if os.path.isfile(str(i) + "_" + query_name + "_" + transform_type + "_distribution_output_table_p_r.csv") == False:
            continue
        
        records2 = read_in_all_records(str(i) + "_" + query_name + "_" + transform_type +"_distribution_output_table_p_r.csv")
        
        ##Varying distribution pruning knobs
        pruning_distribution = "True"
        pruning_distribution_samplesize = "5000"
        pruning_distribution_alpha = "0.5"
        pruning_distribution_test_mean = None
        pruning_distribution_test_variance= None
        pruning_distribution_test_ks= None
        plot_time_precision_recall_vs_x_axix(records2, 
                                                  query_name=query_name,
                                                  transform_type = transform_type,
                                                  exit_status = "Completed", 
                                                   loader_type = "POSTGRES_LOADER", 
                                                   density_tau = density_tau,
                                                   pruning_dependency = "True", 
                                                   pruning_triviality = "False", 
                                                   pruning_triviality_approximation = "1.0", 
                                                   pruning_triviality_approximation_propagate = "False",
                                                   pruning_distribution = pruning_distribution,
                                                   pruning_distribution_samplesize = pruning_distribution_samplesize,
                                                   pruning_distribution_alpha = pruning_distribution_alpha,
                                                   pruning_distribution_test_mean = pruning_distribution_test_mean,
                                                   pruning_distribution_test_variance= pruning_distribution_test_variance,
                                                   pruning_distribution_test_ks=pruning_distribution_test_ks,
                                                   which_x_axix = "macrobase.analysis.contextual.pruning.distribution.test",
                                                   output_file_path = str(i) + 
                                                   "_" + query_name + 
                                                   "_" + transform_type +
                                                   "_" + str(pruning_distribution_samplesize) + 
                                                   "_" + pruning_distribution_alpha + 
                                                   "_" + str(pruning_distribution_test_mean) + 
                                                   "_" + str(pruning_distribution_test_variance) + 
                                                   "_" + str(pruning_distribution_test_ks) + 
                                                   "_distribution_whichtest.csv")
        
        
        ##Varying sample size
        pruning_distribution = "True"
        pruning_distribution_samplesize = None
        pruning_distribution_alpha = "0.5"
        pruning_distribution_test_mean = "True"
        pruning_distribution_test_variance="True"
        pruning_distribution_test_ks="True"
        plot_time_precision_recall_vs_x_axix(records2, 
                                                  query_name=query_name,
                                                  transform_type = transform_type,
                                                  exit_status = "Completed", 
                                                   loader_type = "POSTGRES_LOADER", 
                                                   density_tau = density_tau,
                                                   pruning_dependency = "True", 
                                                   pruning_triviality = "False", 
                                                   pruning_triviality_approximation = "1.0", 
                                                   pruning_triviality_approximation_propagate = "False",
                                                   pruning_distribution = pruning_distribution,
                                                   pruning_distribution_samplesize = pruning_distribution_samplesize,
                                                   pruning_distribution_alpha = pruning_distribution_alpha,
                                                   pruning_distribution_test_mean = pruning_distribution_test_mean,
                                                   pruning_distribution_test_variance= pruning_distribution_test_variance,
                                                   pruning_distribution_test_ks=pruning_distribution_test_ks,
                                                   which_x_axix = "macrobase.analysis.contextual.pruning.distribution.minsamplesize",
                                                   output_file_path = str(i) + 
                                                   "_" + query_name + 
                                                   "_" + transform_type +
                                                   "_" + str(pruning_distribution_samplesize) + 
                                                   "_" + pruning_distribution_alpha + 
                                                   "_" + pruning_distribution_test_mean + 
                                                   "_" + pruning_distribution_test_variance + 
                                                   "_" + pruning_distribution_test_ks + 
                                                   "_distribution_minsamplesize.pdf")
        
        ##Varying significance level
        pruning_distribution = "True"
        pruning_distribution_samplesize = "5000"
        pruning_distribution_alpha = None
        pruning_distribution_test_mean = "True"
        pruning_distribution_test_variance="True"
        pruning_distribution_test_ks="True"                                    
        plot_time_precision_recall_vs_x_axix(records2, 
                                                  query_name=query_name,
                                                  transform_type = transform_type,
                                                  exit_status = "Completed", 
                                                   loader_type = "POSTGRES_LOADER", 
                                                   density_tau = density_tau,
                                                   pruning_dependency = "True", 
                                                   pruning_triviality = "False", 
                                                   pruning_triviality_approximation = "1.0", 
                                                   pruning_triviality_approximation_propagate = "False",
                                                   pruning_distribution = pruning_distribution,
                                                   pruning_distribution_samplesize = pruning_distribution_samplesize,
                                                   pruning_distribution_alpha = pruning_distribution_alpha,
                                                   pruning_distribution_test_mean = pruning_distribution_test_mean,
                                                   pruning_distribution_test_variance= pruning_distribution_test_variance,
                                                   pruning_distribution_test_ks=pruning_distribution_test_ks,
                                                   which_x_axix="macrobase.analysis.contextual.pruning.distribution.alpha",
                                                   output_file_path = str(i) + 
                                                   "_" + query_name + 
                                                   "_" + transform_type +
                                                   "_" + pruning_distribution_samplesize + 
                                                   "_" + str(pruning_distribution_alpha) + 
                                                   "_" + pruning_distribution_test_mean + 
                                                   "_" + pruning_distribution_test_variance + 
                                                   "_" + pruning_distribution_test_ks + 
                                                   "_distribution_confidencelevel.pdf")

def inverse_contextual_exp(directory, whichy):
    
    datasets = ["marketing", "uk_road_accidents", "fed_disbursements", "campaign_expenditures", "cmt"]
    legends = ["Marketing", "Accidents", "Disburse", "Campaign", "CMT"]
    linestyles = ['', '-', '--', '-.', ':']
    dataset2y_values = dict()
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.tsv'):
                fileName = os.path.join(root, file)
                if "output0.txt_inverse_varying_num_suspicious_tuples.tsv" in fileName:
                    print fileName
                    
                    x_values = list()
                    y_values = list()
                    legend= None
                        
                    with open(fileName) as f:
                        contentLines = f.readlines()
                        for i in range(1, len(contentLines)):
                            splits = contentLines[i].split("\t")
                            x_values.append(int(splits[0]))
                            if whichy == 1:
                                y_values.append(float(splits[1]) / 1000)
                            elif whichy == 2:
                                y_values.append(float(splits[4]))
                            elif whichy == 3:
                                y_values.append(float(splits[6]))
                            elif whichy == 4:
                                y_values.append(float(splits[4])/( float(splits[6]) + 1))
                    for dataset in datasets:
                        if dataset in fileName:
                            dataset2y_values[dataset] = y_values
                                
                                
    for i in range(0,len(datasets)):
         plt.plot(x_values,dataset2y_values[datasets[i]],linestyles[i],label=legends[i],linewidth=4.0)               
                    
    
    y_label = None;
    if whichy == 1:
        y_label = "Time"
    elif whichy == 2:
        y_label = "Number of Contexts"
    elif whichy == 3:
        y_label = "Number of Contexts with Maximal Contextual Outliers"
    elif whichy == 4:
        y_label = "Yield"

    legend = plt.legend(loc='upper left', shadow=True,fontsize=18)
            
    plt.xlabel('Number of Suspicious Tuples', fontsize=20)
    plt.ylabel(y_label,fontsize=20)    
    
    plt.rc('xtick', labelsize=20)  # fontsize of the tick labels
    plt.rc('ytick', labelsize=20)  # fontsize of the tick labels

    #plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    picPath = None;
    if whichy == 1:
       picPath = directory + "inverse_time_varying_number_suspicious_tuples_.pdf"
    elif whichy == 2:
       picPath = directory + "inverse_number_contexts_varying_number_suspicious_tuples_.pdf"
    elif whichy == 3:
       picPath = directory + "inverse_number_contexts_with_maximal_outliers_varying_number_suspicious_tuples_.pdf"
    elif whichy == 4:
       picPath = directory + "inverse_yield_varying_number_suspicious_tuples_.pdf"
    plt.savefig(picPath , bbox_inches='tight')
    plt.legend()
    plt.close()
    
def inverse_contextual_exp_per_trial(directory, whichx):
    
    datasets = ["marketing", "uk_road_accidents", "fed_disbursements", "campaign_expenditures", "cmt"]
    #datasets = ["fed_disbursements"]
   
    legends = ["Marketing", "Accidents", "Disburse", "Campaign", "CMT"]
    linestyles = ['', '-', '--', '-.', ':']
    dataset2x_values = dict()
    dataset2y_values = dict()
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.tsv'):
                fileName = os.path.join(root, file)
                if "output0.txt_inverse_experiment_1_outliers_trials.tsv" in fileName:
                    print fileName
                    
                    x_values = list()
                    y_values = list()
                    legend= None
                        
                    with open(fileName) as f:
                        contentLines = f.readlines()
                        for i in range(1, len(contentLines)):
                            splits = contentLines[i].split("\t")
                            y_values.append( float(splits[1]) / 1000)
                            if whichx == 1:
                                x_values.append(float(splits[4]))
                            elif whichx == 2:
                                x_values.append(float(splits[6]))
                            elif whichx == 3:
                                x_values.append(float(splits[4])/( float(splits[6]) + 1))
                    for dataset in datasets:
                        if dataset in fileName:
                            dataset2x_values[dataset] = x_values[0:200]
                            dataset2y_values[dataset] = y_values[0:200]
                                
    colors = ['r','b','g','c','m']                           
    markers = ['+','s','x','D','o']
    for i in range(0,len(datasets)):
        plt.scatter(dataset2x_values[datasets[i]], dataset2y_values[datasets[i]],label=legends[i], c = colors[i],marker=markers[i])               

    
    x_label = None;
    if whichx == 1:
        x_label = "Number of Contexts"
    elif whichx == 2:
        x_label = "Number of Contexts with Maximal Contextual Outliers"
    elif whichx == 3:
        x_label = "Yield"

    legend = plt.legend(loc='upper left', shadow=True,fontsize=18)
            
    plt.xlabel(x_label, fontsize=20)
    plt.ylabel("Runtime (s)",fontsize=20)    
    
    plt.rc('xtick', labelsize=20)  # fontsize of the tick labels
    plt.rc('ytick', labelsize=20)  # fontsize of the tick labels

    #plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    picPath = None;
    if whichx == 1:
       picPath = directory + "inverse_time_vs_number_contexts_varying_trials.pdf"
    elif whichx == 2:
       picPath = directory + "inverse_time_vs_number_contexts_with_maximal_outliers_varying_trials.pdf"
    elif whichx == 3:
       picPath = directory + "inverse_time_vs_yield_varying_trials.pdf"
    plt.savefig(picPath , bbox_inches='tight')
    plt.legend()
    plt.close()                    
                    
def experiments_settings(transform_type,density_tau):
    query_name_list = ["cmt100000", "marketing", "uk_road_accident", "fed_disbursements100000", "campaign_expenditures100000"]

    current_time = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    
    for i in range(0,len(query_name_list)):
        output_file = None
        if i == 0:
            output_file = "contextual_workflow_output_istc3.txt"
        else:
            output_file = "contextual_workflow_output_husky0" + str(i) + ".txt"
            
        if os.path.isfile(output_file) == False:
            continue
        
        records = assemble_all_records(output_file)
        
        query_name = query_name_list[i]
        write_out_all_records(current_time + "_" + str(i) + "_" + query_name + "_" + transform_type + ".csv", records)



def varying_theta(directory, whichy):
    
    datasets = ["marketing", "uk_road_accident", "fed_disbursements", "campaign_expenditures", "cmt"]
    legends = ["Marketing", "Accidents", "Disburse", "Campaign", "CMT"]
    linestyles = ['', '-', '--', '-.', ':']
    dataset2y_values = dict()
    
    x_values= None
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                fileName = os.path.join(root, file)
                if ".csv" in fileName and "vary_theta" in fileName:
                    print fileName
                    
                    x_values = list()
                    y_values = list()
                    legend= None
                        
                    with open(fileName) as f:
                        contentLines = f.readlines()
                        for i in range(1, len(contentLines)):
                            splits = contentLines[i].split(",")
                            x_values.append(int(splits[6]))
                            if whichy == 1:
                                y_values.append(float(splits[12]) / 1000)  ##running time
                            elif whichy == 2:
                                y_values.append(float(splits[19])) ## number of generated contexts
                            elif whichy == 3:
                                y_values.append(float(splits[22])) ## number of contexts with maximal outliers
                           
                    for dataset in datasets:
                        if dataset in fileName:
                            dataset2y_values[dataset] = y_values
                                
    colors = ['r','b','g','c','m']   
    markers = ['+','s','x','D','o']                           
    for i in range(0,len(datasets)):
         plt.plot(x_values,dataset2y_values[datasets[i]],linestyles[i],label=legends[i],linewidth=4.0,c=colors[i])               
                    
    
    y_label = None;
    if whichy == 1:
        y_label = "Runntime (s)"
    elif whichy == 2:
        y_label = "Number of Contexts"
    elif whichy == 3:
        y_label = "Result Size"
    

    legend = plt.legend(loc='upper right', shadow=True,fontsize=18)
            
    plt.xlabel('Hampel X84 threshold', fontsize=20)
    plt.ylabel(y_label,fontsize=20)    
    
    plt.rc('xtick', labelsize=20)  # fontsize of the tick labels
    plt.rc('ytick', labelsize=20)  # fontsize of the tick labels

    #plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    picPath = None;
    if whichy == 1:
       picPath = directory + "varying_theta_time.pdf"
    elif whichy == 2:
       picPath = directory + "varying_theta_number_of_contexts.pdf"
    elif whichy == 3:
       picPath = directory + "varying_theta_number_of_maximal_contexts.pdf"
   
    plt.savefig(picPath , bbox_inches='tight')
    plt.legend()
    plt.close()       
        
        
        
        
if __name__ == '__main__':
    #inverse_contextual_exp("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",1);
    #inverse_contextual_exp("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",2);
    #inverse_contextual_exp("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",3);
    #inverse_contextual_exp("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",4);
    inverse_contextual_exp_per_trial("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",1)
    inverse_contextual_exp_per_trial("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",2)
    inverse_contextual_exp_per_trial("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",3)
    varying_theta("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",1)
    varying_theta("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",2)
    varying_theta("/Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/",3)

    #experiments_settings("MAD","0.01")
    