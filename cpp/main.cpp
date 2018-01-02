#include <assert.h>
#include <readline/history.h>
#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <roaring.hh>
#include "compare_by.h"
#include "io_util.h"
#include "sql-parser/src/SQLParser.h"
#include "time_util.h"
#include "types.h"
#include "util.h"

#ifdef DEBUG
#include "sql-parser/src/util/sqlhelper.h"
#endif

using std::cout;
using std::endl;
using std::map;
using std::min;
using std::set;
using std::string;
using std::tuple;
using std::vector;

void diff(hsql::DiffDefinition* diff, const vector<Row>& input,
          const map<string, uint32_t>& input_schema, vector<Row>& output,
          map<string, uint32_t>& output_schema);

void apply_where_predicates(hsql::Expr* whereClause, const vector<Row>& input,
                            const map<string, uint32_t>& input_schema,
                            vector<Row>& output) {
  const hsql::OperatorType op = whereClause->opType;
  const string column_name = whereClause->expr->name;
  // TODO: handle multiple types
  const float val = whereClause->expr2->fval;
  const uint32_t column_ind = input_schema.at(column_name);
#ifdef DEBUG
  cout << "Where Operator Type: " << op << endl;
  cout << "Where Column: " << column_name << endl;

  cout << "Where Value: " << val << endl;
#endif

  for (auto i = 0u; i < input.size(); ++i) {
    Row row = input[i];
    switch (op) {
      case hsql::kOpEquals:
        if (std::stof(row[column_ind]) == val) {
          output.push_back(row);
        }
        break;
      case hsql::kOpNotEquals:
        if (std::stof(row[column_ind]) != val) {
          output.push_back(row);
        }
        break;
      case hsql::kOpLess:
        if (std::stof(row[column_ind]) < val) {
          output.push_back(row);
        }
        break;
      case hsql::kOpLessEq:
        if (std::stof(row[column_ind]) <= val) {
          output.push_back(row);
        }
        break;
      case hsql::kOpGreater:
        if (std::stof(row[column_ind]) > val) {
          output.push_back(row);
        }
        break;
      case hsql::kOpGreaterEq:
        if (std::stof(row[column_ind]) >= val) {
          output.push_back(row);
        }
        break;

      case hsql::kOpNone:
      // Ternary operators
      case hsql::kOpBetween:
      case hsql::kOpCase:
      // Binary operators.
      case hsql::kOpPlus:
      case hsql::kOpMinus:
      case hsql::kOpAsterisk:
      case hsql::kOpSlash:
      case hsql::kOpPercentage:
      case hsql::kOpCaret:

      case hsql::kOpLike:
      case hsql::kOpNotLike:
      case hsql::kOpILike:
      case hsql::kOpAnd:
      case hsql::kOpOr:
      case hsql::kOpIn:
      case hsql::kOpConcat:

      // Unary operators.
      case hsql::kOpNot:
      case hsql::kOpUnaryMinus:
      case hsql::kOpIsNull:
      case hsql::kOpExists:
      default:
        break;
    }
  }
}

// TODO: handle projections
void select(const hsql::SelectStatement* stmt, const vector<Row>& input,
            const map<string, uint32_t>& input_schema, vector<Row>& output,
            map<string, uint32_t>& output_schema) {
  const hsql::TableRef* fromTable = stmt->fromTable;

  if (fromTable->type == hsql::kTableDiff) {
    if (stmt->whereClause != nullptr) {
      vector<Row> diff_output;
      diff(stmt->fromTable->diff, input, input_schema, diff_output, output_schema);
      apply_where_predicates(stmt->whereClause, diff_output, output_schema,
                             output);
    } else {
      diff(stmt->fromTable->diff, input, input_schema, output, output_schema);
    }
    return;
  }

  if (stmt->whereClause != nullptr) {
    apply_where_predicates(stmt->whereClause, input, input_schema, output);
  }
}

/**
 * Input:
 * |usage|latency|location|version
 * ratio: pmi_ratio
 * metric col: usage
 * attribute cols: location, version
 * Return:
 * |location|version|avg_usage|pmi_ratio|support_ratio
 *
 * @param input: the input table
 * @param counts: the stats we're returning
 * @param attr_indices: vector that contains indices for the attribute cols
 * in the _input table. With these inputs, it would be `attr_indices = {2, 3}`.
 * @param max_combo: The maximum number of combinations for a given explanation
 * (i.e., in the output, there can only be a max_combo values in a single row
 * that are non-null).
 **/
void count_diff_stats(const vector<Row>& input, map<Row, uint32_t>& counts,
                      const vector<uint32_t>& attr_indices, const uint32_t max_combo, 
                      map<int, set<string> >& prunedValues, set<Row>& candidates) {
  const uint32_t num_rows = input.size();
  const uint32_t num_compare_attrs = attr_indices.size();

  for (auto i = 0u; i < num_rows; ++i) {
    const Row input_row = input[i];

    for (auto j = 0u; j < num_compare_attrs; ++j) {
      const uint32_t first_attr_index = attr_indices[j];
      const string first_attr = input_row[first_attr_index];
      set<string>& first_vals = prunedValues[j];
      if (first_vals.find(first_attr) != first_vals.end())
        continue;
      Row order_one_attr_key(num_compare_attrs, "null");
      order_one_attr_key[j] = first_attr;

      if (max_combo == 1) {
        counts[order_one_attr_key] += 1;
      } else if (candidates.find(order_one_attr_key) != candidates.end()) {
        // TODO: code-gen? Right now, we only support max_combo = {1-3}
        // 2-order combinations
        for (auto k = j + 1; k < num_compare_attrs; ++k) {
          const uint32_t second_attr_index = attr_indices[k];
          const string second_attr = input_row[second_attr_index];
          set<string>& second_vals = prunedValues[k];
          if (second_vals.find(second_attr) != second_vals.end())
            continue;
          Row order_two_attr_key(order_one_attr_key);
          order_two_attr_key[k] = second_attr;

          order_one_attr_key[j] = "null";
          order_one_attr_key[k] = second_attr;
          if (candidates.find(order_one_attr_key) == candidates.end())
            continue;
          
          if (max_combo == 2) {
            counts[order_two_attr_key] += 1;
          } else if (candidates.find(order_two_attr_key) != candidates.end()) {
            // 3-order combinations
            for (auto l = k + 1; l < num_compare_attrs; ++l) {
              const uint32_t third_attr_index = attr_indices[l];
              const string third_attr = input_row[third_attr_index];
              set<string>& third_vals = prunedValues[l];
              if (third_vals.find(third_attr) != third_vals.end())
                continue;
              Row order_three_attr_key(order_two_attr_key);
              order_three_attr_key[l] = third_attr;

              order_one_attr_key[k] = "null";
              order_one_attr_key[l] = third_attr;
              if (candidates.find(order_one_attr_key) == candidates.end())
                continue;
              order_two_attr_key[j] = "null";
              order_two_attr_key[l] = third_attr;
              if (candidates.find(order_two_attr_key) == candidates.end())
                continue;
              order_two_attr_key[k] = "null";
              order_two_attr_key[j] = first_attr;
              if (candidates.find(order_two_attr_key) == candidates.end())
                continue;

              counts[order_three_attr_key] += 1;
            }
          }
        }
      }
    }
  }
}

void generate_bitmaps(const vector<Row>& input, map<Row, uint32_t>& counts,
                      const vector<uint32_t>& attr_indices, map<int, set<string> >& prunedValues, vector<map<string, Roaring> >& bitmaps) {
  const uint32_t num_rows = input.size();
  const uint32_t num_compare_attrs = attr_indices.size();
  bitmaps.resize(num_compare_attrs);

  for (auto i = 0u; i < num_rows; ++i) {
    const Row input_row = input[i];

    for (auto j = 0u; j < num_compare_attrs; ++j) {
      const uint32_t attr_index = attr_indices[j];
      const string attr = input_row[attr_index];
      set<string>& first_vals = prunedValues[j];
      if (first_vals.find(attr) != first_vals.end())
        continue;
      Row order_one_attr_key(num_compare_attrs, "null");
      order_one_attr_key[j] = attr;
      counts[order_one_attr_key] += 1;
      bitmaps[j][attr].add(i);
    }
  }
}

const double minOutlierSupport = 0.0;
const double minRatioMetric = 0.0;
const double INFTY = 1E30;

double computeRatio(int matchedOutliers, int unMatchedOutliers, int matchedTotal, int unMatchedTotal) {
    if (unMatchedTotal == 0)
        return 0;
    if (unMatchedOutliers == 0)
        return INFTY;
    return 1.0*matchedOutliers*unMatchedTotal/matchedTotal/unMatchedOutliers;
}

void prune_outliers(map<Row, uint32_t>& cur_outlier_counts, map<int, set<string> >& prunedValues, const uint32_t suppCount) {
    vector<Row> prunedRows;
    for (auto& entry : cur_outlier_counts) {
        if (entry.second < suppCount) {
            for (int i = 0; i < entry.first.size(); i++) {
                if (entry.first[i] != "null") {
                    prunedValues[i].insert(entry.first[i]);
                }
            }
            prunedRows.push_back(entry.first);
        }
    }
    for (Row& r : prunedRows) {
        cur_outlier_counts.erase(r);
    }
}

void prune_outliers(map<Row, uint32_t>& cur_outlier_counts, map<int, set<string> >& prunedValues, const uint32_t suppCount, vector<map<string, Roaring> >& bitmaps) {
    vector<Row> prunedRows;
    for (auto& entry : cur_outlier_counts) {
        if (entry.second < suppCount) {
            for (int i = 0; i < entry.first.size(); i++) {
                if (entry.first[i] != "null") {
                    prunedValues[i].insert(entry.first[i]);
                    bitmaps[i].erase(entry.first[i]);
                }
            }
            prunedRows.push_back(entry.first);
        }
    }
    for (Row& r : prunedRows) {
        cur_outlier_counts.erase(r);
    }
}

void prune_inliers(map<Row, uint32_t>& cur_outlier_counts, map<Row, uint32_t>& cur_inlier_counts, 
                   map<Row, uint32_t>& outlier_counts, map<Row, uint32_t>& inlier_counts,
                   set<Row>& candidates, int outlierSize, int inlierSize) {
    for (auto& entry : cur_outlier_counts) {
        int matchedOutliers = entry.second;
        int matchedInliers = cur_inlier_counts.count(entry.first) ? cur_inlier_counts[entry.first] : 0;
        int unMatchedOutliers = outlierSize - matchedOutliers;
        int matchedTotal = matchedOutliers + matchedInliers;
        int unMatchedTotal = outlierSize + inlierSize - matchedTotal; 
        
        double risk_ratio = computeRatio(matchedOutliers, unMatchedOutliers, matchedTotal, unMatchedTotal);
        if (risk_ratio > minRatioMetric) {
            outlier_counts[entry.first] = matchedOutliers;
            inlier_counts[entry.first] = matchedInliers;
        } else {
            candidates.insert(entry.first);
        }
    }
}

void count_diff_stats(const vector<Row>& outliers, const vector<Row>& inliers, 
                      const vector<uint32_t>& attr_indices, const uint32_t max_combo, 
                      map<Row, uint32_t>& outlier_counts, map<Row, uint32_t>& inlier_counts) {
    bench_timer_t start = time_start();
    map<int, set<string> > prunedValues;
    uint32_t suppCount = minOutlierSupport*outliers.size();
    set<Row> candidates;

    for (int combo = 1; combo <= max_combo; combo++) {
        map<Row, uint32_t> cur_outlier_counts, cur_inlier_counts;
        count_diff_stats(outliers, cur_outlier_counts, attr_indices, combo, prunedValues, candidates);
        prune_outliers(cur_outlier_counts, prunedValues, suppCount);
        count_diff_stats(inliers, cur_inlier_counts, attr_indices, combo, prunedValues, candidates);
        prune_inliers(cur_outlier_counts, cur_inlier_counts, outlier_counts, inlier_counts, candidates, outliers.size(), inliers.size());
    }
    cout << time_stop(start) << endl;
}

void merge_bitmaps(map<Row, uint32_t>& counts, const vector<uint32_t>& attr_indices, const uint32_t max_combo, 
                   map<int, set<string> >& prunedValues, set<Row>& candidates, vector<map<string, Roaring> >& bitmaps) {
    const uint32_t num_compare_attrs = attr_indices.size();
    
    for (auto i = 0u; i < num_compare_attrs; ++i) {
        for (auto& entry1 : bitmaps[i]) {
            set<string>& first_vals = prunedValues[i];
            if (first_vals.find(entry1.first) != first_vals.end())
                continue;
            Row r1(num_compare_attrs, "null");
            r1[i] = entry1.first;
            if (candidates.find(r1) == candidates.end())
                continue;

            for (auto j = i + 1; j < num_compare_attrs; ++j) {
                for (auto& entry2 : bitmaps[j]) {
                    set<string>& second_vals = prunedValues[j];
                    if (second_vals.find(entry2.first) != second_vals.end())
                        continue;
                    Row r2(num_compare_attrs, "null");
                    r2[j] = entry2.first;
                    if (candidates.find(r2) == candidates.end())
                        continue;
                    
                    Row order_two_attr_key(num_compare_attrs, "null");
                    order_two_attr_key[i] = entry1.first;
                    order_two_attr_key[j] = entry2.first;
                    Roaring intersection = entry1.second & entry2.second;

                    if (max_combo == 2) {
                        counts[order_two_attr_key] = intersection.cardinality();
                    } else if (candidates.find(order_two_attr_key) != candidates.end()) {
                        for (auto k = j + 1; k < num_compare_attrs; ++k) {
                            for (auto& entry3 : bitmaps[k]) {
                                set<string>& third_vals = prunedValues[k];
                                if (third_vals.find(entry3.first) != third_vals.end())
                                    continue;
                                Row r3(num_compare_attrs, "null");
                                r3[k] = entry2.first;
                                if (candidates.find(r3) == candidates.end())
                                    continue;

                                Row order_three_attr_key(order_two_attr_key);
                                order_three_attr_key[k] = entry3.first;
                                intersection &= entry3.second;
                                
                                order_two_attr_key[i] = "null";
                                order_two_attr_key[k] = entry3.first;
                                if (candidates.find(order_two_attr_key) == candidates.end())
                                    continue;
                                order_two_attr_key[j] = "null";
                                order_two_attr_key[i] = entry1.first;
                                if (candidates.find(order_two_attr_key) == candidates.end())
                                    continue;

                                counts[order_three_attr_key] = intersection.cardinality();
                            }
                        }
                    }
                }
            }
        }
    }
}

void count_diff_stats_by_col(const vector<Row>& outliers, const vector<Row>& inliers, 
                      const vector<uint32_t>& attr_indices, const uint32_t max_combo, 
                      map<Row, uint32_t>& outlier_counts, map<Row, uint32_t>& inlier_counts) {
    bench_timer_t start = time_start();
    map<int, set<string> > prunedValues;
    uint32_t suppCount = minOutlierSupport*outliers.size();
    set<Row> candidates;
    vector<map<string, Roaring> > outlier_bitmaps, inlier_bitmaps;

    for (int combo = 1; combo <= max_combo; combo++) {
        map<Row, uint32_t> cur_outlier_counts, cur_inlier_counts;
        if (combo == 1) {
            generate_bitmaps(outliers, cur_outlier_counts, attr_indices, prunedValues, outlier_bitmaps);
            prune_outliers(cur_outlier_counts, prunedValues, suppCount, outlier_bitmaps);
            generate_bitmaps(inliers, cur_inlier_counts, attr_indices, prunedValues, inlier_bitmaps);
        }
        else {
            merge_bitmaps(cur_outlier_counts, attr_indices, combo, prunedValues, candidates, outlier_bitmaps);
            prune_outliers(cur_outlier_counts, prunedValues, suppCount, outlier_bitmaps);
            merge_bitmaps(cur_inlier_counts, attr_indices, combo, prunedValues, candidates, inlier_bitmaps);
        }
        prune_inliers(cur_outlier_counts, cur_inlier_counts, outlier_counts, inlier_counts, candidates, outliers.size(), inliers.size()); 
    }
    
    cout << time_stop(start) << endl;
}


vector<uint32_t> get_attribute_indices(
    const std::vector<string>& attribute_cols,
    const map<string, uint32_t>& schema) {
  vector<uint32_t> indices;
  for (string col : attribute_cols) {
    indices.push_back(schema.at(col));
  }
  return indices;
}

void diff(hsql::DiffDefinition* diff, const vector<Row>& input,
          const map<string, uint32_t>& input_schema, vector<Row>& output,
          map<string, uint32_t>& output_schema) {
  vector<Row> outliers;
  map<string, uint32_t> dummy_schema;  // TODO: this is janky; get rid of this
  select(diff->first->select, input, input_schema, outliers, dummy_schema);
  vector<Row> inliers;
  // TODO: handle case where->diff->second == null
  select(diff->second->select, input, input_schema, inliers, dummy_schema);

  const string compare_by_fn_name = diff->compare_by->getName();
  const string metric_col = diff->compare_by->exprList->at(0)->getName();

  const std::vector<string> attribute_cols =
      get_attribute_cols(diff->attribute_cols);
  vector<uint32_t> attr_indices =
      get_attribute_indices(attribute_cols, input_schema);
  const uint32_t max_combo =
      min((uint32_t)attribute_cols.size(), (uint32_t)diff->max_combo->ival);
#ifdef DEBUG
  cout << "Attribute cols: ";
  for (auto col : attribute_cols) {
    cout << col << ", ";
  }
  cout << endl;
  cout << "Compare By function: " << compare_by_fn_name << endl;
  cout << "Max Combo: " << max_combo << endl;
#endif

  macrodiff_compare_by_func compare_by_fn =
      get_compare_by_func(compare_by_fn_name);

#ifdef DEBUG
  bench_timer_t start = time_start();
  cout << "Beginning APriori" << endl;
#endif

  const uint32_t outlier_count = outliers.size();
  const uint32_t inlier_count = inliers.size();
  const uint32_t total_count = inlier_count + outlier_count;
  
  map<Row, uint32_t> outlier_counts;
  map<Row, uint32_t> inlier_counts;
#ifdef OUTLIER_BY_ROW
  cout << "processing by row\n";
  count_diff_stats(outliers, inliers, attr_indices, max_combo, outlier_counts, inlier_counts);
#endif

#ifdef OUTLIER_BY_COL
  cout << "processing by column\n";
  count_diff_stats(outliers, inliers, attr_indices, max_combo, outlier_counts, inlier_counts);
#endif

#ifdef DEBUG
  cout << "Total outlier count: " << outlier_count << endl;
  cout << "Total count: " << total_count << endl;
#endif

  // Create new output_schema
  uint32_t ind = 0;
  for (auto col : attribute_cols) {
    // copy the attributes to the output schema
    output_schema[col] = ind++;
  }
  // add the metric column, ratio column, and support column
  output_schema[metric_col] = ind++;
  output_schema[compare_by_fn_name] = ind++;
  output_schema["support"] = ind++;

  for (auto it = outlier_counts.begin(); it != outlier_counts.end(); ++it) {
    Row attrs_and_vals;
    Row attrs = it->first;
    attrs_and_vals.insert(std::begin(attrs_and_vals), std::begin(attrs),
                          std::end(attrs));

    const uint32_t matched_outlier_count = it->second;
    const uint32_t matched_inlier_count = inlier_counts[attrs];
    const uint32_t matched_total_count =
        matched_outlier_count + matched_inlier_count;

    const double ratio_for_attrs = compare_by_fn(
        matched_outlier_count, matched_total_count, outlier_count, total_count);
    const double support_ratio = matched_outlier_count / (double)outlier_count;

    attrs_and_vals.push_back(std::to_string(matched_outlier_count));
    attrs_and_vals.push_back(std::to_string(ratio_for_attrs));
    attrs_and_vals.push_back(std::to_string(support_ratio));

    output.push_back(attrs_and_vals);
  }

#ifdef DEBUG
  const double time = time_stop(start);
  cout << "APriori Time: " << time << endl;
#endif
}

void repl() {
  rl_bind_key('\t', rl_complete);
  map<string, uint32_t> input_schema;
  vector<Row> INPUT;
  map<string, uint32_t> output_schema;
  vector<Row> OUTPUT;

  while (true) {
    const string query_str = read_repl_input();
    if (query_str == "") {
      break;
    }

    // Add query to history.
    add_history(query_str.c_str());

    // parse a given query_str
    hsql::SQLParserResult query;
    hsql::SQLParser::parseSQLString(query_str, &query);

    // check whether the parsing was successful
    if (query.isValid()) {
      assert(query.size() == 1);
      const hsql::SQLStatement* query_statement = query.getStatement(0);
#ifdef DEBUG
      cout << "Parsed successfully!" << endl;
      cout << "Number of statements: " << query.size() << endl;
      // Print a statement summary.
      hsql::printStatementInfo(query_statement);
#endif

      switch (query_statement->type()) {
        case hsql::kStmtImport:
          INPUT.clear();
          import_table(
              static_cast<const hsql::ImportStatement*>(query_statement), INPUT,
              input_schema);
          cout << "Num rows: " << INPUT.size() << endl;
          print_table(INPUT, input_schema);
          break;
        case hsql::kStmtSelect:
          OUTPUT.clear();
          output_schema.clear();
          select(static_cast<const hsql::SelectStatement*>(query_statement),
                 INPUT, input_schema, OUTPUT, output_schema);
          print_table(OUTPUT, output_schema);
          break;
        case hsql::kStmtError:  // unused
        case hsql::kStmtInsert:
        case hsql::kStmtUpdate:
        case hsql::kStmtDelete:
        case hsql::kStmtCreate:
        case hsql::kStmtDrop:
        case hsql::kStmtPrepare:
        case hsql::kStmtExecute:
        case hsql::kStmtExport:
        case hsql::kStmtRename:
        case hsql::kStmtAlter:
        case hsql::kStmtShow:
        default:
          break;
      }

    } else {
      fprintf(stderr, "Given string is not a valid SQL query.\n");
      fprintf(stderr, "%s (L%d:%d)\n", query.errorMsg(), query.errorLine(),
              query.errorColumn());
    }
  }
}

void print_welcome() {
  const string ascii_art =
      R"!(
Welcome to
    __  ___                          ___ ________
   /  |/  /___ _______________  ____/ (_) __/ __/
  / /|_/ / __ `/ ___/ ___/ __ \/ __  / / /_/ /_  
 / /  / / /_/ / /__/ /  / /_/ / /_/ / / __/ __/  
/_/  /_/\__,_/\___/_/   \____/\__,_/_/_/ /_/     

)!";
  cout << ascii_art << endl;
}

int main(/*int argc, const char* argv[]*/) {
  print_welcome();
  repl();
  return 0;
}
