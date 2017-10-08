#include <assert.h>
#include <readline/history.h>
#include <stdio.h>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "compare_by.h"
#include "time_util.h"
#include "types.h"
#include "util.h"

// include the sql parser
#include "sql-parser/src/SQLParser.h"
// contains printing utilities
#include "sql-parser/src/util/sqlhelper.h"

using std::cout;
using std::cin;
using std::endl;
using std::ifstream;
using std::cerr;
using std::vector;
using std::array;
using std::string;
using std::map;
using std::stringstream;
using std::pair;
using std::tuple;

// TODO: handle projections
void select(hsql::TableRef* tbl, const vector<Row>& input, vector<Row>& output,
            const map<string, uint32_t>& schema) {
  const hsql::Expr* whereClause = tbl->select->whereClause;
  const hsql::OperatorType op = whereClause->opType;
  const string column_name = whereClause->expr->name;
  const uint32_t column_ind = schema.at(column_name);

  // TODO: handle multiple types
  const float val = whereClause->expr2->fval;
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
                      const vector<uint32_t>& attr_indices,
                      const uint32_t max_combo) {
  const uint32_t num_rows = input.size();
  const uint32_t num_compare_attrs = attr_indices.size();

  for (auto i = 0u; i < num_rows; ++i) {
    const Row input_row = input[i];

    for (auto j = 0u; j < num_compare_attrs; ++j) {
      const uint32_t first_attr_index = attr_indices[j];
      const string first_attr = input_row[first_attr_index];
      Row order_one_attr_key(num_compare_attrs, "null");
      order_one_attr_key[j] = first_attr;
      counts[order_one_attr_key] += 1;

      // TODO: code-gen? Right now, we only support max_combo = {1-3}
      if (max_combo > 1) {
        // 2-order combinations
        for (auto k = j + 1; k < num_compare_attrs; ++k) {
          const uint32_t second_attr_index = attr_indices[k];
          const string second_attr = input_row[second_attr_index];
          Row order_two_attr_key(order_one_attr_key);
          order_two_attr_key[k] = second_attr;
          counts[order_two_attr_key] += 1;

          if (max_combo > 2) {
            // 3-order combinations
            for (auto l = k + 1; l < num_compare_attrs; ++l) {
              const uint32_t third_attr_index = attr_indices[l];
              const string third_attr = input_row[third_attr_index];
              Row order_three_attr_key(order_two_attr_key);
              order_three_attr_key[l] = third_attr;
              counts[order_three_attr_key] += 1;
            }
          }
        }
      }
    }
  }
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

void diff(const hsql::SelectStatement* stmt, const vector<Row>& data,
          const map<string, uint32_t>& schema, vector<Row>& output,
          map<string, uint32_t>& output_schema) {
  // assert(stmt->fromTable->type == hsql::kTableDiff);
  vector<Row> outliers;
  select(stmt->fromTable->diff->first, data, outliers, schema);
  vector<Row> inliers;
  // TODO: handle case where->diff->second == null
  select(stmt->fromTable->diff->second, data, inliers, schema);

  const string compare_by_fn_name =
      stmt->fromTable->diff->compare_by->getName();
  const string metric_col =
      stmt->fromTable->diff->compare_by->exprList->at(0)->getName();

  const uint32_t max_combo = stmt->fromTable->diff->max_combo->ival;
  const std::vector<string> attribute_cols =
      get_attribute_cols(stmt->fromTable->diff->attribute_cols);
  vector<uint32_t> attr_indices = get_attribute_indices(attribute_cols, schema);
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
  count_diff_stats(outliers, outlier_counts, attr_indices, max_combo);

  map<Row, uint32_t> inlier_counts;
  count_diff_stats(inliers, inlier_counts, attr_indices, max_combo);

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

void read_csv(const char* filename, vector<Row>& data,
              map<string, uint32_t>& schema) {
  ifstream input(filename);
  // Parse header first to get schema (TODO: handle schemaless CSVs)
  string line;
  getline(input, line);
  size_t last = 0;
  size_t next = 0;
  auto ind = 0u;
  while ((next = line.find(',', last)) != string::npos) {
    schema[line.substr(last, next - last)] = ind++;
    last = next + 1;
  }
  schema[line.substr(last)] = ind;

  // Parse remaining rows
  for (; getline(input, line);) {
    if (line == "") {
      continue;
    }
    Row row;
    size_t last = 0;
    size_t next = 0;
    while ((next = line.find(',', last)) != string::npos) {
      row.push_back(line.substr(last, next - last));
      last = next + 1;
    }
    row.push_back(line.substr(last));
    data.push_back(row);
  }
}

void import_table(const hsql::ImportStatement* query, vector<Row>& data,
                  map<string, uint32_t>& schema) {
  read_csv(query->filePath, data, schema);
}

void repl() {
  rl_bind_key('\t', rl_complete);
  map<string, uint32_t> schema;
  vector<Row> DATA;
  map<string, uint32_t> output_schema;
  vector<Row> output;

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
          DATA.clear();
          import_table(
              static_cast<const hsql::ImportStatement*>(query_statement), DATA,
              schema);
          cout << "Num rows: " << DATA.size() << endl;
          print_table(DATA, schema);
          break;
        case hsql::kStmtSelect:
          output.clear();
          output_schema.clear();
          diff(static_cast<const hsql::SelectStatement*>(query_statement), DATA,
               schema, output, output_schema);
          print_table(output, output_schema);
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
