#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <vector>
#include "time_util.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::cerr;
using std::vector;
using std::array;
using std::string;
using std::map;
using std::pair;
using std::tuple;

// CSV should have following schema
// |outlier|app_version|device|country|
using APP_VERSION = string;
using DEVICE = string;
using COUNTRY = string;
using Row = vector<string>;

inline double prevalence_ratio(const unsigned int matched_outlier,
                               const unsigned int matched_total,
                               const unsigned int outlier_count,
                               const unsigned int total_count) {
  if (outlier_count == 0 || matched_outlier == 0) {
    return 0;
  }

  const unsigned int inlier_count = total_count - outlier_count;
  unsigned int matched_inlier = matched_total - matched_outlier;

  if (matched_inlier == 0) {
    matched_inlier += 1;  // increment by 1 to avoid divide-by-zero error
  }

  return ((double)matched_outlier / outlier_count) /
         ((double)matched_inlier / inlier_count);
}

void apriori(const vector<Row>& data) {
  const double min_ratio = 3.0;
  bench_timer_t start = time_start();
  cout << "Beginning APriori" << endl;

  unsigned int outlier_count = 0;
  const unsigned int total_count = data.size();
  map<string, unsigned int> single_counts;
  map<string, unsigned int> single_outlier_counts;

  map<pair<string, string>, unsigned int> order_two_counts;
  map<pair<string, string>, unsigned int> order_two_outlier_counts;

  map<tuple<string, string, string>, unsigned int> order_three_counts;
  map<tuple<string, string, string>, unsigned int> order_three_outlier_counts;

  const unsigned int num_cols = data[0].size();
  for (unsigned int i = 0; i < total_count; ++i) {
    Row row = data[i];
    string outlier_val = row[0];
    const bool outlier = outlier_val == "0";
    if (outlier) ++outlier_count;

    // first column is outlier column, so we start at 1
    for (unsigned int j = 1; j < num_cols; ++j) {
      if (outlier) single_outlier_counts[row[j]] += 1;
      const string first_attr = row[j];
      single_counts[first_attr] += 1;

      // 2-order combinations
      for (unsigned int k = j + 1; k < num_cols; ++k) {
        const string second_attr = row[k];
        pair<string, string> both_attrs =
            std::make_pair(first_attr, second_attr);
        if (outlier) order_two_outlier_counts[both_attrs] += 1;
        order_two_counts[both_attrs] += 1;

        // 3-order combinations
        for (unsigned int l = k + 1; l < num_cols; ++l) {
          const string third_attr = row[l];
          tuple<string, string, string> three_attrs =
              std::make_tuple(first_attr, second_attr, third_attr);
          if (outlier) order_three_outlier_counts[three_attrs] += 1;
          order_three_counts[three_attrs] += 1;
        }
      }
    }
  }

  cout << "Total outlier count: " << outlier_count << endl;
  cout << "Total count: " << total_count << endl;

  // Print first order ratios
  for (auto it = single_outlier_counts.begin();
       it != single_outlier_counts.end(); ++it) {
    const string attr = it->first;
    const unsigned int matched_outlier_count = it->second;
    const unsigned int matched_total_count = single_counts[attr];
    const double prev_ratio_for_attr = prevalence_ratio(
        matched_outlier_count, matched_total_count, outlier_count, total_count);
    if (prev_ratio_for_attr > min_ratio) {
      cout << attr << ": " << prev_ratio_for_attr << endl;
    }
  }

  // Print second order ratios
  for (auto it = order_two_outlier_counts.begin();
       it != order_two_outlier_counts.end(); ++it) {
    const pair<string, string> both_attrs = it->first;
    const unsigned int matched_outlier_count = it->second;
    const unsigned int matched_total_count = order_two_counts[both_attrs];
    const double prev_ratio_for_attr = prevalence_ratio(
        matched_outlier_count, matched_total_count, outlier_count, total_count);
    if (prev_ratio_for_attr > min_ratio) {
      cout << "(" << both_attrs.first << ", " << both_attrs.second
           << "): " << prev_ratio_for_attr << endl;
    }
  }

  // Print third order ratios
  for (auto it = order_three_outlier_counts.begin();
       it != order_three_outlier_counts.end(); ++it) {
    const tuple<string, string, string> three_attrs = it->first;
    const unsigned int matched_outlier_count = it->second;
    const unsigned int matched_total_count = order_three_counts[three_attrs];
    const double prev_ratio_for_attr = prevalence_ratio(
        matched_outlier_count, matched_total_count, outlier_count, total_count);
    if (prev_ratio_for_attr > min_ratio) {
      cout << "(" << std::get<0>(three_attrs) << ", "
           << std::get<1>(three_attrs) << ", " << std::get<2>(three_attrs)
           << "): " << prev_ratio_for_attr << endl;
    }
  }

  const double time = time_stop(start);
  cout << "APriori Time: " << time << endl;
}

void read_csv(const char* filename, vector<Row>& data) {
  ifstream input(filename);
  for (string line; getline(input, line);) {
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

int main(int argc, const char* argv[]) {
  if (argc < 2) {
    cerr << "Usage: ./macrodiff [csv file]" << endl;
    exit(1);
  }
  vector<Row> DATA;
  read_csv(argv[1], DATA);
  apriori(DATA);
  return 0;
}
