#ifndef _UTIL_H_
#define _UTIL_H_

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "sql-parser/src/SQLParser.h"
#include "types.h"

using std::string;
using std::vector;
using std::map;
using std::cout;
using std::endl;

bool ignore_case_pred(unsigned char a, unsigned char b) {
  return std::tolower(a) == std::tolower(b);
}

bool equals_ignorecase(std::string const& a, std::string const& b) {
  if (a.length() == b.length()) {
    return std::equal(b.begin(), b.end(), a.begin(), ignore_case_pred);
  } else {
    return false;
  }
}

vector<string> get_attribute_cols(vector<hsql::Expr*>* attribute_cols) {
  vector<string> to_return;
  for (auto it = attribute_cols->begin(); it != attribute_cols->end(); ++it) {
    to_return.push_back((*it)->getName());
  }
  return to_return;
}

void print_table(const vector<Row>& data, const map<string, uint32_t>& schema,
                 const uint32_t num_lines = 10) {
  const uint32_t num_attrs = schema.size();
  vector<string> schemaAsVec(num_attrs);
  for (auto it = schema.begin(); it != schema.end(); ++it) {
    schemaAsVec[it->second] = it->first;
  }

  cout << "------------------------------------------" << endl;
  cout << "|";
  for (auto it = schemaAsVec.begin(); it != schemaAsVec.end(); ++it) {
    cout << *it << "|";
  }
  cout << endl;
  cout << "------------------------------------------" << endl;

  for (auto i = 0u; i < std::min(num_lines, (uint32_t)data.size()); ++i) {
    const Row row = data[i];
    cout << "|";
    for (auto j = 0u; j < num_attrs; ++j) {
      cout << row[j] << "|";
    }
    cout << endl;
  }
}

#endif
