#ifndef _UTIL_H_
#define _UTIL_H_

#include <string>
#include <vector>

#include "sql-parser/src/SQLParser.h"

using std::equal;
using std::string;
using std::tolower;
using std::vector;

bool ignore_case_pred(unsigned char a, unsigned char b) {
  return tolower(a) == tolower(b);
}

bool equals_ignorecase(string const& a, string const& b) {
  if (a.length() == b.length()) {
    return equal(b.begin(), b.end(), a.begin(), ignore_case_pred);
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

#endif
