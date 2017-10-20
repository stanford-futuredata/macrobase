#ifndef _IO_UTIL_H_
#define _IO_UTIL_H_

#include <readline/readline.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "sql-parser/src/SQLParser.h"
#include "types.h"

using std::string;
using std::vector;
using std::map;
using std::setw;
using std::setfill;
using std::cout;
using std::cin;
using std::endl;
using std::cerr;
using std::ifstream;

string read_repl_input() {
  string query_str = "";
  char* temp = readline("macrodiff> ");
  if (temp == NULL) {
    return "";
  }

  query_str += temp;
  free(temp);

  while (*(query_str.end() - 1) != ';') {
    temp = readline("... ");
    if (temp == NULL) {
      return "";
    }
    query_str += "\n";
    query_str += temp;
    free(temp);
  }
  return query_str;
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

void print_table(const vector<Row>& data, const map<string, uint32_t>& schema,
                 const uint32_t num_lines = 10, const uint32_t col_width = 10) {
  const uint32_t num_attrs = schema.size();
  vector<string> schemaAsVec(num_attrs);
  for (auto it = schema.begin(); it != schema.end(); ++it) {
    schemaAsVec[it->second] = it->first;
  }

  cout << setfill('-') << setw((col_width + 1) * schemaAsVec.size() + 2)
       << '\n';
  cout << setfill(' ') << "|";
  for (auto it = schemaAsVec.begin(); it != schemaAsVec.end(); ++it) {
    cout << setw(col_width) << *it << "|";
  }
  cout << endl;
  cout << setfill('-') << setw((col_width + 1) * schemaAsVec.size() + 2)
       << '\n';
  cout << setfill(' ');

  if (data.size() < num_lines) {
    for (auto i = 0u; i < data.size(); ++i) {
      const Row row = data[i];
      cout << "|";
      for (auto j = 0u; j < num_attrs; ++j) {
        cout << setw(col_width) << row[j] << "|";
      }
      cout << endl;
    }
  } else {
    // print first 5 lines and last 5 lines
    for (auto i = 0u; i < 5; ++i) {
      const Row row = data[i];
      cout << "|";
      for (auto j = 0u; j < num_attrs; ++j) {
        cout << setw(col_width) << row[j] << "|";
      }
      cout << endl;
    }
    cout << "..." << endl;
    for (auto i = data.size() - 5; i < data.size(); ++i) {
      const Row row = data[i];
      cout << "|";
      for (auto j = 0u; j < num_attrs; ++j) {
        cout << setw(col_width) << row[j] << "|";
      }
      cout << endl;
    }
  }
}

#endif
