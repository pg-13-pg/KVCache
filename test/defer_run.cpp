//
// Created by henry on 24-1-23.
//
#include <iostream>
#include <string>
#include "include/defer.h"

using namespace std;

int main() {
  string result;
  {
    string str1 = "Hello";
    string str2 = " world";
    DEFER {
      result += str1;
      result += str2;
    };
    if (!result.empty()) {
      cerr << "deferred callback ran before scope exit" << endl;
      return 1;
    }
  }
  if (result != "Hello world") {
    cerr << "deferred callback did not run at scope exit" << endl;
    return 1;
  }
  return 0;
}
