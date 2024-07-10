#pragma once

#include <iostream>

#include "arrow/status.h"

#define LOG_NOT_OK(status)                        \
  do {                                            \
    if (!status.ok()) {                           \
      std::cerr << status.message() << std::endl; \
    }                                             \
  } while (false)
