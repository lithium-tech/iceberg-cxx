#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "gen/src/batch.h"
#include "gen/src/generators.h"

namespace gen {

class Assignment {
 public:
  Assignment(std::string_view result_name, std::shared_ptr<Generator> generator)
      : result_name_(std::string(result_name)), generator_(generator) {}

  arrow::Status Apply(BatchPtr& batch) {
    ARROW_ASSIGN_OR_RAISE(auto new_data, generator_->Generate(batch));
    ARROW_ASSIGN_OR_RAISE(batch, batch->AddColumn(result_name_, new_data));
    return arrow::Status::OK();
  }

 private:
  std::string result_name_;
  std::shared_ptr<Generator> generator_;
};

struct Projection {
 public:
  Projection(const std::vector<std::string>& columns) : columns_(columns.begin(), columns.end()) {}

  Projection(const std::vector<std::string_view>& columns) : columns_(columns.begin(), columns.end()) {}

  arrow::Status Apply(BatchPtr& batch) {
    ARROW_ASSIGN_OR_RAISE(batch, batch->GetProjection(columns_));
    return arrow::Status::OK();
  }

 private:
  std::vector<std::string> columns_;
};

struct ProgramStep {
  std::vector<Assignment> new_assignments;
  std::vector<Projection> projections;

  arrow::Status Apply(BatchPtr& batch) {
    for (auto& assignment : new_assignments) {
      ARROW_RETURN_NOT_OK(assignment.Apply(batch));
    }

    for (auto& projection : projections) {
      ARROW_RETURN_NOT_OK(projection.Apply(batch));
    }

    return arrow::Status::OK();
  }
};

struct Program {
  std::vector<ProgramStep> steps;

  void AddAssign(const Assignment& assign) {
    steps.emplace_back(ProgramStep{.new_assignments = {assign}, .projections = {}});
  }

  void AddProjection(const Projection& projection) {
    steps.emplace_back(ProgramStep{.new_assignments = {}, .projections = {projection}});
  }

  arrow::Result<BatchPtr> Generate(int32_t num_rows) {
    auto batch = std::make_shared<Batch>(num_rows);

    for (auto& step : steps) {
      ARROW_RETURN_NOT_OK(step.Apply(batch));
    }

    return batch;
  }
};

}  // namespace gen
