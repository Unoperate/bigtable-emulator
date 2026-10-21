#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "absl/strings/str_format.h"
#include "table.h"
#include "test_util.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

TEST(InMemoryConditionalMutations, TestTrueMutations) {
  auto const* const table_name = "projects/test/instances/test/tables/test";
  auto const* const column_family_name = "test_column_family";
  auto const* const row_key = "0";
  auto const* const column_qualifier = "column_1";
  auto timestamp_micros = 1000;
  auto const* const true_mutation_value = "set by a true mutation";
  auto const* const false_mutation_value = "set by a false mutation";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  ::google::bigtable::v2::Mutation true_mutation;
  auto* set_cell_mutation = true_mutation.mutable_set_cell();
  set_cell_mutation->set_family_name(column_family_name);
  set_cell_mutation->set_column_qualifier(column_qualifier);
  set_cell_mutation->set_timestamp_micros(timestamp_micros);
  set_cell_mutation->set_value(true_mutation_value);

  std::vector<google::bigtable::v2::Mutation> true_mutations = {true_mutation};

  ::google::bigtable::v2::Mutation false_mutation;
  set_cell_mutation = false_mutation.mutable_set_cell();
  set_cell_mutation->set_family_name(column_family_name);
  set_cell_mutation->set_column_qualifier(column_qualifier);
  set_cell_mutation->set_timestamp_micros(timestamp_micros);
  set_cell_mutation->set_value(false_mutation_value);

  std::vector<google::bigtable::v2::Mutation> false_mutations = {
      false_mutation};

  std::vector<SetCellParams> v = {
      {column_family_name, "column_2", 1000, "some_value"}};
  ASSERT_STATUS_OK(SetCells(table, table_name, row_key, v));
  ASSERT_STATUS_OK(HasInMemoryCell(table, v[0].column_family_name, row_key,
                                   v[0].column_qualifier, v[0].timestamp_micros,
                                   v[0].data));

  google::bigtable::v2::CheckAndMutateRowRequest cond_mut_with_pass_all;

  cond_mut_with_pass_all.set_row_key(row_key);
  cond_mut_with_pass_all.set_table_name(table_name);
  cond_mut_with_pass_all.mutable_predicate_filter()->set_pass_all_filter(true);
  cond_mut_with_pass_all.mutable_true_mutations()->Assign(
      true_mutations.begin(), true_mutations.end());
  cond_mut_with_pass_all.mutable_false_mutations()->Assign(
      false_mutations.begin(), false_mutations.end());

  auto status_or = table->CheckAndMutateRow(cond_mut_with_pass_all);
  ASSERT_STATUS_OK(status_or);

  // pass_all_filter means that true_mutation should have succeeded,
  // so check for the true_mutation cell value e.t.c.
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, timestamp_micros,
                                   true_mutation_value));

  // And just for good measure, ensure that false_mutation was not written.
  ASSERT_EQ(false, HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, timestamp_micros,
                                   false_mutation_value)
                       .ok());
}

TEST(InMemoryConditionalMutations, RejectInvalidRequest) {
  auto const* const table_name = "projects/test/instances/test/tables/test";
  auto const* const column_family_name = "test_column_family";
  auto const* const row_key = "0";
  auto const* const column_qualifier = "column_1";
  auto timestamp_micros = 1000;
  auto const* const true_mutation_value = "set by a true mutation";
  auto const* const false_mutation_value = "set by a false mutation";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  ::google::bigtable::v2::Mutation true_mutation;
  auto* set_cell_mutation = true_mutation.mutable_set_cell();
  set_cell_mutation->set_family_name(column_family_name);
  set_cell_mutation->set_column_qualifier(column_qualifier);
  set_cell_mutation->set_timestamp_micros(timestamp_micros);
  set_cell_mutation->set_value(true_mutation_value);

  std::vector<google::bigtable::v2::Mutation> true_mutations = {true_mutation};

  ::google::bigtable::v2::Mutation false_mutation;
  set_cell_mutation = false_mutation.mutable_set_cell();
  set_cell_mutation->set_family_name(column_family_name);
  set_cell_mutation->set_column_qualifier(column_qualifier);
  set_cell_mutation->set_timestamp_micros(timestamp_micros);
  set_cell_mutation->set_value(false_mutation_value);

  // Will be configured so that row_key is not set.
  std::vector<google::bigtable::v2::Mutation> false_mutations = {
      false_mutation};

  google::bigtable::v2::CheckAndMutateRowRequest cond_mutation_no_row_key;

  cond_mutation_no_row_key.set_table_name(table_name);
  cond_mutation_no_row_key.mutable_true_mutations()->Assign(
      true_mutations.begin(), true_mutations.end());
  cond_mutation_no_row_key.mutable_false_mutations()->Assign(
      false_mutations.begin(), false_mutations.end());

  auto status_or = table->CheckAndMutateRow(cond_mutation_no_row_key);
  ASSERT_EQ(false, status_or.ok());

  // Will be configured so that both true_mutations and
  // false_mutations are empty.
  google::bigtable::v2::CheckAndMutateRowRequest cond_mutation_no_mutations;
  cond_mutation_no_mutations.set_row_key(row_key);
  cond_mutation_no_row_key.set_table_name(table_name);
  ASSERT_EQ(false, table->CheckAndMutateRow(cond_mutation_no_mutations).ok());
}

TEST(PersistentConditionalMutations, TestTrueMutations) {
  auto const* const table_name =
      "conditional_projects/test/instances/test/tables/test";
  auto const* const column_family_name = "test_column_family";
  auto const* const row_key = "0";
  auto const* const column_qualifier = "column_1";
  auto timestamp_micros = 1000;
  auto const* const true_mutation_value = "set by a true mutation";
  auto const* const false_mutation_value = "set by a false mutation";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();
  ::google::bigtable::v2::Mutation true_mutation;
  auto* set_cell_mutation = true_mutation.mutable_set_cell();
  set_cell_mutation->set_family_name(column_family_name);
  set_cell_mutation->set_column_qualifier(column_qualifier);
  set_cell_mutation->set_timestamp_micros(timestamp_micros);
  set_cell_mutation->set_value(true_mutation_value);

  std::vector<google::bigtable::v2::Mutation> true_mutations = {true_mutation};

  ::google::bigtable::v2::Mutation false_mutation;
  set_cell_mutation = false_mutation.mutable_set_cell();
  set_cell_mutation->set_family_name(column_family_name);
  set_cell_mutation->set_column_qualifier(column_qualifier);
  set_cell_mutation->set_timestamp_micros(timestamp_micros);
  set_cell_mutation->set_value(false_mutation_value);

  std::vector<google::bigtable::v2::Mutation> false_mutations = {
      false_mutation};

  std::vector<SetCellParams> v = {
      {column_family_name, "column_2", 1000, "some_value"}};
  ASSERT_STATUS_OK(SetCells(table, table_name, row_key, v));
  ASSERT_STATUS_OK(HasPersistentCell(table, v[0].column_family_name, row_key,
                                     v[0].column_qualifier,
                                     v[0].timestamp_micros, v[0].data));

  google::bigtable::v2::CheckAndMutateRowRequest cond_mut_with_pass_all;

  cond_mut_with_pass_all.set_row_key(row_key);
  cond_mut_with_pass_all.set_table_name(table_name);
  cond_mut_with_pass_all.mutable_predicate_filter()->set_pass_all_filter(true);
  cond_mut_with_pass_all.mutable_true_mutations()->Assign(
      true_mutations.begin(), true_mutations.end());
  cond_mut_with_pass_all.mutable_false_mutations()->Assign(
      false_mutations.begin(), false_mutations.end());

  auto status_or = table->CheckAndMutateRow(cond_mut_with_pass_all);
  ASSERT_STATUS_OK(status_or);

  // pass_all_filter means that true_mutation should have succeeded,
  // so check for the true_mutation cell value e.t.c.
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, timestamp_micros,
                                     true_mutation_value));

  // And just for good measure, ensure that false_mutation was not written.
  ASSERT_EQ(false, HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, timestamp_micros,
                                     false_mutation_value)
                       .ok());

  std::filesystem::remove_all("/tmp/conditional_projects");
}

TEST(PersistentConditionalMutations, RejectInvalidRequest) {
  auto const* const table_name =
      "conditional_projects/test/instances/test/tables/test";
  auto const* const column_family_name = "test_column_family";
  auto const* const row_key = "0";
  auto const* const column_qualifier = "column_1";
  auto timestamp_micros = 1000;
  auto const* const true_mutation_value = "set by a true mutation";
  auto const* const false_mutation_value = "set by a false mutation";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  ::google::bigtable::v2::Mutation true_mutation;
  auto* set_cell_mutation = true_mutation.mutable_set_cell();
  set_cell_mutation->set_family_name(column_family_name);
  set_cell_mutation->set_column_qualifier(column_qualifier);
  set_cell_mutation->set_timestamp_micros(timestamp_micros);
  set_cell_mutation->set_value(true_mutation_value);

  std::vector<google::bigtable::v2::Mutation> true_mutations = {true_mutation};

  ::google::bigtable::v2::Mutation false_mutation;
  set_cell_mutation = false_mutation.mutable_set_cell();
  set_cell_mutation->set_family_name(column_family_name);
  set_cell_mutation->set_column_qualifier(column_qualifier);
  set_cell_mutation->set_timestamp_micros(timestamp_micros);
  set_cell_mutation->set_value(false_mutation_value);

  // Will be configured so that row_key is not set.
  std::vector<google::bigtable::v2::Mutation> false_mutations = {
      false_mutation};

  google::bigtable::v2::CheckAndMutateRowRequest cond_mutation_no_row_key;

  cond_mutation_no_row_key.set_table_name(table_name);
  cond_mutation_no_row_key.mutable_true_mutations()->Assign(
      true_mutations.begin(), true_mutations.end());
  cond_mutation_no_row_key.mutable_false_mutations()->Assign(
      false_mutations.begin(), false_mutations.end());

  auto status_or = table->CheckAndMutateRow(cond_mutation_no_row_key);
  ASSERT_EQ(false, status_or.ok());

  // Will be configured so that both true_mutations and
  // false_mutations are empty.
  google::bigtable::v2::CheckAndMutateRowRequest cond_mutation_no_mutations;
  cond_mutation_no_mutations.set_row_key(row_key);
  cond_mutation_no_row_key.set_table_name(table_name);
  ASSERT_EQ(false, table->CheckAndMutateRow(cond_mutation_no_mutations).ok());

  std::filesystem::remove_all("/tmp/conditional_projects");
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google