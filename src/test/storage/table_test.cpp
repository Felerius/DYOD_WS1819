#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/dictionary_segment.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, TableAppend) { EXPECT_THROW(t.append({4, "Hello,", 45, 3}), std::exception); }

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(ChunkID{0});
  // TODO(anyone): Do we want checks here?
  EXPECT_THROW(t.get_chunk(ChunkID{1}), std::exception);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(ChunkID{1});
}

TEST_F(StorageTableTest, ColumnCount) { EXPECT_EQ(t.column_count(), 2u); }

TEST_F(StorageTableTest, AddColumn) {
  EXPECT_EQ(t.column_count(), 2u);
  t.add_column("col_0", "int");
  EXPECT_EQ(t.column_count(), 3u);
  EXPECT_THROW(t.add_column("col_0", "int"), std::exception);
  t.append({4, "Hello,", {5}});
  EXPECT_THROW(t.add_column("col_28", "string"), std::exception);
}

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  // TODO(anyone): Do we want checks here?
  EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  // TODO(anyone): Do we want checks here?
  EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.chunk_size(), 2u); }

TEST_F(StorageTableTest, CompressChunk) {
  t.append({1, "Hello"});
  t.append({2, "World"});
  t.compress_chunk(ChunkID{0});
  const auto& chunk = t.get_chunk(ChunkID{0});
  const auto& first_segment = chunk.get_segment(ColumnID{0});
  EXPECT_NE(dynamic_cast<DictionarySegment<int>*>(first_segment.get()), nullptr);
}

TEST_F(StorageTableTest, EmplaceChunk) {
  EXPECT_EQ(t.chunk_count(), 1u);
  Chunk c;
  auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>("int");
  auto segment2 = make_shared_by_data_type<BaseSegment, ValueSegment>("string");
  c.add_segment(segment);
  c.add_segment(segment2);
  c.append({42, "test_string"});
  t.emplace_chunk(std::move(c));
  EXPECT_EQ(t.chunk_count(), 1u);

  Chunk c2;
  auto segment3 = make_shared_by_data_type<BaseSegment, ValueSegment>("int");
  auto segment4 = make_shared_by_data_type<BaseSegment, ValueSegment>("string");
  c2.add_segment(segment3);
  c2.add_segment(segment4);
  t.emplace_chunk(std::move(c2));
  EXPECT_EQ(t.chunk_count(), 2u);
}

}  // namespace opossum
