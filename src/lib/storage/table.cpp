#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunk_size{chunk_size} { create_new_chunk(); }

void Table::add_column_definition(const std::string& name, const std::string& type) {
  DebugAssert(row_count() == 0, "Columns can only be appended to empty tables");
  DebugAssert(_name_column_map.find(name) == _name_column_map.end(), "Column with that name already exists");
  _name_column_map.emplace(name, ColumnID{column_count()});
  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  add_column_definition(name, type);
  _chunks.front().add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
}

void Table::append(std::vector<AllTypeVariant> values) {
  DebugAssert(values.size() == column_count(), "Number of passed arguments does not match number of columns");
  auto& mutable_chunk = _chunks.back();
  mutable_chunk.append(std::move(values));
  if (mutable_chunk.size() == _chunk_size) {
    create_new_chunk();
  }
}

void Table::create_new_chunk() {
  Chunk chunk;
  for (const auto& type : _column_types) {
    chunk.add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
  }

  {
    auto guard = std::lock_guard{_compression_mutex};
    _chunks.emplace_back(std::move(chunk));
    _compressed_chunks.emplace_back(false);
  }
}

uint16_t Table::column_count() const { return static_cast<uint16_t>(_name_column_map.size()); }

uint64_t Table::row_count() const {
  DebugAssert(!_chunks.empty(), "There should always be at least one chunk");
  return std::accumulate(_chunks.begin(), _chunks.end(), uint64_t{0},
                         [](const auto sum, const Chunk& chunk) { return sum + chunk.size(); });
}

ChunkID Table::chunk_count() const { return ChunkID{static_cast<uint32_t>(_chunks.size())}; }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto id_it = _name_column_map.find(column_name);
  DebugAssert(id_it != _name_column_map.end(), "Unknown column name");
  return id_it->second;
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const {
  DebugAssert(column_id < column_count(), "Column id out of range");
  return _column_names[column_id];
}

const std::string& Table::column_type(ColumnID column_id) const {
  DebugAssert(column_id < column_count(), "Column id out of range");
  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < chunk_count(), "Chunk id out of range");
  return _chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < chunk_count(), "Chunk id out of range");
  return _chunks[chunk_id];
}

void Table::emplace_chunk(Chunk&& chunk) {
  DebugAssert(chunk.column_count() == column_count(), "chunk and table must have equal column count for emplace");

  if (_chunks.back().size() == 0) {
    _chunks.back() = std::move(chunk);
  } else {
    _chunks.emplace_back(std::move(chunk));
  }
}

void Table::compress_chunk(ChunkID chunk_id) {
  {
    auto guard = std::lock_guard{_compression_mutex};
    if (_compressed_chunks[chunk_id]) {
      return;
    }

    _compressed_chunks[chunk_id] = true;
  }

  Chunk compressed_chunk;
  const auto& chunk = get_chunk(chunk_id);
  for (ColumnID column_id{0}; column_id < chunk.column_count(); ++column_id) {
    const auto segment = chunk.get_segment(column_id);
    const auto& column_type = _column_types[column_id];
    const auto compressed_segment = make_shared_by_data_type<BaseSegment, DictionarySegment>(column_type, segment);
    compressed_chunk.add_segment(std::move(compressed_segment));
  }

  _chunks[chunk_id] = std::move(compressed_chunk);
}

}  // namespace opossum
