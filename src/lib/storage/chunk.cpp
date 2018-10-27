#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) {
  _segments.emplace_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(), "Number of passed arguments does not match number of columns");

  for (ColumnID column_id{0}; column_id < values.size(); ++column_id) {
    get_segment(column_id)->append(values[column_id]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  return _segments[column_id];
}

uint16_t Chunk::column_count() const {
  return static_cast<uint16_t>(_segments.size());
}

uint32_t Chunk::size() const {
  if (_segments.empty()) {
    return 0;
  }

  return static_cast<uint32_t>(get_segment(ColumnID{0})->size());
}

}  // namespace opossum
