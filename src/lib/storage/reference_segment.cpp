#include "reference_segment.hpp"

#include "utils/assert.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table> referenced_table,
                                   const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos)
    : _referenced_table{referenced_table}, _referenced_column_id{referenced_column_id}, _pos_list{pos} {}

const AllTypeVariant ReferenceSegment::operator[](const size_t i) const {
  DebugAssert(i < _pos_list->size(), "Index to reference segment out of bounds");
  const auto row_id = (*_pos_list)[i];
  const auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);
  const auto segment = chunk.get_segment(_referenced_column_id);
  return (*segment)[row_id.chunk_offset];
}

size_t ReferenceSegment::size() const { return _pos_list->size(); }

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const { return _pos_list; }

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const { return _referenced_table; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

}  // namespace opossum
