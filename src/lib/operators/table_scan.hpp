#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan() = default;

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  class BaseTableScanImpl {
   public:
    virtual ~BaseTableScanImpl() = default;

    virtual std::shared_ptr<const Table> on_execute(TableScan& outer) = 0;
  };

  template <class T>
  class TableScanImpl : public BaseTableScanImpl {
   public:
    std::shared_ptr<const Table> on_execute(TableScan& outer) override;
  };

  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;
  std::unique_ptr<BaseTableScanImpl> _impl;
};

template <class T>
std::shared_ptr<const Table> TableScan::TableScanImpl<T>::on_execute(TableScan& outer) {
  const auto search_value = type_cast<T>(outer._search_value);
  const auto input_table = outer._input_table_left();
  Assert(outer._scan_type == ScanType::OpGreaterThan, "Not implemented");
  auto pos_list = std::make_shared<PosList>();

  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto segment = chunk.get_segment(outer._column_id);
    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
    Assert(value_segment != nullptr, "Not implemented");

    const auto& data = value_segment->values();
    for (ChunkOffset offset{0}; offset < data.size(); ++offset) {
      if (data[offset] > search_value) {
        pos_list->emplace_back(RowID{chunk_id, offset});
      }
    }
  }

  auto result_table = std::make_shared<Table>();
  Chunk result_chunk;
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    auto segment = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);
    result_chunk.add_segment(segment);
    result_table->add_column_definition(input_table->column_name(column_id), input_table->column_type(column_id));
  }

  result_table->emplace_chunk(std::move(result_chunk));
  return result_table;
}

}  // namespace opossum
