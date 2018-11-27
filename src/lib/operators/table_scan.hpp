#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/dictionary_segment.hpp"
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
   private:
    template <ScanType scan_op>
    void _scan_value_segment(PosList& pos_list, ChunkID chunk_id, const T& search_value,
                             const ValueSegment<T>& segment);

    template <ScanType scan_op>
    void _scan_dictionary_segment(PosList& pos_list, ChunkID chunk_id, const T& search_value,
                                  const DictionarySegment<T>& segment);

    template <ScanType scan_op>
    void _scan_reference_segment(PosList& pos_list, ChunkID chunk_id, const T& search_value,
                                 const ReferenceSegment& segment);

    template <ScanType scan_op>
    std::shared_ptr<const Table> _on_execute_internal(TableScan& outer);

   public:
    std::shared_ptr<const Table> on_execute(TableScan& outer) override;
  };

  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;
};

}  // namespace opossum
