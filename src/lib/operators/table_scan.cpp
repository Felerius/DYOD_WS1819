#include "table_scan.hpp"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/base_attribute_vector.hpp"
#include "storage/fitted_attribute_vector.hpp"

namespace opossum {

template <class T, ScanType scan_op>
auto comparator() {
  if constexpr (scan_op == ScanType::OpEquals) {
    return std::equal_to<T>{};
  } else if constexpr (scan_op == ScanType::OpNotEquals) {
    return std::not_equal_to<T>{};
  } else if constexpr (scan_op == ScanType::OpGreaterThan) {
    return std::greater<T>{};
  } else if constexpr (scan_op == ScanType::OpGreaterThanEquals) {
    return std::greater_equal<T>{};
  } else if constexpr (scan_op == ScanType::OpLessThan) {
    return std::less<T>{};
  } else {
    static_assert(scan_op == ScanType::OpLessThanEquals, "Unknown scan type");
    return std::less_equal<T>{};
  }
}

template <ScanType scan_op, class T>
void scan_vector(const std::vector<T>& data, PosList& pos_list, T search_value, ChunkID chunk_id) {
  auto compare = comparator<T, scan_op>();
  for (ChunkOffset offset{0}; offset < data.size(); ++offset) {
    if (compare(data[offset], search_value)) {
      pos_list.emplace_back(RowID{chunk_id, offset});
    }
  }
}

template <ScanType scan_op>
void scan_attribute_vector(std::shared_ptr<const BaseAttributeVector> attribute_vector, PosList& pos_list,
                           ValueID search_value, ChunkID chunk_id) {
  if (const auto uint8_vec = std::dynamic_pointer_cast<const FittedAttributeVector<uint8_t>>(attribute_vector);
      uint8_vec != nullptr) {
    scan_vector<scan_op>(uint8_vec->indices(), pos_list, static_cast<uint8_t>(search_value), chunk_id);
  } else if (const auto uint16_vec = std::dynamic_pointer_cast<const FittedAttributeVector<uint16_t>>(attribute_vector);
             uint16_vec != nullptr) {
    scan_vector<scan_op>(uint16_vec->indices(), pos_list, static_cast<uint16_t>(search_value), chunk_id);
  } else if (const auto uint32_vec = std::dynamic_pointer_cast<const FittedAttributeVector<uint32_t>>(attribute_vector);
             uint32_vec != nullptr) {
    scan_vector<scan_op>(uint32_vec->indices(), pos_list, static_cast<uint32_t>(search_value), chunk_id);
  } else {
    Fail("TableScan not implemented for this type of attribute vector");
  }
}

void full_scan(std::shared_ptr<const BaseAttributeVector> attribute_vector, PosList& pos_list, ChunkID chunk_id) {
  scan_attribute_vector<ScanType::OpNotEquals>(attribute_vector, pos_list, INVALID_VALUE_ID, chunk_id);
}

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator{in}, _column_id{column_id}, _scan_type{scan_type}, _search_value{search_value} {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::string get_type_string(int index){
  switch (index) {
    case 0: return hana::at_c<0>(detail::type_strings);
    case 1: return hana::at_c<1>(detail::type_strings);
    case 2: return hana::at_c<2>(detail::type_strings);
    case 3: return hana::at_c<3>(detail::type_strings);
    case 4: return hana::at_c<4>(detail::type_strings);
    default:
      Fail("unexpected type string index");
      return "";
  }
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto& data_type = _input_table_left()->column_type(_column_id);
  DebugAssert(data_type == get_type_string(_search_value.which()), "data types of column and search value must match for table scan");
  auto impl = make_unique_by_data_type<BaseTableScanImpl, TableScanImpl>(data_type);
  return impl->on_execute(*this);
}

template <class T>
std::shared_ptr<const Table> TableScan::TableScanImpl<T>::on_execute(TableScan& outer) {
  switch (outer._scan_type) {
    case ScanType::OpEquals:
      return _on_execute_internal<ScanType::OpEquals>(outer);
    case ScanType::OpNotEquals:
      return _on_execute_internal<ScanType::OpNotEquals>(outer);
    case ScanType::OpGreaterThan:
      return _on_execute_internal<ScanType::OpGreaterThan>(outer);
    case ScanType::OpGreaterThanEquals:
      return _on_execute_internal<ScanType::OpGreaterThanEquals>(outer);
    case ScanType::OpLessThan:
      return _on_execute_internal<ScanType::OpLessThan>(outer);
    case ScanType::OpLessThanEquals:
      return _on_execute_internal<ScanType::OpLessThanEquals>(outer);
    default:
      Fail("Invalid scan type");
      return {};
  }
}

template <class T>
template <ScanType scan_op>
std::shared_ptr<const Table> TableScan::TableScanImpl<T>::_on_execute_internal(TableScan& outer) {
  const auto search_value = type_cast<T>(outer._search_value);
  const auto input_table = outer._input_table_left();
  auto pos_list = std::make_shared<PosList>();
  std::shared_ptr<const Table> referenced_table = input_table;

  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto segment = chunk.get_segment(outer._column_id);
    if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment); value_segment != nullptr) {
      _scan_value_segment<scan_op>(*pos_list, chunk_id, search_value, *value_segment);
    } else if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
               dictionary_segment != nullptr) {
      _scan_dictionary_segment<scan_op>(*pos_list, chunk_id, search_value, *dictionary_segment);
    } else if (const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
               reference_segment != nullptr) {
      _scan_reference_segment<scan_op>(*pos_list, chunk_id, search_value, *reference_segment);
      referenced_table = reference_segment->referenced_table();
    }
  }

  auto result_table = std::make_shared<Table>();
  Chunk result_chunk;
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    auto segment = std::make_shared<ReferenceSegment>(referenced_table, column_id, pos_list);
    result_chunk.add_segment(segment);
    result_table->add_column_definition(input_table->column_name(column_id), input_table->column_type(column_id));
  }

  result_table->emplace_chunk(std::move(result_chunk));
  return result_table;
}

template <class T>
template <ScanType scan_op>
void TableScan::TableScanImpl<T>::_scan_value_segment(PosList& pos_list, ChunkID chunk_id, const T& search_value,
                                                      const ValueSegment<T>& segment) {
  const auto& data = segment.values();
  scan_vector<scan_op>(data, pos_list, search_value, chunk_id);
}

template <class T>
template <ScanType scan_op>
void TableScan::TableScanImpl<T>::_scan_dictionary_segment(PosList& pos_list, ChunkID chunk_id, const T& search_value,
                                                           const DictionarySegment<T>& segment) {
  auto search_value_id = segment.lower_bound(search_value);
  const auto attribute_vector = segment.attribute_vector();

  if (search_value_id == INVALID_VALUE_ID) {
    if constexpr (scan_op == ScanType::OpLessThanEquals || scan_op == ScanType::OpLessThan ||
                  scan_op == ScanType::OpNotEquals) {
      full_scan(attribute_vector, pos_list, chunk_id);
    }

    return;
  }

  if constexpr (scan_op == ScanType::OpGreaterThanEquals || scan_op == ScanType::OpLessThan) {
    scan_attribute_vector<scan_op>(attribute_vector, pos_list, search_value_id, chunk_id);
  } else if (segment.value_by_value_id(search_value_id) == search_value) {
    scan_attribute_vector<scan_op>(attribute_vector, pos_list, search_value_id, chunk_id);
  } else {
    if constexpr (scan_op == ScanType::OpNotEquals) {
      full_scan(attribute_vector, pos_list, chunk_id);
    } else if constexpr (scan_op == ScanType::OpGreaterThan) {
      scan_attribute_vector<ScanType::OpGreaterThanEquals>(attribute_vector, pos_list, search_value_id, chunk_id);
    } else if constexpr (scan_op == ScanType::OpLessThanEquals) {
      scan_attribute_vector<ScanType::OpLessThan>(attribute_vector, pos_list, search_value_id, chunk_id);
    }
    // Operator == and element not in dictionary -> no matching values
  }
}

template <class T>
template <ScanType scan_op>
void TableScan::TableScanImpl<T>::_scan_reference_segment(PosList& pos_list, ChunkID chunk_id, const T& search_value,
                                                          const ReferenceSegment& segment) {
  const auto& table = segment.referenced_table();
  const auto compare = comparator<T, scan_op>();

  for (const auto& row_id : *segment.pos_list()) {
    const auto& chunk = table->get_chunk(row_id.chunk_id);
    const auto& referenced_segment = *chunk.get_segment(segment.referenced_column_id());
    if (compare(type_cast<T>(referenced_segment[row_id.chunk_offset]), search_value)) {
      pos_list.emplace_back(row_id);
    }
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(TableScan::TableScanImpl);

}  // namespace opossum
