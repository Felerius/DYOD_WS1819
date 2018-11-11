#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../utils/assert.hpp"
#include "all_type_variant.hpp"
#include "base_attribute_vector.hpp"
#include "base_segment.hpp"
#include "fitted_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    const auto size = base_segment->size();

    // Create map as temporary dictionary
    std::map<T, uint32_t> temp_dictionary{};
    for (ValueID value_id{0}; value_id < size; ++value_id) {
      auto value = type_cast<T>((*base_segment)[value_id]);
      temp_dictionary[value] = 0;
    }
    const auto unique_values = temp_dictionary.size();

    // Create dictionary
    _dictionary = std::make_shared<std::vector<T>>(unique_values);
    uint64_t counter = 0;
    for (auto& [key, value] : temp_dictionary) {
      value = counter;
      (*_dictionary)[counter] = key;
      counter++;
    }

    // Create attribute vector
    if (unique_values < std::numeric_limits<uint8_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint8_t>>(size);
    } else if (unique_values < std::numeric_limits<uint16_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint16_t>>(size);
    } else {
      DebugAssert(unique_values < std::numeric_limits<uint32_t>::max(), "Segments cannot be larger than 2^32 items");
      _attribute_vector = std::make_shared<FittedAttributeVector<uint32_t>>(size);
    }

    // Fill attribute vector
    for (ValueID value_id{0}; value_id < size; ++value_id) {
      auto value = type_cast<T>((*base_segment)[value_id]);
      auto compressed_value = temp_dictionary[value];
      _attribute_vector->set(compressed_value, value_id);
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override { return get(i); }

  // return the value at a certain position.
  const T get(const size_t i) const { return (*_dictionary)[_attribute_vector->get(i)]; }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override {
    throw std::runtime_error{"Cannot call append on immutable dictionary segment"};
  }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return (*_dictionary)[value_id]; }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    const auto it = std::lower_bound(_dictionary->begin(), _dictionary->end(), value);
    const auto position = ValueID{static_cast<uint32_t>(it - _dictionary->begin())};
    return it == _dictionary->end() ? INVALID_VALUE_ID : position;
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    const auto it = std::upper_bound(_dictionary->begin(), _dictionary->end(), value);
    const auto position = ValueID{static_cast<uint32_t>(it - _dictionary->begin())};
    return it == _dictionary->end() ? INVALID_VALUE_ID : position;
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
