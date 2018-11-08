#pragma once

#include "base_attribute_vector.hpp"

namespace opossum {

template <typename T>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  explicit FittedAttributeVector(size_t size);

  ValueID get(const size_t i) const override;

  void set(const size_t i, const ValueID value_id) override;

  size_t size() const override;

  AttributeVectorWidth width() const override;

 private:
  std::vector<T> _indices;
};

}  // namespace opossum