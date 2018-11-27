#include "fitted_attribute_vector.hpp"

#include <limits>
#include <vector>

#include "../utils/assert.hpp"

namespace opossum {

template <typename T>
FittedAttributeVector<T>::FittedAttributeVector(size_t size) : _indices(size) {}

template <typename T>
ValueID FittedAttributeVector<T>::get(const size_t i) const {
  return ValueID{_indices[i]};
}

template <typename T>
void FittedAttributeVector<T>::set(const size_t i, const ValueID value_id) {
  DebugAssert(static_cast<uint32_t>(value_id) <= std::numeric_limits<T>::max(),
              "Value id out of range for value id type");
  _indices[i] = value_id;
}

template <typename T>
size_t FittedAttributeVector<T>::size() const {
  return _indices.size();
}

template <typename T>
AttributeVectorWidth FittedAttributeVector<T>::width() const {
  return sizeof(T);
}

template <class T>
const std::vector<T>& FittedAttributeVector<T>::indices() const {
  return _indices;
}

template class FittedAttributeVector<uint8_t>;
template class FittedAttributeVector<uint16_t>;
template class FittedAttributeVector<uint32_t>;

}  // namespace opossum
