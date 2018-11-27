#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "storage/fitted_attribute_vector.hpp"

namespace opossum {

class StorageFittedAttributeVectorTest : public BaseTest {};

TEST_F(StorageFittedAttributeVectorTest, FixedSize) {
  FittedAttributeVector<uint32_t> attribute_vector(10);
  EXPECT_EQ(attribute_vector.size(), 10u);
}

TEST_F(StorageFittedAttributeVectorTest, StoringValues) {
  FittedAttributeVector<uint32_t> attribute_vector(10);
  for (uint32_t i = 0; i < 10; ++i) {
    attribute_vector.set(i, ValueID{100 + i});
  }

  EXPECT_EQ(attribute_vector.get(0), 100u);
  EXPECT_EQ(attribute_vector.get(9), 109u);
}

TEST_F(StorageFittedAttributeVectorTest, TestsValueIDRange) {
  FittedAttributeVector<uint8_t> attribute_vector_uint8(1);
  FittedAttributeVector<uint16_t> attribute_vector_uint16(1);

  EXPECT_THROW(attribute_vector_uint8.set(0, ValueID{0x100}), std::logic_error);
  EXPECT_THROW(attribute_vector_uint8.set(0, ValueID{0x10000}), std::logic_error);
  // Test not possible for uint32_t because ValueID is also limited to 32 bits
}

TEST_F(StorageFittedAttributeVectorTest, ByteWidth) {
  FittedAttributeVector<uint8_t> attribute_vector_uint8(1);
  FittedAttributeVector<uint16_t> attribute_vector_uint16(1);
  FittedAttributeVector<uint32_t> attribute_vector_uint32(1);

  EXPECT_EQ(attribute_vector_uint8.width(), 1u);
  EXPECT_EQ(attribute_vector_uint16.width(), 2u);
  EXPECT_EQ(attribute_vector_uint32.width(), 4u);
}

TEST_F(StorageFittedAttributeVectorTest, RetrieveIndices) {
  FittedAttributeVector<uint32_t> attribute_vector(10);
  for (uint32_t i = 0; i < 10; ++i) {
    attribute_vector.set(i, ValueID{100 + i});
  }

  const auto& indices = attribute_vector.indices();
  EXPECT_EQ(indices[1], 101u);
  EXPECT_EQ(indices[7], 107u);
}

}  // namespace opossum
