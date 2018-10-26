#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  _name_table_map.emplace(name, table);
}

void StorageManager::drop_table(const std::string& name) {
  DebugAssert(has_table(name), "Cannot drop a table that does not exist");
  _name_table_map.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  DebugAssert(has_table(name), "Cannot get a table that does not exist");
  return _name_table_map.at(name);
}

bool StorageManager::has_table(const std::string& name) const { return _name_table_map.count(name) == 1; }

std::vector<std::string> StorageManager::table_names() const {
  auto keys = std::vector<std::string>();
  for (const auto& key_val_pair : _name_table_map) {
    keys.emplace_back(key_val_pair.first);
  }
  return keys;
}

void StorageManager::print(std::ostream& out) const {
  // Implementation goes here
}

void StorageManager::reset() { get() = StorageManager(); }

}  // namespace opossum
