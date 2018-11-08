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
  DebugAssert(!has_table(name), "Table with that name already exists");
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

bool StorageManager::has_table(const std::string& name) const {
  return _name_table_map.find(name) != _name_table_map.end();
}

std::vector<std::string> StorageManager::table_names() const {
  auto keys = std::vector<std::string>();
  for (const auto& [key, _] : _name_table_map) {
    keys.emplace_back(key);
  }
  return keys;
}

void StorageManager::print(std::ostream& out) const {
  out << "NAME, COLUMNS, ROWS, CHUNKS\n";
  for (const auto& [key, _] : _name_table_map) {
    const auto& table = _name_table_map.at(key);
    out << key << "\t" << table->column_count() << "\t" << table->row_count() << "\t"
        << table->chunk_count() << "\n";
  }
}

void StorageManager::reset() { get() = StorageManager(); }

}  // namespace opossum
