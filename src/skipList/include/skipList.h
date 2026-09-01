#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <stdexcept>
#include <utility>
#include <vector>

#define STORE_FILE "store/dumpFile"
static std::string delimiter = ":";

template <typename K, typename V>
class Node {
 public:
  Node() = default;
  Node(K k, V v, int);
  ~Node();
  K get_key() const;
  V get_value() const;
  void set_value(V);
  Node<K, V>** forward;
  int node_level;

 private:
  K key;
  V value;
};

template <typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level)
    : forward(new Node<K, V>*[level + 1]), node_level(level), key(k), value(v) {
  memset(forward, 0, sizeof(Node<K, V>*) * (level + 1));
}

template <typename K, typename V>
Node<K, V>::~Node() {
  delete[] forward;
}

template <typename K, typename V>
K Node<K, V>::get_key() const {
  return key;
}

template <typename K, typename V>
V Node<K, V>::get_value() const {
  return value;
}

template <typename K, typename V>
void Node<K, V>::set_value(V value) {
  this->value = value;
}

template <typename K, typename V>
class SkipListDump {
 public:
  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& keyDumpVt_;
    ar& valDumpVt_;
  }

  std::vector<K> keyDumpVt_;
  std::vector<V> valDumpVt_;

  void insert(const Node<K, V>& node);
};

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V>& node) {
  keyDumpVt_.emplace_back(node.get_key());
  valDumpVt_.emplace_back(node.get_value());
}

template <typename K, typename V>
class SkipList {
 public:
  explicit SkipList(int);
  ~SkipList();
  int get_random_level();
  Node<K, V>* create_node(K, V, int);
  int insert_element(K, V);
  void display_list();
  bool search_element(K, V& value);
  void delete_element(K);
  void insert_set_element(K&, V&);
  std::string dump_file();
  void load_file(const std::string& dumpStr);
  void clear(Node<K, V>*);
  int size();

 private:
  int getRandomLevelUnlocked();
  Node<K, V>* createNodeUnlocked(const K&, const V&, int);
  int insertElementUnlocked(const K&, const V&);
  void clearUnlocked();
  void clearNodesUnlocked(Node<K, V>*);
  Node<K, V>* findNodeUnlocked(const K&);
  void get_key_value_from_string(const std::string& str, std::string* key, std::string* value);
  bool is_valid_string(const std::string& str);

  int _max_level;
  int _skip_list_level;
  Node<K, V>* _header;
  std::ofstream _file_writer;
  std::ifstream _file_reader;
  int _element_count;
  std::mutex _mtx;
};

template <typename K, typename V>
Node<K, V>* SkipList<K, V>::createNodeUnlocked(const K& key, const V& value, int level) {
  return new Node<K, V>(key, value, level);
}

template <typename K, typename V>
Node<K, V>* SkipList<K, V>::create_node(K key, V value, int level) {
  std::lock_guard<std::mutex> lock(_mtx);
  return createNodeUnlocked(key, value, level);
}

template <typename K, typename V>
int SkipList<K, V>::getRandomLevelUnlocked() {
  int level = 1;
  while (rand() % 2) {
    ++level;
  }
  return level < _max_level ? level : _max_level;
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level() {
  std::lock_guard<std::mutex> lock(_mtx);
  return getRandomLevelUnlocked();
}

template <typename K, typename V>
Node<K, V>* SkipList<K, V>::findNodeUnlocked(const K& key) {
  Node<K, V>* current = _header;
  for (int level = _skip_list_level; level >= 0; --level) {
    while (current->forward[level] != nullptr && current->forward[level]->get_key() < key) {
      current = current->forward[level];
    }
  }
  current = current->forward[0];
  return current != nullptr && current->get_key() == key ? current : nullptr;
}

template <typename K, typename V>
int SkipList<K, V>::insertElementUnlocked(const K& key, const V& value) {
  Node<K, V>* current = _header;
  std::vector<Node<K, V>*> update(_max_level + 1, nullptr);
  for (int level = _skip_list_level; level >= 0; --level) {
    while (current->forward[level] != nullptr && current->forward[level]->get_key() < key) {
      current = current->forward[level];
    }
    update[level] = current;
  }

  current = current->forward[0];
  if (current != nullptr && current->get_key() == key) {
    return 1;
  }

  const int randomLevel = getRandomLevelUnlocked();
  if (randomLevel > _skip_list_level) {
    for (int level = _skip_list_level + 1; level <= randomLevel; ++level) {
      update[level] = _header;
    }
    _skip_list_level = randomLevel;
  }

  Node<K, V>* insertedNode = createNodeUnlocked(key, value, randomLevel);
  for (int level = 0; level <= randomLevel; ++level) {
    insertedNode->forward[level] = update[level]->forward[level];
    update[level]->forward[level] = insertedNode;
  }
  ++_element_count;
  return 0;
}

template <typename K, typename V>
int SkipList<K, V>::insert_element(K key, V value) {
  std::lock_guard<std::mutex> lock(_mtx);
  return insertElementUnlocked(key, value);
}

template <typename K, typename V>
void SkipList<K, V>::display_list() {
  std::lock_guard<std::mutex> lock(_mtx);
  std::cout << "\n*****Skip List*****\n";
  for (int level = 0; level <= _skip_list_level; ++level) {
    Node<K, V>* node = _header->forward[level];
    std::cout << "Level " << level << ": ";
    while (node != nullptr) {
      std::cout << node->get_key() << ":" << node->get_value() << ";";
      node = node->forward[level];
    }
    std::cout << std::endl;
  }
}

template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V& value) {
  std::lock_guard<std::mutex> lock(_mtx);
  Node<K, V>* node = findNodeUnlocked(key);
  if (node == nullptr) {
    return false;
  }
  value = node->get_value();
  return true;
}

template <typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
  std::lock_guard<std::mutex> lock(_mtx);
  Node<K, V>* current = _header;
  std::vector<Node<K, V>*> update(_max_level + 1, nullptr);
  for (int level = _skip_list_level; level >= 0; --level) {
    while (current->forward[level] != nullptr && current->forward[level]->get_key() < key) {
      current = current->forward[level];
    }
    update[level] = current;
  }

  current = current->forward[0];
  if (current == nullptr || current->get_key() != key) {
    return;
  }
  for (int level = 0; level <= _skip_list_level; ++level) {
    if (update[level]->forward[level] != current) {
      break;
    }
    update[level]->forward[level] = current->forward[level];
  }
  while (_skip_list_level > 0 && _header->forward[_skip_list_level] == nullptr) {
    --_skip_list_level;
  }
  delete current;
  --_element_count;
}

template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K& key, V& value) {
  std::lock_guard<std::mutex> lock(_mtx);
  Node<K, V>* node = findNodeUnlocked(key);
  if (node != nullptr) {
    node->set_value(value);
    return;
  }
  insertElementUnlocked(key, value);
}

template <typename K, typename V>
std::string SkipList<K, V>::dump_file() {
  SkipListDump<K, V> dumper;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    for (Node<K, V>* node = _header->forward[0]; node != nullptr; node = node->forward[0]) {
      dumper.insert(*node);
    }
  }
  std::stringstream stream;
  boost::archive::text_oarchive archive(stream);
  archive << dumper;
  return stream.str();
}

template <typename K, typename V>
void SkipList<K, V>::clearNodesUnlocked(Node<K, V>* node) {
  while (node != nullptr) {
    Node<K, V>* next = node->forward[0];
    delete node;
    node = next;
  }
}

template <typename K, typename V>
void SkipList<K, V>::clearUnlocked() {
  clearNodesUnlocked(_header->forward[0]);
  for (int level = 0; level <= _max_level; ++level) {
    _header->forward[level] = nullptr;
  }
  _skip_list_level = 0;
  _element_count = 0;
}

template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string& dumpStr) {
  if (dumpStr.empty()) {
    return;
  }

  SkipListDump<K, V> dumper;
  std::stringstream stream(dumpStr);
  boost::archive::text_iarchive archive(stream);
  archive >> dumper;

  if (dumper.keyDumpVt_.size() != dumper.valDumpVt_.size()) {
    throw std::runtime_error("skip-list snapshot has mismatched key/value counts");
  }

  SkipList<K, V> staged(_max_level);
  for (std::size_t index = 0; index < dumper.keyDumpVt_.size(); ++index) {
    staged.insertElementUnlocked(dumper.keyDumpVt_[index], dumper.valDumpVt_[index]);
  }

  std::lock_guard<std::mutex> lock(_mtx);
  std::swap(_skip_list_level, staged._skip_list_level);
  std::swap(_header, staged._header);
  std::swap(_element_count, staged._element_count);
}

template <typename K, typename V>
int SkipList<K, V>::size() {
  std::lock_guard<std::mutex> lock(_mtx);
  return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string& str, std::string* key,
                                                std::string* value) {
  if (!is_valid_string(str)) {
    return;
  }
  *key = str.substr(0, str.find(delimiter));
  *value = str.substr(str.find(delimiter) + 1, str.length());
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string& str) {
  return !str.empty() && str.find(delimiter) != std::string::npos;
}

template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level)
    : _max_level(max_level),
      _skip_list_level(0),
      _header(new Node<K, V>(K(), V(), max_level)),
      _element_count(0) {}

template <typename K, typename V>
SkipList<K, V>::~SkipList() {
  std::lock_guard<std::mutex> lock(_mtx);
  if (_file_writer.is_open()) {
    _file_writer.close();
  }
  if (_file_reader.is_open()) {
    _file_reader.close();
  }
  clearUnlocked();
  delete _header;
}

template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V>* node) {
  std::lock_guard<std::mutex> lock(_mtx);
  if (node == _header->forward[0]) {
    clearUnlocked();
    return;
  }
  clearNodesUnlocked(node);
}

#endif  // SKIPLIST_H
