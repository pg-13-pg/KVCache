#ifndef SKIPLIST_H
#define SKIPLIST_H
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#define STORE_FILE "store/dumpFile"
static std::string delimiter = ":";

//跳表节点
template <typename K, typename V>
class Node {
 public:
  Node() {}
  Node(K k, V v, int);
  ~Node();
  K get_key() const;
  V get_value() const;
  void set_value(V);
  Node<K, V> **forward; // forward 是一个指针数组，forward[i] 指向当前节点在第 i 层的下一个节点
  int node_level;  //节点指针层数
 private:
  K key;
  V value;
};

template <typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level) {
  this->key = k;
  this->value = v;
  this->node_level = level;
  this->forward = new Node<K, V> *[level + 1];
  memset(this->forward, 0, sizeof(Node<K, V> *) * (level + 1)); //初始化为0
};

template <typename K, typename V>
Node<K, V>::~Node() {
  delete[] forward;
};
template <typename K, typename V>
K Node<K, V>::get_key() const {
  return key;
};
template <typename K, typename V>
V Node<K, V>::get_value() const {
  return value;
};
template <typename K, typename V>
void Node<K, V>::set_value(V value) {
  this->value = value;
};



//用来保存/序列化跳表数据的辅助类。
template <typename K, typename V>
class SkipListDump {
 public:
  friend class boost::serialization::access; 
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &keyDumpVt_;
    ar &valDumpVt_;
  }
  std::vector<K> keyDumpVt_;
  std::vector<V> valDumpVt_;
 public:
  void insert(const Node<K, V> &node);
};

//保存node的key和value到SkipListDump对象中，方便后续序列化。
template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V> &node) {
  keyDumpVt_.emplace_back(node.get_key());
  valDumpVt_.emplace_back(node.get_value());
}

// Class template for Skip list
template <typename K, typename V>
class SkipList {
 public:
  SkipList(int);
  ~SkipList();
  int get_random_level();  //获取随机层数
  Node<K, V> *create_node(K, V, int);
  int insert_element(K, V);  
  void display_list(); //打印跳表
  bool search_element(K, V &value);
  void delete_element(K);
  void insert_set_element(K &, V &);  //插入元素到跳表中，如果key已经存在，则更新value
  std::string dump_file();  //把跳表数据序列化成字符串，方便存储到文件中
  void load_file(const std::string &dumpStr);  //把字符串反序列化成跳表数据，方便从文件中恢复数据
  void clear(Node<K, V> *);  //递归删除节点
  int size();

 private:
  //从字符串中解析出key和value，返回true表示解析成功，false表示解析失败
  void get_key_value_from_string(const std::string &str, std::string *key, std::string *value);
  bool is_valid_string(const std::string &str); //判断字符串是否是合法的 key/value 格式。

 private:
  int _max_level;  //最大层数
  int _skip_list_level;  //当前跳表的层数
  Node<K, V> *_header;  //跳表头节点
  std::ofstream _file_writer;  //文件写入器
  std::ifstream _file_reader;  //文件读取器
  int _element_count;  //当前跳表中元素的数量
  std::mutex _mtx;  
};

//创建新节点
template <typename K, typename V>
Node<K, V> *SkipList<K, V>::create_node(const K k, const V v, int level) {
  Node<K, V> *n = new Node<K, V>(k, v, level);
  return n;
}

//插入元素到跳表中
template <typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value) {
  _mtx.lock();
  Node<K, V> *current = this->_header;
  Node<K, V> *update[_max_level + 1]; //在第 i 层，待插入节点应该插到哪个节点后面。
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));
  //从最高层开始查找，找到每一层待插入节点的前驱节点保存到 update 数组中
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }
  current = current->forward[0]; //把 current 移动到第 0 层中“可能等于 key 的那个节点”
  //key 已经存在，返回1 
  if (current != NULL && current->get_key() == key) { 
    std::cout << "key: " << key << ", exists" << std::endl;
    _mtx.unlock();
    return 1;
  }
  //key 不存在，插入新节点
  if (current == NULL || current->get_key() != key) {
    int random_level = get_random_level();
    //新节点的层数大于当前跳表的层数，则把 update 数组中高于当前跳表层数的元素都指向头节点，并更新跳表的层数
    if (random_level > _skip_list_level) {
      for (int i = _skip_list_level + 1; i < random_level + 1; i++) {
        update[i] = _header;
      }
      _skip_list_level = random_level;
    }
    Node<K, V> *inserted_node = create_node(key, value, random_level);

    for (int i = 0; i <= random_level; i++) {
      inserted_node->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = inserted_node;
    }
    std::cout << "Successfully inserted key:" << key << ", value:" << value << std::endl;
    _element_count++;
  }
  _mtx.unlock();
  return 0;
}


//按层级打印跳表
template <typename K, typename V>
void SkipList<K, V>::display_list() {
  std::cout << "\n*****Skip List*****"<< "\n";
  for (int i = 0; i <= _skip_list_level; i++) {
    Node<K, V> *node = this->_header->forward[i];
    std::cout << "Level " << i << ": ";
    while (node != NULL) {
      std::cout << node->get_key() << ":" << node->get_value() << ";";
      node = node->forward[i];
    }
    std::cout << std::endl;
  }
}

//序列化跳表数据
template <typename K, typename V>
std::string SkipList<K, V>::dump_file() {
  SkipListDump<K, V> dumper;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    Node<K, V> *node = this->_header->forward[0];
    while (node != nullptr) {
    dumper.insert(*node);
    node = node->forward[0];
    }
  }
  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << dumper;
  
  return ss.str();
}

// 
template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr) {
  if (dumpStr.empty()) {
    return;
  }
  SkipListDump<K, V> dumper;
  std::stringstream iss(dumpStr);
  boost::archive::text_iarchive ia(iss);
  ia >> dumper;
  std::lock_guard<std::mutex> lock(_mtx);
  for (int i = 0; i < dumper.keyDumpVt_.size(); ++i) {
    insert_element(dumper.keyDumpVt_[i], dumper.keyDumpVt_[i]);
  }
}

// 跳表节点个数
template <typename K, typename V>
int SkipList<K, V>::size() {
  return _element_count;
}

//从字符串中解析出key和value，返回true表示解析成功，false表示解析失败
template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string &str, std::string *key, std::string *value) {
  if (!is_valid_string(str)) {
    return;
  }
  *key = str.substr(0, str.find(delimiter));
  *value = str.substr(str.find(delimiter) + 1, str.length());
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string &str) {
  if (str.empty()) {
    return false;
  }
  if (str.find(delimiter) == std::string::npos) {
    return false;
  }
  return true;
}

// 
template <typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
  _mtx.lock();
  Node<K, V> *current = this->_header;
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

  // start from highest level of skip list
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }
  current = current->forward[0];
  if (current != NULL && current->get_key() == key) {
    for (int i = 0; i <= _skip_list_level; i++) {
      if (update[i]->forward[i] != current) break;
      update[i]->forward[i] = current->forward[i];
    }
    while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0) {
      _skip_list_level--;
    }
    std::cout << "Successfully deleted key " << key << std::endl;
    delete current;
    _element_count--;
  }
  _mtx.unlock();
  return;
}


// insert_element是插入新元素，insert_set_element是插入元素，如果元素存在则改变其值
template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &value) {
  V oldValue;
  if (search_element(key, oldValue)) {
    delete_element(key);
  }
  insert_element(key, value);
}

template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V &value) {
  std::cout << "search_element-----------------" << std::endl;
  Node<K, V> *current = _header;

  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
  }
  current = current->forward[0];
  if (current and current->get_key() == key) {
    value = current->get_value();
    std::cout << "Found key: " << key << ", value: " << current->get_value() << std::endl;
    return true;
  }
  std::cout << "Not Found Key:" << key << std::endl;
  return false;
}

// construct skip list
template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level) {
  this->_max_level = max_level;
  this->_skip_list_level = 0;
  this->_element_count = 0;
  K k;
  V v;
  this->_header = new Node<K, V>(k, v, _max_level);
};

template <typename K, typename V>
SkipList<K, V>::~SkipList() {
  if (_file_writer.is_open()) {
    _file_writer.close();
  }
  if (_file_reader.is_open()) {
    _file_reader.close();
  }

  //递归删除跳表链条
  if (_header->forward[0] != nullptr) {
    clear(_header->forward[0]);
  }
  delete (_header);
}

template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V> *cur) {
  if (cur->forward[0] != nullptr) {
    clear(cur->forward[0]);
  }
  delete (cur);
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level() {
  int k = 1;
  while (rand() % 2) {  //随机生成一个0或1，如果是1，则继续生成下一层，直到生成0或者达到最大层数
    k++;
  }
  k = (k < _max_level) ? k : _max_level;
  return k;
};
#endif  // SKIPLIST_H
