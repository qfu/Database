//
// Created by apple on 2017/7/21.
//

#ifndef KUNSHEN_DATABASE_H
#define KUNSHEN_DATABASE_H

#include <string>
#include <fstream>

class IndexPointer
{
private:
    off_t _offset;
    int _count;

public:
    IndexPointer(off_t offset = 0, int count = 0) : _offset(offset), _count(count) {}
    ~IndexPointer(void) {}
    off_t offset(void) const { return _offset; }
    void set_offset(off_t offset) { _offset = offset; }
    int count(void) const { return _count; }
    void set_count(int count) { _count = count; }
};

inline size_t hash(const std::string &key) const
{
    unsigned long prod = 1;
    constexpr unsigned long mask = 10007;
    for (auto ch : key)
    {
        prod *= (ch + 1);
    }
    return (prod % mask);
}

class HashTable
{
private:
    size_t _size;
    IndexPointer *_table;

public:
    HashTable(size_t size) : _size(size), _table(new IndexPointer[size]) {}
    ~HashTable(void) { delete _table; }
    off_t index_offset(const std::string &key)
    {
        size_t hash_val = hash(key);
        return (hash_val < _size && _table[hash_val].count() > 0) ? _table[hash_val].offset() : 0;
    }
};

struct IndexInfo
{
    off_t _next;
    size_t _key_len;
    off_t _data_offset;
    size_t _data_len;
};

class IndexNode
{
private:
    IndexInfo _info;
    std::string _key;

public:
    IndexNode(void) {}
    IndexNode(const IndexInfo &info, const std::string &key)
            : _info(info)
    _key(key) {}
    off_t next(void) const { return _next; }
    size_t key_len(void) const { return _key_len; }
    off_t data_offset(void) const { return _data_offset; }
    size_t data_len(void) const { return _data_len; }
    const std::string &key(void) const { return _key; }
};

class Database
{
private:
    std::fstream _index_file;
    std::fstream _data_file;

public:
    void open(const std::string &index_file_name, const std::string &data_file_name);
    void close(void);
    void insert(const std::string &key, const std::string &data);
    void replace(const std::string &key, const std::string &data);
    void remove(const std::string &key);
    const std::string fetch(const std::string &key);
};

#endif //KUNSHEN_DATABASE_H
