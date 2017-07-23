//
// Created by apple on 2017/7/21.
//

#include "database.h"

Database::Database(const std::string &db_name)
{
    if (db_name != "") { open(db_name); }
}

Database::~Database(void)
{
    close();
}

inline Offset Database::_hash(const std::string &key)
{
    long hash_val = 1;
    constexpr unsigned long mask = hash_table_size - 1;
    
    for (auto ch : key)
    {
        hash_val *= ch;
        hash_val %= mask;
    }
    
    return hash_val + 1;    // Ensure that the hash value is in [1, hash_table_size), for 0 is preserved for other use.
}

void Database::open(const std::string &db_name)
{
    close();
    _index_file.open(db_name + index_file_suffix, std::ios::binary | std::ios::in | std::ios::out);
    _data_file.open(db_name + data_file_suffix, std::ios::binary | std::ios::in | std::ios::out);
}

void Database::close(void)
{
    _index_file.close();
    _data_file.close();
}

void Database::insert(const std::string &key, const std::string &data)
{
    if (!_index_file.is_open() || !_data_file.is_open()) { return; }
    
    Offset hash_offset = _hash(key) * sizeof(HashItem);
    HashItem hash_item = _read_hash_item(hash_offset);
    
    if (_get_index_offset(hash_item, key) != -1) { return; }
    
    _data_file.seekp(0, std::ios::end);
    
    HashItem hash_head = _read_hash_item(0);
    IndexInfo index_info = { .next = hash_item.first, .key_len = (Size)(key.size() + 1) };
    Offset index_offset = hash_head.first;
    Offset data_offset = _data_file.tellp();
    DataInfo data_info = { .data_len = (Size)(data.length() + 1), .deleted = 0 };
    
    _write_index_info(index_offset, index_info);
    _write_data_offset(index_offset, data_offset);
    _write_key(index_offset, key);
    _write_data_info(data_offset, data_info);
    _write_data(data_offset, data);
    
    hash_head.first = _index_file.tellp();
    _write_hash_item(0, hash_head);
    
    hash_item.count++;
    hash_item.first = index_offset;
    _write_hash_item(hash_offset, hash_item);
}

void Database::remove(const std::string &key)
{
    if (!_index_file.is_open() || !_data_file.is_open()) { return; }
    
    Offset hash_offset = _hash(key) * sizeof(HashItem);
    HashItem hash_item = _read_hash_item(hash_offset);
    if (hash_item.count == 0) { return; }
    
    IndexInfo index_info;
    Offset prev_index_offset = -1;
    Offset curr_index_offset = hash_item.first;
    
    bool found = false;
    
    for (int i = 0; i < hash_item.count; i++)
    {
        index_info = _read_index_info(curr_index_offset);
        std::string index_key = _read_key(curr_index_offset, index_info.key_len);
        
        if (index_key == key)
        {
            found = true;
            break;
        }
        
        prev_index_offset = curr_index_offset;
        curr_index_offset = index_info.next;
    }
    
    if (found)
    {
        Offset data_offset = _read_data_offset(curr_index_offset);
        DataInfo data_info = _read_data_info(data_offset);
        
        data_info.deleted = 1;
        _write_data_info(data_offset, data_info);
        
        if (prev_index_offset == -1)
        {
            hash_item.first = index_info.next;
        }
        else
        {
            IndexInfo prev_index_info = _read_index_info(prev_index_offset);
            
            prev_index_info.next = index_info.next;
            _write_index_info(prev_index_offset, prev_index_info);
        }
        
        hash_item.count--;
        _write_hash_item(hash_offset, hash_item);
    }
}

const std::string Database::fetch(const std::string &key)
{
    // Return directly if the database is not opened.
    if (!_index_file.is_open() || !_data_file.is_open()) { return std::string(); }
    
    Offset hash_offset = _hash(key) * sizeof(HashItem);
    HashItem hash_item = _read_hash_item(hash_offset);
    Offset index_offset = _get_index_offset(hash_item, key);
    
    // Return if no matched key.
    if (index_offset == -1) { return std::string(); }
    
    Offset data_offset = _read_data_offset(index_offset);
    DataInfo data_info = _read_data_info(data_offset);
    std::string data = _read_data(data_offset, data_info.data_len);
    
    return std::move(data);
}

void Database::replace(const std::string &key, const std::string &data)
{
    remove(key);
    insert(key, data);
}

void Database::create(const std::string &db_name)
{
    std::ifstream index_existence_tester;
    std::ifstream data_existence_tester;
    
    index_existence_tester.open(db_name + index_file_suffix, std::ios::in);
    data_existence_tester.open(db_name + data_file_suffix, std::ios::in);
    
    // Return if the database already exists.
    if (index_existence_tester.is_open() || data_existence_tester.is_open()) { return; }
    
    // Create the database.
    std::ofstream index_file(db_name + index_file_suffix, std::ios::binary);
    std::ofstream data_file(db_name + data_file_suffix, std::ios::binary);
    
    HashItem item;
    
    item.count = 0;
    item.first = hash_table_size * sizeof(HashItem);
    
    // Create hash table.
    for (int i = 0; i < hash_table_size; i++)
    {
        index_file.write((char *)&item, sizeof(HashItem));
    }
}

void Database::rewind(void)
{
    if (_data_file.is_open()) { _data_file.seekg(0, std::ios::beg); }
}

const std::string Database::next_record(void)
{
    std::string data;
    DataInfo data_info;
    
    if (!_data_file.is_open()) { return std::move(data); }
    
    while (!_data_file.eof())
    {
        _data_file.read((char *)&data_info, sizeof(DataInfo));
        
        if (!data_info.deleted)
        {
            char buffer[data_info.data_len];
            _data_file.read(buffer, data_info.data_len);
            data = buffer;
            break;
        }
        _data_file.seekg(data_info.data_len, std::ios::cur);
    }
    
    return std::move(data);
}

// Assumed that the index file is already open.
inline const HashItem Database::_read_hash_item(Offset hash_offset)
{
    Offset hash_index;      // Offset of the chain pointer in the hash table.
    HashItem hash_item;     // Pointer to the first index in the index chain.
    
    // Get the index chain pointer.
    _index_file.seekg(hash_offset, std::ios::beg);
    _index_file.read((char *)&hash_item, sizeof(HashItem));
    
    return std::move(hash_item);
}

// Assumed that the index file is already open.
inline const IndexInfo Database::_read_index_info(Offset index_offset)
{
    IndexInfo index_info;
    
    _index_file.seekg(index_offset, std::ios::beg);
    _index_file.read((char *)&index_info, sizeof(IndexInfo));
    
    return std::move(index_info);
}

// Assumed that the index file and data file is already open.
inline Offset Database::_read_data_offset(Offset index_offset)
{
    Offset data_offset;
    
    _index_file.seekg(index_offset + sizeof(IndexInfo), std::ios::beg);
    _index_file.read((char *)&data_offset, sizeof(Offset));
    
    return data_offset;
}

// Assumed that the data file is already open.
inline const DataInfo Database::_read_data_info(Offset data_offset)
{
    DataInfo data_info;
    
    _data_file.seekg(data_offset, std::ios::beg);
    _data_file.read((char *)&data_info, sizeof(DataInfo));
    
    return std::move(data_info);
}

// Assumed that the index file is already open.
inline const std::string Database::_read_key(Offset index_offset, Size key_len)
{
    char buffer[key_len];
    
    _index_file.seekg(index_offset + sizeof(IndexInfo) + sizeof(Offset), std::ios::beg);
    _index_file.read(buffer, key_len);
    
    return std::string(buffer);
}

// Assumed that the data file is already open.
inline const std::string Database::_read_data(Offset data_offset, Size data_len)
{
    char buffer[data_len];
    
    _data_file.seekg(data_offset + sizeof(DataInfo), std::ios::beg);
    _data_file.read(buffer, data_len);
    
    return std::string(buffer);
}

// Assumed that the index file is already open.
inline Offset Database::_get_index_offset(const HashItem &hash_item, const std::string &key)
{
    // Return if the index does not exist.
    if (hash_item.count == 0) { return -1; }
    
    Offset curr_offset;     // Current index node offset.
    
    curr_offset = hash_item.first;
    
    // Find the index record.
    for (int i = 0; i < hash_item.count; i++)
    {
        // Read the index node info and stored key.
        IndexInfo index_info = _read_index_info(curr_offset);
        std::string curr_key = _read_key(curr_offset, index_info.key_len);
        
        // Return if the index is found.
        if (key == curr_key) { return curr_offset; }
        
        // Move to next index.
        curr_offset = index_info.next;
    }
    
    return -1;
}

inline void Database::_write_hash_item(Offset hash_offset, const HashItem &hash_item)
{
    _index_file.seekp(hash_offset, std::ios::beg);
    _index_file.write((char *)&hash_item, sizeof(HashItem));
}

inline void Database::_write_index_info(Offset index_offset, const IndexInfo &index_info)
{
    _index_file.seekp(index_offset, std::ios::beg);
    _index_file.write((char *)&index_info, sizeof(IndexInfo));
}

inline void Database::_write_key(Offset index_offset, const std::string &key)
{
    _index_file.seekp(index_offset + sizeof(IndexInfo) + sizeof(Offset), std::ios::beg);
    _index_file.write(key.c_str(), key.size() + 1);
}

inline void Database::_write_data_info(Offset data_offset, const DataInfo &data_info)
{
    _data_file.seekp(data_offset, std::ios::beg);
    _data_file.write((char *)&data_info, sizeof(DataInfo));
}

inline void Database::_write_data(Offset data_offset, const std::string &data)
{
    _data_file.seekp(data_offset + sizeof(DataInfo), std::ios::beg);
    _data_file.write(data.c_str(), data.size() + 1);
}

inline void Database::_write_data_offset(Offset index_offset, Offset data_offset)
{
    _index_file.seekp(index_offset + sizeof(IndexInfo), std::ios::beg);
    _index_file.write((char *)&data_offset, sizeof(Offset));
}
