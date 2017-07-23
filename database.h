//
// Created by apple on 2017/7/21.
//

#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <fstream>

using Offset   = int64_t;
using Size     = int64_t;
using Flag     = int32_t;

struct HashItem
{
    Offset first;       // First index node with the associated hash value.
    Size count;         // Number of indices sharing the same hash value.
};

struct IndexInfo
{
    Offset next;
    Size key_len;
};

// Index Item Arrangement
// || Next Index offset: Offset | Key Length: Size || Data Offset: Offset || key: char[] ||
//                   sizeof(IndexInfo)                   sizeof(Offset)       Key Length

struct DataInfo
{
    Size data_len;
    Flag deleted;
};

// Data Item Arrangement
// || Data Length: Size | Data Flag: Flag || Data: char[] ||
//               sizeof(DataInfo)            Data Length

constexpr Size hash_table_size = 10008;
constexpr const char *index_file_suffix = ".idx";
constexpr const char *data_file_suffix = ".dat";

class Database
{
private:
    std::fstream _index_file;
    std::fstream _data_file;
    
    Offset _hash(const std::string &key);
    
    const HashItem _read_hash_item(Offset hash_offset);
    const IndexInfo _read_index_info(Offset index_offset);
    const std::string _read_key(Offset index_offset, Size key_len);
    Offset _read_data_offset(Offset index_offset);
    const DataInfo _read_data_info(Offset data_offset);
    const std::string _read_data(Offset data_offset, Size data_len);
    Offset _get_index_offset(const HashItem &hash_item, const std::string &key);
    
    void _write_hash_item(Offset hash_offset, const HashItem &hash_item);
    void _write_index_info(Offset index_offset, const IndexInfo &index_info);
    void _write_key(Offset index_offset, const std::string &key);
    void _write_data_offset(Offset index_offset, Offset data_offset);
    void _write_data_info(Offset data_offset, const DataInfo &data_info);
    void _write_data(Offset data_offset, const std::string &data);

public:
    Database(const std::string &db_name = "");
    ~Database(void);
    void open(const std::string &db_name);
    void close(void);
    void insert(const std::string &key, const std::string &data);
    void replace(const std::string &key, const std::string &data);
    void remove(const std::string &key);
    const std::string fetch(const std::string &key);
    void rewind(void);
    const std::string next_record(void);
    
    static void create(const std::string &db_name);
};

#endif  //DATABASE_H
