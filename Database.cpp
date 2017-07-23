//
// Created by apple on 2017/7/21.
//

#include "Database.h"

void Database::open(const std::string &index_file_name, const std::string &data_file_name)
{

}

void Database::close(void)
{

}

void Database::insert(const std::string &key, const std::string &data)
{

}

void Database::remove(const std::string &key)
{

}

const std::string Database::fetch(const std::string &key)
{
    IndexPointer index_pointer;
    off_t hash_offset = (off_t)(hash(key) * sizeof(IndexPointer));
    
    // Get the index chain pointer.
    _index_file.seekg(hash_offset, std::ios::beg);
    _index_file.read((char *)&index_pointer, sizeof(IndexPointer));
    
    std::string data;
    bool existent = false;
    
    for (int i = 0; i < index_pointer.count(); i++)
    {
    
    }
    
    return std::move(data);
}

void Database::replace(const std::string &key, const std::string &data)
{

}
