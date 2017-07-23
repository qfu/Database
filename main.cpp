#include <iostream>
#include <random>
#include <chrono>
#include "database.h"

// Generate a random number in [0, upper).
inline int rand_num(int upper)
{
    return int(random() / (double)RAND_MAX * upper);
}

int main(void)
{
    Database::create("test_db");
    
    Database db("test_db");
    constexpr int num_records = 500;
    
    std::chrono::high_resolution_clock clock;
    auto t0 = clock.now().time_since_epoch();
    
    // Write.
    for (int i = 0; i < num_records; i++)
    {
        db.insert(std::string("key_") + std::to_string(i), std::string("test record ") + std::to_string(i));
    }
    
    // Fetch.
    for (int i = 0; i < num_records; i++)
    {
        db.fetch(std::string("key_") + std::to_string(i));
    }
    
    for (int i = 0; i < num_records * 5; i++)
    {
        // Random access.
        db.fetch(std::string("key_") + std::to_string(rand_num(num_records)));
        
        // Random deletion.
        if (i % 37 == 0) { db.remove(std::string("key_") + std::to_string(rand_num(num_records))); }
        
        // Random insertion and access.
        if (i % 11 == 0)
        {
            db.insert(std::string("ins_") + std::to_string(rand_num(num_records)), "insertion...");
            db.fetch(std::string("ins_") + std::to_string(rand_num(num_records)));
        }
        
        // Random replacement.
        if (i % 17 == 0) { db.replace(std::string("key_") + std::to_string(rand_num(num_records)), "new records"); }
    }
    
    // Remove all and random access.
    for (int i = 0; i < num_records; i += 11)
    {
        db.remove(std::string("ins_") + std::to_string(i));
        for (int j = 0; j < 10; j++) { db.fetch(std::string("key_") + std::to_string(rand_num(i))); }
    }
    for (int i = 0; i < num_records; i++)
    {
        db.remove(std::string("key_") + std::to_string(i));
        for (int j = 0; j < 10; j++) { db.fetch(std::string("key_") + std::to_string(rand_num(i))); }
    }
    
    auto t1 = clock.now().time_since_epoch();
    
    std::cout << "Time elapsed: " << std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000.0f << "ms" << std::endl;
    
    return 0;
}