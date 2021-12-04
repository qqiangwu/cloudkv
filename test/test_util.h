#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

struct DBCleaner
{
    const fs::path path_;

    DBCleaner(const fs::path& p)
        : path_(p)
    {
        fs::remove_all(path_);
    }

    ~DBCleaner()
    {
        fs::remove_all(path_);
    }
};

void create_file(const fs::path& p, const std::string& data = {})
{
    std::ofstream ofs(p);

    ofs.write(data.data(), data.size());
    EXPECT_TRUE(ofs);
}