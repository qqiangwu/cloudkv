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

void create_file(const fs::path& p)
{
    std::ofstream ofs(p);
}