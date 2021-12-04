#include <chrono>
#include "path_conf.h"

using namespace cloudkv;

path_t path_conf::next_sst_path() const
{
    using namespace std::chrono;
    return sst_path(steady_clock::now().time_since_epoch().count());
}