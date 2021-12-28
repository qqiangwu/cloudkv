#pragma once

#include <vector>
#include <string>

namespace cloudkv {

class write_batch_accessor;

class write_batch {
public:
    write_batch();

    void add(std::string_view key, std::string_view value);

    void remove(std::string_view key);

    int size() const;

    struct handler {
        virtual ~handler() = default;

        virtual void add(std::string_view key, std::string_view value) = 0;

        virtual void remove(std::string_view key) = 0;
    };

    void iterate(handler* h) const;

private:
    friend write_batch_accessor;

    std::string buf_;
};

}