add_includedirs("../src/")

add_requires("gtest 1.11.0", { config = {main = true} })

target("memtable")
    set_kind("binary")
    add_files("memtable_test.cpp")
    add_packages("gtest")
    add_deps("cloudkv")