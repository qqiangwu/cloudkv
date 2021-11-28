add_rules("mode.debug", "mode.release")

set_warnings("all", "error")
set_languages("c++17")
set_optimize("faster")

add_requires("boost 1.76.0", { configs = { thread = true } })
add_requires("fmt 8.0.1")
add_requires("spdlog v1.9.1", { config = { fmt_external = true } })
add_requires("abseil 20210324.2")

target("cloudkv")
    set_kind("$(kind)")
    add_includedirs("include", { public = true })
    add_includedirs("src/", { public = false })
    add_files("src/*.cpp")
    add_packages("boost", { public = true })
    add_packages("fmt")
    add_packages("spdlog")
    add_packages("abseil")

includes("test/")
