add_rules("mode.debug", "mode.release")

set_warnings("allextra", "error")
set_languages("c++17")
set_symbols("debug")  -- always use debug symbol
add_cxflags("-Wno-missing-field-initializers")

package("scope_guard")
	add_urls("https://github.com/Neargye/scope_guard.git")
	add_versions("0.9.1", "8444f7096ea4874817d3421dc8ca596e984d6eaf")
	add_deps("cmake")
	on_install(function(package)
        import("package.tools.cmake").install(package)
    end)
package_end()

boost_configs = { context = true, filesystem = true, program_options = true, regex = true, system = true, thread = true, serialization = true }

add_requires("boost 1.76.0", { configs = boost_configs })
add_requires("fmt 8.0.1")
add_requires("spdlog v1.9.1", { configs = { fmt_external = true } })
add_requires("range-v3 0.11.0")
add_requires("scope_guard 0.9.1")
add_requires("folly 2021.08.02")
add_requireconfs("folly.boost", { version = "1.76.0", override = true, configs = boost_configs })

target("cloudkv")
    set_kind("$(kind)")
    add_headerfiles("include/(cloudkv/*.h)")
    add_includedirs("include", { public = true })
    add_includedirs("src/", { public = false })
    add_files("src/**.cpp")
    add_defines("BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION", { public = true })
    add_defines("BOOST_THREAD_PROVIDES_EXECUTORS", { public = true })
    add_defines("BOOST_THREAD_USES_MOVE", { public = true })
    add_packages("boost", { public = true })
    add_packages("fmt")
    add_packages("spdlog")
    add_packages("range-v3")
    add_packages("scope_guard")
    add_packages("folly")

option("with_test")
    set_default(false)
    set_showmenu(true)
    set_description("compile tests or not")
option_end()

if has_config("with_test") then
    includes("test/")
end

includes("util/")
