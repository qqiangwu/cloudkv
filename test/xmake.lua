add_includedirs("../src/")

add_requires("gtest 1.11.0", { config = {main = true} })

function all_tests()
    local res = {}
    for _, x in ipairs(os.files("**_test.cpp")) do
        local item = {}
        local s = path.filename(x)
        table.insert(item, s:sub(1, #s - 4))       -- target
        table.insert(item, path.relative(x, "."))  -- source
        table.insert(res, item)
    end
    return res
end

for _, test in ipairs(all_tests()) do
target(test[1])
    set_kind("binary")
    add_files(test[2])
    add_packages("gtest")
    add_packages("range-v3")
    add_deps("cloudkv")
end