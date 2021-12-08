function all_utils()
    local res = {}
    for _, x in ipairs(os.files("*.cpp")) do
        local item = {}
        local s = path.filename(x)
        table.insert(item, s:sub(1, #s - 4))       -- target
        table.insert(item, path.relative(x, "."))  -- source
        table.insert(res, item)
    end
    return res
end

add_requires("gflags v2.2.2")

for _, util in ipairs(all_utils()) do
target(util[1])
    set_kind("binary")
    set_default(false)
    add_files(util[2])
    add_packages("boost", { public = true })
    add_packages("range-v3")
    add_packages("fmt")
    add_packages("gflags")
    add_packages("spdlog")
    add_deps("cloudkv")
end