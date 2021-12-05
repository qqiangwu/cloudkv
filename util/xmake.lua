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

for _, util in ipairs(all_utils()) do
target(util[1])
    set_kind("binary")
    add_files(util[2])
    add_packages("boost", { public = true })
    add_packages("range-v3")
    add_packages("fmt")
    add_deps("cloudkv")
end