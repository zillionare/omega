#!lua name=omega

local function round2(num)
    return math.floor(num * 100 + 0.5) / 100
end

local function newsplit(delimiter, str)
    assert(type(delimiter) == "string")
    assert(#delimiter > 0, "Must provide non empty delimiter")

    -- Add escape characters if delimiter requires it
    -- delimiter = delimiter:gsub("[%(%)%.%%%+%-%*%?%[%]%^%$]", "%%%0")

    local start_index = 1
    local result = {}

    while true do
       local delimiter_index, _ = str:find(delimiter, start_index)

       if delimiter_index == nil then
          table.insert(result, str:sub(start_index))
          break
       end

       table.insert(result, str:sub(start_index, delimiter_index - 1))

       start_index = delimiter_index + 1
    end

    return result
end

local function close_frame(keys_, args)
--local function close_frame(frame_type, frame)
    -- close the frame, write unclosed_5m hash to bars:5m:{code} hash.
    local frame_type, frame = unpack(args)
    local hm = redis.call('hgetall', 'bars:' .. frame_type .. ':unclosed')

    for i = 1, #hm, 2 do
        local code = hm[i]
        local bar = hm[i + 1]
        redis.call('hset', 'bars:' .. frame_type .. ':' .. code, frame, bar)
    end

    redis.call('del', 'bars:' .. frame_type .. ':unclosed')
end

local function decode_bar(bars)
    -- 将string表示的bar解码成为正确类型的OHLC,但对frame仍保持为字符串
    local frame, open, high, low, close, volume, amount, factor = unpack(newsplit(',', bars))

    return frame, round2(tonumber(open)), round2(tonumber(high)), round2(tonumber(low)), round2(tonumber(close)), tonumber(volume), tonumber(amount), tonumber(factor)
end

local function update_unclosed(keys_, args)
--local function update_unclosed(frame_type, min_frame)
    -- merge bars:{frame_type.value}:unclosed with bars:1m:{code} hash.
    -- args are: frame_type(str), min_frame(int, minute frame)

    local frame_type, min_frame = unpack(args)
    local unclosed_key = 'bars:' .. frame_type .. ':unclosed'

    -- bars:1m:* should contains NO bars:1m:unclosed
    local keys = redis.call('keys', 'bars:1m:*')

    for _, key_ in ipairs(keys) do
        local code = key_:match('bars:1m:(.*)')

        -- get 1m bar to merge from
        local mbar = redis.call('hget', key_, min_frame)
        if mbar then
            local t2, o2, h2, l2, c2, v2, a2, f2 = decode_bar(mbar)

            -- get unclosed bar and do the merge
            local unclosed = redis.call('hget', unclosed_key, code)

            local t, opn, high, low, close, volume, amount, factor = '', o2, h2, l2, c2, v2, a2, f2
            if unclosed then
                local _, o1, h1, l1, c1, v1, a1, f1 = decode_bar(unclosed)
                opn = o1
                high = math.max(h1, h2)
                low = math.min(l1, l2)
                close = c2
                volume = v1 + v2
                amount = a1 + a2
                factor = f2
            end

            -- save unclosed bar
            local bar = min_frame .. ',' .. opn .. ',' .. high .. ',' .. low .. ',' .. close .. ',' .. volume .. ',' .. amount .. ',' .. factor
            redis.call('hset', unclosed_key, code, bar)
        end
    end
end

-- update_unclosed('5m', 202207180935, 202207180931)
-- update_unclosed('5m', 202207180935, 202207180932)
-- update_unclosed('5m', 202207180935, 202207180933)
-- update_unclosed('5m', 202207180935, 202207180935)
-- update_unclosed('5m', 202207180935, 202207180936)


-- close_frame('5m', "2022020935")

redis.register_function(
    'close_frame',
    close_frame
)
redis.register_function(
  'update_unclosed',
   update_unclosed
)
