-- Globals
-- query_type      --- type of query. Only "match", "match_phrase", etc are likely to work
-- operator        --- default operator for terms. "and" or "or"
-- word_count      --- number of words to add to the query
fuzziness = 0      --- how much fuziness is ok? 0, 1, 2, or "AUTO"
slop = 0           --- how much phrase slop is allowed?
degenerate = false --- should the generated queries repeate the same words?
-- words           --- the dictionary
total_hits = 0     --- total number of hits for all requests

JSON = (loadfile "JSON.lua")()

function init(args)
  query_type = table.remove(args, 1)
  operator = table.remove(args, 1)
  word_count = tonumber(table.remove(args, 1))
  local random_seed = tonumber(table.remove(args, 1))
  if (string.match(query_type, 'degenerate_.+')) then
    query_type = string.gsub(query_type, 'degenerate_', '')
    degenerate = true
  end
  if (string.find(query_type, 'match_phrase')) then
    query_type, slop = string.match(query_type, '([^~]+)~?(.*)')
    if slop == '' then
      slop = 0
    end
  else
    query_type, fuzziness = string.match(query_type, '([^~]+)~?(.*)')
    if fuzziness == '' then
      fuzziness = 0
    end
  end
  if (thread_number == 1) then
    print('query_type=' .. query_type)
    print('operator=' .. operator)
    print('word_count=' .. word_count)
    print('fuzziness=' .. fuzziness)
    print('slop=' .. slop)
    print('degenerate=' .. tostring(degenerate))
    print('random_seed=' .. random_seed)
  end
  math.randomseed(random_seed * thread_number) -- meh
end

function request()
  local text = ""
  local word = nil
  for i=1,word_count do
    if word == nil or degenerate == false then
      word = words[math.random(#words)]
    end
    text = text .. word .. " "
  end
  local body = [[{
    "size": 0,
    "terminate_after": 1000,
    "query": {
      "]] .. query_type .. [[": {
        "text": {
          "query": "]] .. text .. [[",
          "operator": "]] .. operator .. [[",
          "fuzziness": "]] .. fuzziness .. [[",
          "slop": ]] .. slop .. [[
        }
      }
    }
  }]]
  last_request = body
  return wrk.format("POST", nil, nil, body)
end

function response(status, headers, body)
  last_response = body
  if (status ~= 200) then
    print("last_request[" .. thread_number .. "]=" .. last_request)
    print("last_response[" .. thread_number .. "]=" .. last_response)
    error('Request failed ' .. status)
  end
  local response = JSON:decode(body)
  total_hits = total_hits + response['hits']['total']
end

-- Everything below this is done during setup and teardown
local threads = {}
function setup(thread)
  table.insert(threads, thread)
  thread:set('words', load_dict())
  thread:set('thread_number', #threads)
end

function done(summary, latency, requests)
  local total_hits = 0
  for index, thread in ipairs(threads) do
    -- print("last_request[" .. index .. "]=" .. thread:get('last_request'))
    -- print("last_response[" .. index .. "]=" .. thread:get('last_response'))
    total_hits = total_hits + thread:get('total_hits')
  end
  print(string.format('Avg Hits:%14.2f', total_hits / summary['requests']))
end

function load_dict()
  local words = {}
  wrk.method="POST"
  local dict = io.open("/usr/share/dict/american-english", "r")
  for line in dict:lines() do
    table.insert(words, line)
  end
  dict:close()
  return words
end
