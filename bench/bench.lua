-- Globals
phrase = false       --- should this be a phrase query?
operator = 'and'     --- default operator for terms. "and" or "or".
-- word_count        --- number of words to add to the query
terminate_after = 1000000000 --- maximum number of results to count
fuzziness = 0        --- how much fuzziness is ok? 0, 1, 2, or "AUTO"
slop = 0             --- how much phrase slop is allowed?
prefix = 0           --- should we generate prefix queries?
degenerate = false   --- should the generated queries repeate the same words?
common_freq = 0      --- percentage of words that should come from the list of common words.
-- words             --- the dictionary
total_hits = 0       --- total number of hits for all requests
terminated_early = 0 --- how many requests terminated early

JSON = (loadfile "JSON.lua")()

-- Pattern to match a code point
cp = '[%z\1-\127\194-\244][\128-\191]*'
-- Pattern to optionally match a code point
ocp = '[%z\1-\127\194-\244]?[\128-\191]*'

function init(args)
  local query_type = table.remove(args, 1)
  word_count = tonumber(table.remove(args, 1))
  local random_seed = tonumber(table.remove(args, 1))
  if #args > 0 then
    terminate_after = random_seed
    random_seed = tonumber(table.remove(args, 1))
  end
  if (string.match(query_type, 'degenerate_.+')) then
    query_type = string.gsub(query_type, 'degenerate_', '')
    degenerate = true
  end
  if (string.match(query_type, 'common_%d%d_.+')) then
    common_freq, query_type = string.match(query_type, 'common_(%d%d)_(.+)')
    common_freq = tonumber(common_freq)
  end
  if string.find(query_type, 'phrase') then
    phrase = true
    slop = string.match(query_type, '[^~]+~?(.*)')
    if slop == '' then
      slop = 0
    end
  elseif string.find(query_type, '*') then
    operator, prefix = string.match(query_type, '([^*]+)*(.*)')
    if prefix == '' then
      error('* must be followed by a number')
    else
      prefix = tonumber(prefix)
    end
  else
    operator, fuzziness = string.match(query_type, '([^~]+)~?(.*)')
    if fuzziness == '' then
      fuzziness = 0
    end
  end
  if (thread_number == 1) then
    print('phrase=' .. tostring(phrase))
    print('operator=' .. operator)
    print('word_count=' .. word_count)
    print('fuzziness=' .. fuzziness)
    print('slop=' .. slop)
    print('prefix=' .. tostring(prefix))
    print('degenerate=' .. tostring(degenerate))
    print('common_freq=' .. common_freq)
    print('terminate_after=' .. terminate_after)
    print('random_seed=' .. random_seed)
  end
  math.randomseed(random_seed * thread_number) -- meh
end

function request()
  local query = ""
  local word = nil
  if phrase then
    query = '"'
  end
  for i=1,word_count do
    if word == nil or degenerate == false then
      if common_freq < math.random() then
        word = words[math.random(#words)]
      else
        word = common_words[math.random(#common_words)]
      end
      if prefix > 0 then
        -- grab the first prefix codepoints
        local sub = '(' .. cp
        for cps=2,prefix do
          sub = sub .. ocp
        end
        sub = sub .. ').*'
        word = string.gsub(word, sub, '%1*')
      elseif fuzziness ~= 0 then
        word = word .. '~'
      end
    end
    query = query .. word .. ' '
  end
  query = string.gsub(query, ' $', '')
  if phrase then
    query = query .. '"'
  end
  query = string.gsub(query, '"', '\\"') -- json escape
  local body = [[{
    "size": 0,
    "terminate_after": ]] .. terminate_after .. [[,
    "query": {
      "query_string": {
        "fields": ["text"],
        "query": "]] .. query .. [[",
        "default_operator": "]] .. operator .. [[",
        "fuzziness": "]] .. fuzziness .. [[",
        "phrase_slop": ]] .. slop .. [[
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
  if response['terminated_early'] then
    terminated_early = terminated_early + 1
  end
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
  local terminated_early = 0
  for index, thread in ipairs(threads) do
    -- print("last_request[" .. index .. "]=" .. thread:get('last_request'))
    -- print("last_response[" .. index .. "]=" .. thread:get('last_response'))
    total_hits = total_hits + thread:get('total_hits')
    terminated_early = terminated_early + thread:get('terminated_early')
  end
  print(string.format('Avg Hits:%14.2f', total_hits / summary['requests']))
  print(string.format('Stopped Early:%9.2f', 100 * terminated_early / summary['requests']))
end

function load_dict()
  local words = {}
  wrk.method="POST"
  local dict = io.open("/usr/share/dict/american-english", "r")
  for word in dict:lines() do
    if string.find(word, "'") then
      -- print('dropping ' .. word)
    else
      table.insert(words, word)
    end
  end
  dict:close()
  return words
end

common_words = {'you', 'say', 'that', 'help', 'he', 'low', 'was', 'line',
  'for', 'before', 'on', 'turn', 'are', 'cause', 'with', 'same', 'as', 'mean',
  'I', 'differ', 'his', 'move', 'they', 'right', 'be', 'boy', 'at', 'old',
  'one', 'too', 'have', 'does', 'this', 'tell', 'from', 'sentence', 'or',
  'set', 'had', 'three', 'by', 'want', 'hot', 'air', 'but', 'well', 'some',
  'also', 'what', 'play', 'there', 'small', 'we', 'end', 'can', 'put', 'out',
  'home', 'other', 'read', 'were', 'hand', 'all', 'port', 'your', 'large',
  'when', 'spell', 'up', 'add', 'use', 'even', 'word', 'land', 'how', 'here',
  'said', 'must', 'an', 'big', 'each', 'high', 'she', 'such', 'which',
  'follow', 'do', 'act', 'their', 'why', 'time', 'ask', 'if', 'men', 'will',
  'change', 'way', 'went', 'about', 'light', 'many', 'kind', 'then', 'off',
  'them', 'need', 'would', 'house', 'write', 'picture', 'like', 'try', 'so',
  'us', 'these', 'again', 'her', 'animal', 'long', 'point', 'make', 'mother',
  'thing', 'world', 'see', 'near', 'him', 'build', 'two', 'self', 'has',
  'earth', 'look', 'father', 'more', 'head', 'day', 'stand', 'could', 'own',
  'go', 'page', 'come', 'should', 'did', 'country', 'my', 'found', 'sound',
  'answer', 'no', 'school', 'most', 'grow', 'number', 'study', 'who', 'still',
  'over', 'learn', 'know', 'plant', 'water', 'cover', 'than', 'food', 'call',
  'sun', 'first', 'four', 'people', 'thought', 'may', 'let', 'down', 'keep',
  'side', 'eye', 'been', 'never', 'now', 'last', 'find', 'door', 'any',
  'between', 'new', 'city', 'work', 'tree', 'part', 'cross', 'take', 'since',
  'get', 'hard', 'place', 'start', 'made', 'might', 'live', 'story', 'where',
  'saw', 'after', 'far', 'back', 'sea', 'little', 'draw', 'only', 'left',
  'round', 'late', 'man', 'run', 'year', 'came', 'while', 'show',
  'press', 'every', 'close', 'good', 'night', 'me', 'real', 'give', 'life',
  'our', 'few', 'under', 'stop'}
