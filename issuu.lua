dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_name = os.getenv("item_name")

local item_type, item_value = string.match(item_name, "^([^:]+):(.+)$")
local item_user = nil
local item_issue = nil
local item_id = nil
if item_type == "issue" then
  item_user, item_id, item_issue = string.match(item_value, "^([^:]+):([^:]+):(.+)$")
elseif item_type == "user" then
  item_user = item_value
end

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('queuing' , item)
    target[item] = true
  end
end

allowed = function(url, parenturl)
  return true
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"]) and not processed(url) then
    addedtolist[url] = true
    return true
  end]]

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function extract_all_pub(json)
    for k, v in pairs(json) do
      if type(v) == "table" then
        extract_all_pub(v)
      elseif type(v) == "string"
        and string.match(v, "%.isu%.pub/") then
        local newurl = urlparse.absolute(url, "//" .. v)
        check(newurl)
        local page_num = string.match(newurl, "page_([0-9]+)%.jpg$")
        if page_num then
          check(string.gsub(newurl, "(%.jpg)$", "_thumb_large%1"))
          check(string.gsub(newurl, "(%.jpg)$", "_thumb_medium%1"))
          check(string.gsub(newurl, "(%.jpg)$", "_thumb_small%1"))
        end
      end
    end
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "%.bin$")
    and not string.match(url, "%.jpg$") then
    html = read_file(file)
    if item_type == "issue"
      and string.match(url, "^https?://[^/]*issuu%.com/[^/]+/docs/") then
      check("https://issuu.com/" .. item_user .. "/docs/" .. item_issue)
      check("https://issuu.com/rdr?p=1&r=https%3A%2F%2Fissuu.com%2F" .. item_user .. "&d=" .. item_issue .. "&u=" .. item_issue)
      check("https://issuu.com/call/document-page/stream/more-from-publisher/" .. item_user .. "/" .. item_issue .. "?experiment=see-more-ads,iab-category-new")
      check("https://reader3.isu.pub/" .. item_user .. "/" .. item_issue .. "/reader3_4.json")
      check("https://api.issuu.com/call/backend-reader3/dynamic/" .. item_user .. "/" .. item_issue)
      check("https://api.issuu.com/call/backend-reader3/links/" .. item_user .. "/" .. item_issue)
      check("https://issuu.com/call/document-page/stream/more-from-others/" .. item_user .. "/" .. item_issue)
      check("https://issuu.com/call/profile/v1/article-stories?username=" .. item_user .. "&docname=" .. item_issue)
    end
    if item_type == "user"
      and string.match(url, "^https?://[^/]*issuu%.com/[^/%?&]+$") then
      check("https://issuu.com/" .. item_user)
      check("https://photo.isu.pub/" .. item_user .. "/photo_large.jpg")
      check("https://photo.isu.pub/" .. item_user .. "/photo_small.jpg")
      check("https://issuu.com/call/profile/v1/social?username=" .. item_user)
      check("https://issuu.com/call/profile/v1/documents/" .. item_user .. "?offset=0&limit=25")
      check("https://issuu.com/" .. item_user .. "/stacks")
      check("https://issuu.com/query?format=json&stackUsername=" .. item_user .. "&pageSize=20&access=public&sortBy=title&resultOrder=asc&startIndex=0&action=issuu.stacks.list_anonymous")
      check("https://issuu.com/" .. item_user .. "/followers")
      check("https://issuu.com/query?format=json&subscribedUsername=" .. item_user .. "&pageSize=12&sortBy=subscriberCount&resultOrder=desc&startIndex=0&action=issuu.user.list_subscribers")
    end
    if string.match(url, "/call/profile/v1/documents/") then
      local count = 0
      local json = JSON:decode(html)
      for _ in pairs(json["items"]) do
        count = count + 1
      end
      if count > 0 then
        local offset = tonumber(string.match(url, "offset=([0-9]+)"))
        local limit = tonumber(string.match(url, "limit=([0-9]+)"))
        local newurl = string.gsub(url, "(offset=)[0-9]+", "%1" .. tostring(offset+limit))
        check(newurl)
      end
    end
    if string.match(url, "^https?://[^/]*issuu%.com/query%?") then
      local json = JSON:decode(html)
      if json["rsp"]["stat"] ~= "ok" then
        error("stat was not ok in returned data.")
      end
      local count = 0
      for _ in pairs(json["rsp"]["_content"]["result"]["_content"]) do
        count = count + 1
      end
      if count > 0 then
        local page_size = string.match(url, "pageSize=([0-9]+)")
        local start_index = string.match(url, "startIndex=([0-9]+)")
        local newurl = string.gsub(url, "(startIndex=)[0-9]+", "%1" .. tostring(start_index+page_size))
        check(newurl)
      end
    end
    if string.match(url, "/reader3_4%.json$") then
      local temp_file = file .. ".uncompressed"
      os.execute("gzip -dc " .. file .. " > " .. temp_file)
      local json = JSON:decode(read_file(temp_file))
      if json["error"] then
        error("Found error in reader3_4.json data.")
      end
      extract_all_pub(json)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code == 200 then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 403 and string.match(url["url"], "smartzoom%.bin$") then
    return wget.actions.NOTHING
  end

  if status_code >= 400 or status_code == 0 then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 3
    tries = tries + 1
    if tries > maxtries then
      tries = 0
      abort_item()
      return wget.actions.ABORT
    end
    os.execute("sleep " .. math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    ))
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

