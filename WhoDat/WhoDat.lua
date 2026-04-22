local ADDON_TAG = "|cff33ff99WhoDat|r"

local WhoDat = CreateFrame("Frame")
WhoDat.lookupTimeoutSeconds = 8
WhoDat.friendDataGraceSeconds = 2
WhoDat.systemMessageSuppressSeconds = 5
WhoDat.addFriendFailureFallbackSeconds = 3.5
WhoDat.channelScanIntervalSeconds = 30
WhoDat.channelCacheMaxAgeSeconds = 900
WhoDat.channelMaxProbeMembers = 300
WhoDat.channelNilBreakThreshold = 15
WhoDat.chatPresenceMaxAgeSeconds = 300
WhoDat.peerChannelName = "whodatdata"
WhoDat.peerChannelMinLevel = 8
WhoDat.peerWhisperMinLevel = 9
WhoDat.peerChannelJoinRetrySeconds = 10
WhoDat.peerChannelHelloCooldownSeconds = 30
WhoDat.peerStartupHelloWindowSeconds = 45
WhoDat.peerStartupHelloRetrySeconds = 3
WhoDat.peerRosterProbeCooldownSeconds = 30
WhoDat.peerRosterProbeMaxTargetsPerRun = 12
WhoDat.peerMemberMaxAgeSeconds = 180
WhoDat.peerLookupCooldownSeconds = 20
WhoDat.peerLookupResponseWaitSeconds = 5
WhoDat.peerFactionMaxPerFaction = 32
WhoDat.addonCommPrefix = "WHODATX1"
WhoDat.guildRosterRefreshIntervalSeconds = 10
WhoDat.debugEnabled = false
WhoDat.debugLogMaxEvents = 50
WhoDat.persistDebugToSavedVariables = true
WhoDat.persistentDebugLogMaxEvents = 500
WhoDat.lastChannelScanAt = 0
WhoDat.lastGuildRosterRefreshAt = 0
WhoDat.lastPeerChannelJoinAttemptAt = 0
WhoDat.lastPeerChannelHelloAt = 0
WhoDat.lastPeerChannelHelloEchoAt = 0
WhoDat.lastPeerChannelHelloAttemptAt = 0
WhoDat.peerStartupHelloDeadlineAt = 0
WhoDat.peerStartupHelloDone = false
WhoDat.lastPeerStartupHelloTryAt = 0
WhoDat.lastPeerRosterProbeAt = 0
WhoDat.peerRequestSequence = 0
WhoDat.pending = {}
WhoDat.channelCache = {}
WhoDat.recentChatPresence = {}
WhoDat.peerChannelMembers = {}
WhoDat.peerFactionCache = {}
WhoDat.peerAddonConfirmed = {}
WhoDat.lastPeerLookupAt = {}
WhoDat.methodStats = {}
WhoDat.methodStatsStartedAt = 0
WhoDat.debugEvents = {}
WhoDat.persistentDebugEvents = {}
WhoDat.debugSummary = {}
WhoDat.lookupSequence = 0
WhoDat.scanInterval = 0.2
WhoDat.scanElapsed = 0
WhoDat.suppressedFriendSystemMessages = {
    add = {},
    remove = {},
}

local CLASS_TOKEN_BY_LOCALIZED = {}

for token, localized in pairs(LOCALIZED_CLASS_NAMES_MALE or {}) do
    CLASS_TOKEN_BY_LOCALIZED[localized] = token
end

for token, localized in pairs(LOCALIZED_CLASS_NAMES_FEMALE or {}) do
    CLASS_TOKEN_BY_LOCALIZED[localized] = token
end

local function ChatPrint(message)
    DEFAULT_CHAT_FRAME:AddMessage(ADDON_TAG .. " " .. message)
end

local function BuildSystemMessagePattern(template)
    if type(template) ~= "string" or template == "" then
        return nil
    end

    local token = "\001"
    local pattern = string.gsub(template, "%%s", token)
    pattern = string.gsub(pattern, "([%(%)%.%%%+%-%*%?%[%]%^%$])", "%%%1")
    pattern = string.gsub(pattern, token, "(.+)")

    return "^" .. pattern .. "$"
end

local FRIEND_ADDED_PATTERN = BuildSystemMessagePattern(ERR_FRIEND_ADDED_S)
local FRIEND_REMOVED_PATTERN = BuildSystemMessagePattern(ERR_FRIEND_REMOVED_S)
local ChatFrameAddMessageEventFilter = (ChatFrameUtil and ChatFrameUtil.AddMessageEventFilter) or ChatFrame_AddMessageEventFilter
local HandleFriendLookupFailureMessage
local RequestPeerCrossFactionLookup
local CompleteLookupFromSummary
local SyncWhodatPeerMembership

local FRIEND_ADD_FAILURE_PATTERNS = {}

local function AddFriendFailurePattern(reason, template)
    if type(template) ~= "string" or template == "" then
        return
    end

    local pattern = BuildSystemMessagePattern(template)
    if not pattern then
        return
    end

    table.insert(FRIEND_ADD_FAILURE_PATTERNS, {
        reason = reason,
        pattern = pattern,
        capturesName = string.find(template, "%%s", 1, true) ~= nil,
    })
end

AddFriendFailurePattern("wrong_faction", ERR_FRIEND_WRONG_FACTION)
AddFriendFailurePattern("wrong_faction", ERR_FRIEND_ENEMY_S)
AddFriendFailurePattern("not_found", ERR_FRIEND_NOT_FOUND)
AddFriendFailurePattern("list_full", ERR_FRIEND_LIST_FULL)
AddFriendFailurePattern("self", ERR_FRIEND_SELF)
AddFriendFailurePattern("already_friend", ERR_FRIEND_ALREADY_S)

local function MatchFriendAddFailureReason(message)
    if type(message) ~= "string" or message == "" then
        return nil
    end

    for _, entry in ipairs(FRIEND_ADD_FAILURE_PATTERNS) do
        local capture = string.match(message, entry.pattern)
        if capture then
            if entry.capturesName then
                return entry.reason, capture
            end

            return entry.reason, nil
        end
    end

    local normalizedMessage = string.lower(message)
    if string.find(normalizedMessage, "own faction", 1, true)
        or string.find(normalizedMessage, "is an enemy", 1, true)
        or (string.find(normalizedMessage, "enemy", 1, true) and string.find(normalizedMessage, "friend", 1, true)) then
        return "wrong_faction", nil
    end

    if string.find(normalizedMessage, "friends list is full", 1, true) then
        return "list_full", nil
    end

    if string.find(normalizedMessage, "player not found", 1, true)
        or string.find(normalizedMessage, "not found", 1, true) then
        return "not_found", nil
    end

    return nil
end

local function NormalizeName(name)
    if not name or name == "" then
        return nil
    end

    local shortName = string.match(name, "^([^%-]+)")
    if shortName and shortName ~= "" then
        name = shortName
    end

    return string.lower(name)
end

local METHOD_LOG_ORDER = {
    "lookup:start",
    "lookup:complete",
    "lookup:timeout",
    "friends:GetFriendInfo",
    "channel:GetChannelMemberInfo",
    "channel:GetChannelRosterInfo",
}

local METHOD_LOG_KNOWN = {}
for _, methodName in ipairs(METHOD_LOG_ORDER) do
    METHOD_LOG_KNOWN[methodName] = true
end

local function CountEntries(tableValue)
    if type(tableValue) ~= "table" then
        return 0
    end

    local count = 0
    for _ in pairs(tableValue) do
        count = count + 1
    end

    return count
end

local function EnsureMethodStat(methodName)
    if type(methodName) ~= "string" or methodName == "" then
        return nil
    end

    local stat = WhoDat.methodStats[methodName]
    if stat then
        return stat
    end

    stat = {
        attempts = 0,
        success = 0,
        fail = 0,
        unavailable = 0,
        players = {},
        uniquePlayers = 0,
        reasons = {},
    }

    WhoDat.methodStats[methodName] = stat
    return stat
end

local function RecordMethodDiagnostic(methodName, success, reason, playerName)
    local stat = EnsureMethodStat(methodName)
    if not stat then
        return
    end

    stat.attempts = stat.attempts + 1
    if success then
        stat.success = stat.success + 1
    else
        stat.fail = stat.fail + 1
    end

    if reason == "unavailable" then
        stat.unavailable = stat.unavailable + 1
    end

    if type(reason) == "string" and reason ~= "" then
        stat.reasons[reason] = (stat.reasons[reason] or 0) + 1
    end

    local normalizedPlayerName = NormalizeName(playerName)
    if normalizedPlayerName and not stat.players[normalizedPlayerName] then
        stat.players[normalizedPlayerName] = true
        stat.uniquePlayers = stat.uniquePlayers + 1
    end
end

local function AddDebugEvent(message)
    if type(message) ~= "string" or message == "" then
        return
    end

    local entry = string.format("[%s] %s", date("%H:%M:%S"), message)
    table.insert(WhoDat.debugEvents, entry)

    if WhoDat.persistDebugToSavedVariables then
        if type(WhoDat.persistentDebugEvents) ~= "table" then
            WhoDat.persistentDebugEvents = {}
        end

        table.insert(WhoDat.persistentDebugEvents, entry)
        local persistentMax = WhoDat.persistentDebugLogMaxEvents or 500
        while #WhoDat.persistentDebugEvents > persistentMax do
            table.remove(WhoDat.persistentDebugEvents, 1)
        end
    end

    while #WhoDat.debugEvents > WhoDat.debugLogMaxEvents do
        table.remove(WhoDat.debugEvents, 1)
    end
end

local function IncrementSummaryCounter(counterKey, amount)
    if type(counterKey) ~= "string" or counterKey == "" then
        return
    end

    if type(WhoDat.debugSummary[counterKey]) ~= "number" then
        WhoDat.debugSummary[counterKey] = 0
    end

    WhoDat.debugSummary[counterKey] = WhoDat.debugSummary[counterKey] + (amount or 1)
end

local function IncrementTimeoutReason(reason)
    if type(reason) ~= "string" or reason == "" then
        reason = "unknown"
    end

    local timeoutReasons = WhoDat.debugSummary.timeoutReasons
    timeoutReasons[reason] = (timeoutReasons[reason] or 0) + 1
end

local function ResetDebugTracking()
    WhoDat.methodStats = {}
    WhoDat.methodStatsStartedAt = GetTime()
    WhoDat.debugEvents = {}
    WhoDat.debugSummary = {
        lookupsStarted = 0,
        lookupsCompleted = 0,
        completedFriend = 0,
        completedWithoutFriend = 0,
        prefillCompleted = 0,
        timeouts = 0,
        timeoutReasons = {},
    }
    WhoDat.lookupSequence = 0
end

local function BuildTimeoutReason(pending, baseReason)
    local reason = baseReason or "timeout"

    if pending and pending.lastFailureReason then
        reason = reason .. "|failure:" .. pending.lastFailureReason
    end

    local prefillSource = pending and pending.prefillData and pending.prefillData.source or nil
    if type(prefillSource) == "string" and prefillSource ~= "" then
        reason = reason .. "|prefill:" .. prefillSource
    end

    return reason
end

local function RegisterLookupTimeout(pending, baseReason)
    if not pending or pending.timeoutLogged then
        return
    end

    pending.timeoutLogged = true
    local reason = BuildTimeoutReason(pending, baseReason)
    IncrementSummaryCounter("timeouts", 1)
    IncrementTimeoutReason(reason)
    RecordMethodDiagnostic("lookup:timeout", false, reason, pending.requestName)

    local lookupId = pending.lookupId or 0
    AddDebugEvent(string.format("lookup #%d timeout for %s (%s)", lookupId, pending.requestName, reason))
end

local function BuildReasonSummary(reasonMap)
    if type(reasonMap) ~= "table" then
        return "none"
    end

    local reasonItems = {}
    for reason, count in pairs(reasonMap) do
        table.insert(reasonItems, {
            reason = reason,
            count = count,
        })
    end

    if #reasonItems == 0 then
        return "none"
    end

    table.sort(reasonItems, function(left, right)
        if left.count == right.count then
            return left.reason < right.reason
        end

        return left.count > right.count
    end)

    local textItems = {}
    for _, entry in ipairs(reasonItems) do
        table.insert(textItems, string.format("%s=%d", entry.reason, entry.count))
    end

    return table.concat(textItems, ",")
end

local function PrintDebugLog()
    local elapsed = GetTime() - (WhoDat.methodStatsStartedAt or GetTime())
    ChatPrint(string.format(
        "debug summary: started=%d completed=%d prefill=%d friend=%d without_friend=%d timeouts=%d uptime=%.1fs",
        WhoDat.debugSummary.lookupsStarted or 0,
        WhoDat.debugSummary.lookupsCompleted or 0,
        WhoDat.debugSummary.prefillCompleted or 0,
        WhoDat.debugSummary.completedFriend or 0,
        WhoDat.debugSummary.completedWithoutFriend or 0,
        WhoDat.debugSummary.timeouts or 0,
        elapsed
    ))

    for _, methodName in ipairs(METHOD_LOG_ORDER) do
        local stat = WhoDat.methodStats[methodName]
        if stat then
            ChatPrint(string.format(
                "%s attempts=%d success=%d fail=%d unavailable=%d players=%d reasons=%s",
                methodName,
                stat.attempts,
                stat.success,
                stat.fail,
                stat.unavailable,
                stat.uniquePlayers,
                BuildReasonSummary(stat.reasons)
            ))
        end
    end

    local extraMethods = {}
    for methodName in pairs(WhoDat.methodStats) do
        if not METHOD_LOG_KNOWN[methodName] then
            table.insert(extraMethods, methodName)
        end
    end

    table.sort(extraMethods)
    for _, methodName in ipairs(extraMethods) do
        local stat = WhoDat.methodStats[methodName]
        ChatPrint(string.format(
            "%s attempts=%d success=%d fail=%d unavailable=%d players=%d reasons=%s",
            methodName,
            stat.attempts,
            stat.success,
            stat.fail,
            stat.unavailable,
            stat.uniquePlayers,
            BuildReasonSummary(stat.reasons)
        ))
    end

    if #WhoDat.debugEvents > 0 then
        ChatPrint("recent-events:")
        local startIndex = #WhoDat.debugEvents - 14
        if startIndex < 1 then
            startIndex = 1
        end

        for i = startIndex, #WhoDat.debugEvents do
            ChatPrint(WhoDat.debugEvents[i])
        end
    end
end

local function PrintPersistentDebugLog()
    if type(WhoDat.persistentDebugEvents) ~= "table" or #WhoDat.persistentDebugEvents == 0 then
        ChatPrint("persistent-debug-log: empty.")
        return
    end

    ChatPrint(string.format(
        "persistent-debug-log: entries=%d (written to SavedVariables on logout/reload).",
        #WhoDat.persistentDebugEvents
    ))

    local startIndex = #WhoDat.persistentDebugEvents - 29
    if startIndex < 1 then
        startIndex = 1
    end

    for i = startIndex, #WhoDat.persistentDebugEvents do
        ChatPrint(WhoDat.persistentDebugEvents[i])
    end
end

local function ClearPersistentDebugLog()
    WhoDat.persistentDebugEvents = {}
    if type(WhoDatDB) == "table" then
        WhoDatDB.persistentDebugEvents = WhoDat.persistentDebugEvents
    end

    ChatPrint("Persistent debug log cleared.")
end

local function DebugPrint(message)
    if WhoDat.debugEnabled then
        ChatPrint("debug: " .. message)
    end
end

local function FormatPeerSummaryDataForDebug(summaryData)
    if type(summaryData) ~= "table" then
        return "data=none"
    end

    local function ToText(value)
        if value == nil then
            return "?"
        end

        local text = tostring(value)
        if text == "" then
            return "?"
        end

        return text
    end

    local onlineText = "?"
    if summaryData.online == true then
        onlineText = "1"
    elseif summaryData.online == false then
        onlineText = "0"
    end

    return string.format(
        "name=%s level=%s class=%s area=%s online=%s faction=%s race=%s source=%s",
        ToText(summaryData.name),
        ToText(summaryData.level),
        ToText(summaryData.class),
        ToText(summaryData.area),
        onlineText,
        ToText(summaryData.faction),
        ToText(summaryData.race),
        ToText(summaryData.source)
    )
end

local function PeerDebugPrint(message)
    if not WhoDat.debugEnabled then
        return
    end

    ChatPrint("peer-debug: " .. tostring(message or ""))
end

local function QueueFriendSystemMessageSuppression(name, action)
    local normalizedName = NormalizeName(name)
    if not normalizedName then
        return
    end

    local bucket = WhoDat.suppressedFriendSystemMessages[action]
    if not bucket then
        return
    end

    bucket[normalizedName] = GetTime() + WhoDat.systemMessageSuppressSeconds
    DebugPrint(string.format("queued %s suppress for %s", action, normalizedName))
end

local function ConsumeFriendSystemMessageSuppression(name, action)
    local normalizedName = NormalizeName(name)
    if not normalizedName then
        return false
    end

    local bucket = WhoDat.suppressedFriendSystemMessages[action]
    if not bucket then
        return false
    end

    local expiresAt = bucket[normalizedName]
    if not expiresAt then
        return false
    end

    bucket[normalizedName] = nil
    if GetTime() > expiresAt then
        DebugPrint(string.format("expired %s suppress for %s", action, normalizedName))
        return false
    end

    DebugPrint(string.format("consumed %s suppress for %s", action, normalizedName))
    return true
end

local function ShouldSuppressSystemMessage(message)
    if type(message) ~= "string" then
        return false
    end

    if FRIEND_ADDED_PATTERN then
        local addedName = string.match(message, FRIEND_ADDED_PATTERN)
        if addedName and ConsumeFriendSystemMessageSuppression(addedName, "add") then
            DebugPrint("suppressed system message: " .. message)
            return true
        end
    end

    if FRIEND_REMOVED_PATTERN then
        local removedName = string.match(message, FRIEND_REMOVED_PATTERN)
        if removedName and ConsumeFriendSystemMessageSuppression(removedName, "remove") then
            DebugPrint("suppressed system message: " .. message)
            return true
        end
    end

    if HandleFriendLookupFailureMessage and HandleFriendLookupFailureMessage(message) then
        return true
    end

    local normalizedMessage = string.lower(message)
    for action, bucket in pairs(WhoDat.suppressedFriendSystemMessages) do
        for normalizedName, expiresAt in pairs(bucket) do
            if GetTime() > expiresAt then
                bucket[normalizedName] = nil
            elseif string.find(normalizedMessage, normalizedName, 1, true) then
                bucket[normalizedName] = nil
                DebugPrint(string.format("suppressed %s message by name fallback: %s", action, message))
                return true
            end
        end
    end

    return false
end

local function SystemMessageFilter(_, _, message)
    return ShouldSuppressSystemMessage(message)
end

local function ExtractNameFromPlayerLink(link)
    if type(link) ~= "string" then
        return nil
    end

    local linkType, payload = string.match(link, "^(%a+):(.+)$")
    if linkType ~= "player" then
        return nil
    end

    return string.match(payload, "^([^:]+)")
end

local function GetFriendIndexByName(name)
    local lookup = NormalizeName(name)
    if not lookup then
        return nil
    end

    local totalFriends = GetNumFriends() or 0
    for i = 1, totalFriends do
        local friendName = GetFriendInfo(i)
        if friendName and NormalizeName(friendName) == lookup then
            return i
        end
    end

    return nil
end

local LEVEL_COLOR_GRAY = "9d9d9d"
local LEVEL_COLOR_GREEN = "1eff00"
local LEVEL_COLOR_YELLOW = "ffff00"
local LEVEL_COLOR_ORANGE = "ff9900"
local LEVEL_COLOR_RED = "ff3f3f"
local NAME_COLOR_HORDE = "ff4d4d"
local NAME_COLOR_ALLIANCE = "4da6ff"

local RACE_FACTION_BY_KEY = {
    human = "Alliance",
    dwarf = "Alliance",
    nightelf = "Alliance",
    gnome = "Alliance",
    draenei = "Alliance",
    orc = "Horde",
    undead = "Horde",
    tauren = "Horde",
    troll = "Horde",
    bloodelf = "Horde",
    scourge = "Horde",
    forsaken = "Horde",
}

local function ColorizeHexText(text, colorHex)
    if type(text) ~= "string" or text == "" then
        return text
    end

    if type(colorHex) ~= "string" or colorHex == "" then
        return text
    end

    return string.format("|cff%s%s|r", colorHex, text)
end

local function NormalizeFaction(value)
    if type(value) ~= "string" or value == "" then
        return nil
    end

    local lowerValue = string.lower(value)
    if lowerValue == "horde" then
        return "Horde"
    elseif lowerValue == "alliance" then
        return "Alliance"
    end

    return nil
end

local function NormalizeRaceKey(raceName)
    if type(raceName) ~= "string" then
        return nil
    end

    local key = string.lower(raceName)
    key = string.gsub(key, "[^%a]", "")
    if key == "" then
        return nil
    end

    return key
end

local function InferFactionFromRace(raceName)
    local raceKey = NormalizeRaceKey(raceName)
    if not raceKey then
        return nil
    end

    return RACE_FACTION_BY_KEY[raceKey]
end

local function GetFactionFromUnit(unit)
    if type(UnitFactionGroup) ~= "function" then
        return nil
    end

    local localizedFaction, englishFaction = UnitFactionGroup(unit)
    return NormalizeFaction(englishFaction) or NormalizeFaction(localizedFaction)
end

local function ColorizeNameByFaction(nameText, factionText)
    local faction = NormalizeFaction(factionText)
    if faction == "Horde" then
        return ColorizeHexText(nameText, NAME_COLOR_HORDE)
    elseif faction == "Alliance" then
        return ColorizeHexText(nameText, NAME_COLOR_ALLIANCE)
    end

    return nameText
end

local function ColorizeLevelByDifficulty(levelValue)
    if not levelValue or levelValue <= 0 then
        return "?"
    end

    local levelText = tostring(levelValue)
    local playerLevel = UnitLevel("player")
    if type(playerLevel) ~= "number" or playerLevel <= 0 then
        return levelText
    end

    local delta = levelValue - playerLevel
    if delta < -5 then
        return ColorizeHexText(levelText, LEVEL_COLOR_GRAY)
    elseif delta <= -3 then
        return ColorizeHexText(levelText, LEVEL_COLOR_GREEN)
    elseif delta <= 2 then
        return ColorizeHexText(levelText, LEVEL_COLOR_YELLOW)
    elseif delta <= 5 then
        return ColorizeHexText(levelText, LEVEL_COLOR_ORANGE)
    end

    return ColorizeHexText(levelText, LEVEL_COLOR_RED)
end

local function ColorizeClass(className)
    if not className or className == "" then
        return UNKNOWN
    end

    local token = CLASS_TOKEN_BY_LOCALIZED[className] or string.upper(className)
    local color = RAID_CLASS_COLORS[token]
    if not color then
        return className
    end

    local r = math.floor((color.r or 1) * 255 + 0.5)
    local g = math.floor((color.g or 1) * 255 + 0.5)
    local b = math.floor((color.b or 1) * 255 + 0.5)

    return string.format("|cff%02x%02x%02x%s|r", r, g, b, className)
end

local function BuildSummary(data)
    local name = (data and data.name) or UNKNOWN
    local faction = (data and data.faction) or nil
    if not faction and data and data.race then
        faction = InferFactionFromRace(data.race)
    end
    local coloredName = ColorizeNameByFaction(name, faction)

    if data and data.online == false then
        return string.format("%s is offline.", coloredName)
    end

    local levelText = ColorizeLevelByDifficulty(data and data.level)

    local classText = ColorizeClass(data and data.class)
    local areaText = (data and data.area and data.area ~= "") and data.area or UNKNOWN

    return string.format(
        "%s: Level %s %s, %s.",
        coloredName,
        levelText,
        classText,
        areaText
    )
end

local function HasMinimumInfo(data)
    if not data then
        return false
    end

    if not data.level or data.level <= 0 then
        return false
    end

    if not data.class or data.class == "" or data.class == UNKNOWN then
        return false
    end

    if not data.area or data.area == "" or data.area == UNKNOWN then
        return false
    end

    return true
end

local function HasKnownText(value)
    return value and value ~= "" and value ~= UNKNOWN
end

local function HasAnySummaryInfo(data)
    if not data then
        return false
    end

    if data.level and data.level > 0 then
        return true
    end

    if data.online ~= nil then
        return true
    end

    if HasKnownText(data.name)
        or HasKnownText(data.class)
        or HasKnownText(data.area)
        or HasKnownText(data.race)
        or HasKnownText(data.faction)
        or HasKnownText(data.status)
        or HasKnownText(data.note)
        or HasKnownText(data.extra)
        or HasKnownText(data.source) then
        return true
    end

    return false
end

local function HasPartialIdentityInfo(data)
    if not data then
        return false
    end

    if data.level and data.level > 0 then
        return true
    end

    if HasKnownText(data.class) or HasKnownText(data.area) then
        return true
    end

    return false
end

local function PickText(primary, fallback)
    if HasKnownText(primary) then
        return primary
    end

    if HasKnownText(fallback) then
        return fallback
    end

    return primary or fallback
end

local function PickLevel(primary, fallback)
    if primary and primary > 0 then
        return primary
    end

    if fallback and fallback > 0 then
        return fallback
    end

    return primary or fallback
end

local function MergeSummaryData(primary, fallback)
    primary = primary or {}
    fallback = fallback or {}

    local merged = {
        name = PickText(primary.name, fallback.name),
        level = PickLevel(primary.level, fallback.level),
        class = PickText(primary.class, fallback.class),
        area = PickText(primary.area, fallback.area),
        race = PickText(primary.race, fallback.race),
        faction = PickText(primary.faction, fallback.faction),
        status = PickText(primary.status, fallback.status),
        note = PickText(primary.note, fallback.note),
        extra = PickText(primary.extra, fallback.extra),
    }

    merged.faction = NormalizeFaction(merged.faction) or InferFactionFromRace(merged.race)

    if primary.online ~= nil then
        merged.online = primary.online
    else
        merged.online = fallback.online
    end

    if primary.source and primary.source ~= "" and fallback.source and fallback.source ~= "" and primary.source ~= fallback.source then
        merged.source = primary.source .. "+" .. fallback.source
    else
        merged.source = PickText(primary.source, fallback.source)
    end

    return merged
end

local function CloneSummaryData(data)
    if not data then
        return nil
    end

    local cloned = {}
    for key, value in pairs(data) do
        cloned[key] = value
    end

    return cloned
end

local function LooksLikeClassName(className)
    if type(className) ~= "string" or className == "" then
        return false
    end

    if CLASS_TOKEN_BY_LOCALIZED[className] then
        return true
    end

    return RAID_CLASS_COLORS[string.upper(className)] ~= nil
end

local function ParseOnlineFlag(value)
    if value == true or value == 1 then
        return true
    end

    if value == false or value == 0 then
        return false
    end

    return nil
end

local function ParseLevelValue(value)
    local level = tonumber(value)
    if not level then
        return nil
    end

    level = math.floor(level + 0.5)
    if level <= 0 or level > 100 then
        return nil
    end

    return level
end

local function SanitizeAddonField(value)
    if value == nil then
        return ""
    end

    value = tostring(value)
    value = string.gsub(value, "[\t\r\n]", " ")
    return value
end

local function ParseAddonOnlineField(value)
    if value == "1" then
        return true
    end

    if value == "0" then
        return false
    end

    return nil
end

local function NormalizeChannelNameForMatch(channelName)
    if type(channelName) ~= "string" then
        return ""
    end

    local normalized = string.lower(channelName)
    normalized = string.gsub(normalized, "^%d+%.%s*", "")
    return normalized
end

local function IsWhodatChannelName(channelName)
    return NormalizeChannelNameForMatch(channelName) == NormalizeChannelNameForMatch(WhoDat.peerChannelName)
end

local function GetPlayerFaction()
    local raceLocalized = select(1, UnitRace("player"))
    return GetFactionFromUnit("player") or InferFactionFromRace(raceLocalized)
end

local function CanSendPeerChannelMessages()
    if type(UnitLevel) ~= "function" then
        return false
    end

    local level = UnitLevel("player")
    return type(level) == "number" and level >= WhoDat.peerChannelMinLevel
end

local function CanSendPeerWhisperMessages()
    if type(UnitLevel) ~= "function" then
        return false
    end

    local level = UnitLevel("player")
    local minLevel = WhoDat.peerWhisperMinLevel or 9
    return type(level) == "number" and level >= minLevel
end

local function RegisterWhoDatAddonPrefix()
    local prefix = WhoDat.addonCommPrefix
    if type(prefix) ~= "string" or prefix == "" then
        return false
    end

    if C_ChatInfo and type(C_ChatInfo.RegisterAddonMessagePrefix) == "function" then
        local ok = pcall(C_ChatInfo.RegisterAddonMessagePrefix, prefix)
        return ok
    end

    if type(RegisterAddonMessagePrefix) == "function" then
        local ok = pcall(RegisterAddonMessagePrefix, prefix)
        return ok
    end

    return false
end

local function GetWhodatChannelInfo()
    if type(GetChannelList) ~= "function" then
        return nil, nil
    end

    local channels = { GetChannelList() }
    for index = 1, #channels, 2 do
        local channelId = tonumber(channels[index])
        local channelName = channels[index + 1]
        if channelId and channelId > 0 and IsWhodatChannelName(channelName) then
            return channelId, channelName
        end
    end

    return nil, nil
end

local function EnsureWhodatChannelJoined(forceAttempt)
    local channelId, channelName = GetWhodatChannelInfo()
    if channelId then
        return channelId, channelName
    end

    if type(JoinChannelByName) ~= "function" then
        return nil, nil
    end

    local now = GetTime()
    if not forceAttempt then
        local lastAttemptAt = WhoDat.lastPeerChannelJoinAttemptAt or 0
        if lastAttemptAt > 0 and now - lastAttemptAt < WhoDat.peerChannelJoinRetrySeconds then
            return nil, nil
        end
    end

    WhoDat.lastPeerChannelJoinAttemptAt = now
    pcall(JoinChannelByName, WhoDat.peerChannelName)

    return GetWhodatChannelInfo()
end

local function TrackPeerChannelMember(name, level)
    local normalizedName = NormalizeName(name)
    if not normalizedName then
        return
    end

    local existingEntry = WhoDat.peerChannelMembers[normalizedName]
    local shortName = string.match(name, "^([^%-]+)") or name
    WhoDat.peerChannelMembers[normalizedName] = {
        name = shortName,
        level = ParseLevelValue(level) or (existingEntry and existingEntry.level) or nil,
        seenAt = GetTime(),
    }
end

local function GetKnownPeerChannelMemberLevel(name)
    local normalizedName = NormalizeName(name)
    if not normalizedName then
        return nil
    end

    local entry = WhoDat.peerChannelMembers[normalizedName]
    if not entry then
        return nil
    end

    if type(entry.level) == "number" and entry.level > 0 then
        return entry.level
    end

    return nil
end

local function SetPeerFaction(name, faction, source)
    local normalizedName = NormalizeName(name)
    local normalizedFaction = NormalizeFaction(faction)
    if not normalizedName or not normalizedFaction then
        return false
    end

    local selfName = UnitName("player")
    if selfName and NormalizeName(selfName) == normalizedName then
        return false
    end

    local myFaction = GetPlayerFaction()
    if myFaction and normalizedFaction == myFaction then
        -- Keep cache focused on cross-faction peers only.
        WhoDat.peerFactionCache[normalizedName] = nil
        return false
    end

    local existingEntry = WhoDat.peerFactionCache[normalizedName]
    if existingEntry and existingEntry.faction == normalizedFaction then
        existingEntry.name = string.match(name, "^([^%-]+)") or name
        existingEntry.source = source or existingEntry.source or "peer"
        existingEntry.seenAt = GetTime()
        return true
    end

    local factionCount = 0
    for cachedName, entry in pairs(WhoDat.peerFactionCache) do
        if cachedName ~= normalizedName and entry and entry.faction == normalizedFaction then
            factionCount = factionCount + 1
        end
    end

    if factionCount >= (WhoDat.peerFactionMaxPerFaction or 32) then
        return false
    end

    local shortName = string.match(name, "^([^%-]+)") or name
    WhoDat.peerFactionCache[normalizedName] = {
        name = shortName,
        faction = normalizedFaction,
        source = source or "peer",
        seenAt = GetTime(),
    }

    return true
end

local function GetPeerFactionCacheCount(faction)
    local targetFaction = NormalizeFaction(faction)
    if not targetFaction then
        return 0
    end

    local count = 0
    for normalizedName, entry in pairs(WhoDat.peerFactionCache) do
        if entry and entry.faction == targetFaction and WhoDat.peerChannelMembers[normalizedName] then
            count = count + 1
        end
    end

    return count
end

local function GetPeerFactionCacheCounts()
    local allianceCount = GetPeerFactionCacheCount("Alliance")
    local hordeCount = GetPeerFactionCacheCount("Horde")
    return allianceCount, hordeCount
end

local function ChooseRandomPeerByFaction(faction)
    local targetFaction = NormalizeFaction(faction)
    if not targetFaction then
        return nil
    end

    local selfNormalized = NormalizeName(UnitName("player") or "")
    local selectedName = nil
    local candidateCount = 0

    local minWhisperLevel = WhoDat.peerWhisperMinLevel or 9

    for normalizedName, entry in pairs(WhoDat.peerFactionCache) do
        if entry
            and entry.faction == targetFaction
            and normalizedName ~= selfNormalized
            and WhoDat.peerChannelMembers[normalizedName]
            and WhoDat.peerAddonConfirmed[normalizedName] then
            local knownLevel = GetKnownPeerChannelMemberLevel(entry.name)
            if (not knownLevel) or knownLevel >= minWhisperLevel then
                candidateCount = candidateCount + 1
                if math.random(candidateCount) == 1 then
                    selectedName = entry.name
                end
            end
        end
    end

    return selectedName
end

local function SendWhoDatAddonMessage(message, distribution, target)
    if type(SendAddonMessage) ~= "function" then
        return false, "unavailable"
    end

    if distribution == "WHISPER" then
        if not CanSendPeerWhisperMessages() then
            return false, "whisper_level_gated"
        end

        local knownTargetLevel = GetKnownPeerChannelMemberLevel(target)
        local minLevel = WhoDat.peerWhisperMinLevel or 9
        if knownTargetLevel and knownTargetLevel < minLevel then
            return false, "target_level_gated"
        end
    end

    local ok, result = pcall(SendAddonMessage, WhoDat.addonCommPrefix, message, distribution, target)
    if not ok then
        return false, tostring(result)
    end

    if result == false then
        return false, "api_returned_false"
    end

    return true, nil
end

local function BuildPeerRequestId()
    WhoDat.peerRequestSequence = (WhoDat.peerRequestSequence or 0) + 1
    return string.format("%d-%d", math.floor(GetTime() * 100), WhoDat.peerRequestSequence)
end

local function BuildPeerFactionMessage(faction)
    return string.format("F\t%s", SanitizeAddonField(faction))
end

local function ParsePeerFactionMessage(message)
    if type(message) ~= "string" then
        return nil
    end

    local factionText = string.match(message, "^F\t(.*)$")
    if not factionText or factionText == "" then
        return nil
    end

    return NormalizeFaction(factionText)
end

local function BuildPeerLookupRequestMessage(requestId, targetName)
    return string.format("Q\t%s\t%s", SanitizeAddonField(requestId), SanitizeAddonField(targetName))
end

local function ParsePeerLookupRequestMessage(message)
    if type(message) ~= "string" then
        return nil, nil
    end

    local requestId, targetName = string.match(message, "^Q\t([^\t]*)\t(.*)$")
    if not requestId or requestId == "" or not targetName or targetName == "" then
        return nil, nil
    end

    return requestId, targetName
end

local function BuildPeerLookupResponseMessage(requestId, targetName, summaryData)
    local levelText = summaryData and summaryData.level and tostring(summaryData.level) or ""
    local classText = summaryData and summaryData.class or ""
    local areaText = summaryData and summaryData.area or ""
    local onlineText = ""
    if summaryData and summaryData.online == true then
        onlineText = "1"
    elseif summaryData and summaryData.online == false then
        onlineText = "0"
    end

    local raceText = summaryData and summaryData.race or ""
    local factionText = summaryData and summaryData.faction or InferFactionFromRace(raceText) or ""

    return string.format(
        "R\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
        SanitizeAddonField(requestId),
        SanitizeAddonField(targetName),
        SanitizeAddonField(levelText),
        SanitizeAddonField(classText),
        SanitizeAddonField(areaText),
        SanitizeAddonField(onlineText),
        SanitizeAddonField(factionText),
        SanitizeAddonField(raceText)
    )
end

local function ParsePeerLookupResponseMessage(message)
    if type(message) ~= "string" then
        return nil
    end

    local requestId, targetName, levelText, classText, areaText, onlineText, factionText, raceText =
        string.match(message, "^R\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t(.*)$")

    if not requestId or requestId == "" or not targetName or targetName == "" then
        return nil
    end

    local summaryData = {
        name = targetName,
        level = ParseLevelValue(levelText),
        class = classText ~= "" and classText or nil,
        area = areaText ~= "" and areaText or nil,
        online = ParseAddonOnlineField(onlineText),
        faction = NormalizeFaction(factionText),
        race = raceText ~= "" and raceText or nil,
        source = "peer:proxy",
        extra = "reported by peer addon",
    }

    if summaryData.class and not LooksLikeClassName(summaryData.class) then
        summaryData.class = nil
    end

    if not summaryData.faction then
        summaryData.faction = InferFactionFromRace(summaryData.race)
    end

    return requestId, targetName, summaryData
end

local function ParseWhodatChannelHelloMessage(message)
    if type(message) ~= "string" then
        return nil
    end

    local factionText = string.match(message, "^WDHELLO%s+([%a]+)")
    if not factionText then
        return nil
    end

    return NormalizeFaction(factionText)
end

local function SendPeerFactionWhisper(targetName, reason)
    local myFaction = GetPlayerFaction()
    if not myFaction or type(targetName) ~= "string" or targetName == "" then
        return false
    end

    local selfName = UnitName("player")
    if selfName and NormalizeName(selfName) == NormalizeName(targetName) then
        return false
    end

    local sent, sendError = SendWhoDatAddonMessage(BuildPeerFactionMessage(myFaction), "WHISPER", targetName)
    if sent then
        AddDebugEvent(string.format("peer faction sent to %s (%s)", targetName, reason or "unknown"))
    else
        AddDebugEvent(string.format("peer faction send failed to %s (%s: %s)", targetName, reason or "unknown", sendError or "unknown"))
    end

    return sent
end

local function SendPeerRosterFactionProbe(forceSend)
    if not CanSendPeerChannelMessages() then
        return 0
    end

    local now = GetTime()
    if not forceSend then
        local lastProbeAt = WhoDat.lastPeerRosterProbeAt or 0
        if lastProbeAt > 0 and now - lastProbeAt < (WhoDat.peerRosterProbeCooldownSeconds or 30) then
            return 0
        end
    end

    local channelId, channelName = GetWhodatChannelInfo()
    if channelId then
        SyncWhodatPeerMembership(channelId, channelName)
    end

    local selfNormalized = NormalizeName(UnitName("player") or "")
    local sentCount = 0
    local failCount = 0
    local maxTargets = WhoDat.peerRosterProbeMaxTargetsPerRun or 12

    for normalizedName, entry in pairs(WhoDat.peerChannelMembers) do
        if sentCount >= maxTargets then
            break
        end

        if normalizedName ~= selfNormalized and entry and entry.name and entry.name ~= "" then
            if SendPeerFactionWhisper(entry.name, "roster_probe") then
                sentCount = sentCount + 1
            else
                failCount = failCount + 1
            end
        end
    end

    if sentCount > 0 then
        WhoDat.lastPeerRosterProbeAt = now
        AddDebugEvent(string.format("peer roster probe sent=%d failed=%d", sentCount, failCount))
    else
        if failCount > 0 then
            AddDebugEvent(string.format("peer roster probe sent=0 failed=%d", failCount))
        else
            AddDebugEvent("peer roster probe found no eligible members")
        end
    end

    return sentCount
end

local function SendWhodatChannelHello(channelId, channelName, forceSend)
    if not CanSendPeerChannelMessages() then
        return false, "level_gated"
    end

    local myFaction = GetPlayerFaction()
    if not myFaction then
        return false, "no_faction"
    end

    if type(SendChatMessage) ~= "function" then
        return false, "unavailable"
    end

    local now = GetTime()
    if not forceSend then
        local lastHelloAt = WhoDat.lastPeerChannelHelloAt or 0
        if lastHelloAt > 0 and now - lastHelloAt < WhoDat.peerChannelHelloCooldownSeconds then
            return false, "cooldown"
        end
    end

    local numericChannelId = tonumber(channelId)
    if (not numericChannelId or numericChannelId <= 0) and (type(channelName) ~= "string" or channelName == "") then
        return false, "invalid_channel"
    end

    local helloMessage = string.format("WDHELLO %s", myFaction)
    local sendTarget = numericChannelId and numericChannelId > 0 and numericChannelId or channelName
    local languageTarget = nil
    if type(GetDefaultLanguage) == "function" then
        local defaultLanguage = GetDefaultLanguage("player")
        if type(defaultLanguage) == "string" and defaultLanguage ~= "" then
            languageTarget = defaultLanguage
        end
    end

    local ok, sendError = pcall(SendChatMessage, helloMessage, "CHANNEL", languageTarget, sendTarget)
    if not ok then
        AddDebugEvent(string.format("peer hello send failed (%s)", tostring(sendError)))
        return false, "send_failed"
    end

    WhoDat.lastPeerChannelHelloAt = now
    WhoDat.lastPeerChannelHelloAttemptAt = now

    AddDebugEvent(string.format(
        "peer hello sent to %s as %s (target=%s, language=%s)",
        WhoDat.peerChannelName,
        myFaction,
        tostring(sendTarget),
        tostring(languageTarget)
    ))
    return true, nil
end

local function TryStartupWhodatHello(channelId, channelName)
    local deadlineAt = WhoDat.peerStartupHelloDeadlineAt or 0
    if deadlineAt <= 0 or WhoDat.peerStartupHelloDone then
        return false
    end

    local now = GetTime()
    if now > deadlineAt then
        WhoDat.peerStartupHelloDone = true
        WhoDat.peerStartupHelloDeadlineAt = 0
        return false
    end

    local retrySeconds = WhoDat.peerStartupHelloRetrySeconds or 3
    local lastTryAt = WhoDat.lastPeerStartupHelloTryAt or 0
    if lastTryAt > 0 and now - lastTryAt < retrySeconds then
        return false
    end

    WhoDat.lastPeerStartupHelloTryAt = now

    local sent, reason = SendWhodatChannelHello(channelId, channelName, true)
    if sent or reason == "cooldown" or reason == "level_gated" then
        WhoDat.peerStartupHelloDone = true
        WhoDat.peerStartupHelloDeadlineAt = 0
    end

    return sent
end

local function GetTableCount(tableValue)
    local count = 0
    for _ in pairs(tableValue or {}) do
        count = count + 1
    end

    return count
end

local function StoreCachedSummaryData(name, summaryData)
    local normalizedName = NormalizeName(name)
    if not normalizedName or not summaryData then
        return
    end

    local storedData = CloneSummaryData(summaryData)
    storedData.name = storedData.name or name
    storedData.observedAt = GetTime()

    local existingData = WhoDat.channelCache[normalizedName]
    if existingData then
        local mergedData = MergeSummaryData(storedData, existingData)
        mergedData.observedAt = storedData.observedAt
        WhoDat.channelCache[normalizedName] = mergedData
    else
        WhoDat.channelCache[normalizedName] = storedData
    end
end

local function StoreChannelSummaryData(channelName, memberData)
    local normalizedName = NormalizeName(memberData and memberData.name)
    if not normalizedName then
        return
    end

    local summaryData = CloneSummaryData(memberData)
    summaryData.observedAt = GetTime()
    if channelName and channelName ~= "" then
        summaryData.source = "channel:" .. channelName
    else
        summaryData.source = "channel"
    end

    StoreCachedSummaryData(memberData.name, summaryData)
end

local function PruneExpiredChannelCache()
    local now = GetTime()
    for normalizedName, summaryData in pairs(WhoDat.channelCache) do
        if now - (summaryData.observedAt or 0) > WhoDat.channelCacheMaxAgeSeconds then
            WhoDat.channelCache[normalizedName] = nil
        end
    end
end

local function GetCachedChannelSummaryData(playerName)
    local normalizedName = NormalizeName(playerName)
    if not normalizedName then
        return nil
    end

    local cachedData = WhoDat.channelCache[normalizedName]
    if not cachedData then
        return nil
    end

    if GetTime() - (cachedData.observedAt or 0) > WhoDat.channelCacheMaxAgeSeconds then
        WhoDat.channelCache[normalizedName] = nil
        return nil
    end

    return CloneSummaryData(cachedData)
end

local function GetChannelMemberInfoSummaryData(channelId, memberIndex)
    if type(GetChannelMemberInfo) ~= "function" then
        RecordMethodDiagnostic("channel:GetChannelMemberInfo", false, "unavailable")
        return nil
    end

    local previousDisplayChannel = nil
    local usedSelectedChannel = false
    if type(SetSelectedDisplayChannel) == "function" then
        if type(GetSelectedDisplayChannel) == "function" then
            previousDisplayChannel = GetSelectedDisplayChannel()
        end

        pcall(SetSelectedDisplayChannel, channelId)
        usedSelectedChannel = true
    end

    local ok, name, level, class, area, connected = pcall(GetChannelMemberInfo, memberIndex, channelId)
    local usedFallbackOrder = false
    local usedSingleArg = false
    if (not ok) or type(name) ~= "string" or name == "" then
        usedFallbackOrder = true
        ok, name, level, class, area, connected = pcall(GetChannelMemberInfo, channelId, memberIndex)
    end

    if (not ok) or type(name) ~= "string" or name == "" then
        usedSingleArg = true
        ok, name, level, class, area, connected = pcall(GetChannelMemberInfo, memberIndex)
    end

    if usedSelectedChannel and previousDisplayChannel and previousDisplayChannel > 0 then
        pcall(SetSelectedDisplayChannel, previousDisplayChannel)
    end

    if (not ok) or type(name) ~= "string" or name == "" then
        local failureReason = "no_result"
        if usedSingleArg then
            failureReason = "no_result_after_all_signatures"
        elseif usedFallbackOrder then
            failureReason = "no_result_after_swap"
        end

        RecordMethodDiagnostic(
            "channel:GetChannelMemberInfo",
            false,
            failureReason
        )
        return nil
    end

    if type(class) == "string" and type(area) == "string" and not LooksLikeClassName(class) and LooksLikeClassName(area) then
        class, area = area, class
    end

    if type(class) ~= "string" or class == "" or not LooksLikeClassName(class) then
        class = nil
    end

    if type(area) ~= "string" or area == "" then
        area = nil
    end

    local summaryData = {
        name = name,
        level = ParseLevelValue(level),
        class = class,
        area = area,
        online = ParseOnlineFlag(connected),
    }

    local successReason = "ok"
    if usedSingleArg then
        successReason = "ok_selected_channel"
    elseif usedFallbackOrder then
        successReason = "ok_arg_swap"
    end

    RecordMethodDiagnostic(
        "channel:GetChannelMemberInfo",
        true,
        successReason,
        summaryData.name
    )

    return summaryData
end

local function GetChannelRosterInfoSummaryData(channelId, memberIndex)
    if type(GetChannelRosterInfo) ~= "function" then
        RecordMethodDiagnostic("channel:GetChannelRosterInfo", false, "unavailable")
        return nil
    end

    local previousDisplayChannel = nil
    local usedSelectedChannel = false
    if type(SetSelectedDisplayChannel) == "function" then
        if type(GetSelectedDisplayChannel) == "function" then
            previousDisplayChannel = GetSelectedDisplayChannel()
        end

        pcall(SetSelectedDisplayChannel, channelId)
        usedSelectedChannel = true
    end

    local ok, a, b, c, d, e, f, g, h, i = pcall(GetChannelRosterInfo, channelId, memberIndex)
    local usedFallbackOrder = false
    local usedSingleArg = false
    if (not ok) or type(a) ~= "string" or a == "" then
        usedFallbackOrder = true
        ok, a, b, c, d, e, f, g, h, i = pcall(GetChannelRosterInfo, memberIndex, channelId)
    end

    if (not ok) or type(a) ~= "string" or a == "" then
        usedSingleArg = true
        ok, a, b, c, d, e, f, g, h, i = pcall(GetChannelRosterInfo, memberIndex)
    end

    if usedSelectedChannel and previousDisplayChannel and previousDisplayChannel > 0 then
        pcall(SetSelectedDisplayChannel, previousDisplayChannel)
    end

    if (not ok) or type(a) ~= "string" or a == "" then
        local failureReason = "no_result"
        if usedSingleArg then
            failureReason = "no_result_after_all_signatures"
        elseif usedFallbackOrder then
            failureReason = "no_result_after_swap"
        end

        RecordMethodDiagnostic(
            "channel:GetChannelRosterInfo",
            false,
            failureReason
        )
        return nil
    end

    local values = { a, b, c, d, e, f, g, h, i }
    local summaryData = {
        name = a,
        online = ParseOnlineFlag(b),
    }

    for valueIndex = 2, #values do
        local value = values[valueIndex]

        if summaryData.online == nil then
            summaryData.online = ParseOnlineFlag(value)
        end

        if not summaryData.level then
            summaryData.level = ParseLevelValue(value)
        end

        if type(value) == "string" and value ~= "" and value ~= a then
            if not summaryData.class and LooksLikeClassName(value) then
                summaryData.class = value
            elseif not summaryData.area and value ~= UNKNOWN then
                summaryData.area = value
            end
        end
    end

    local successReason = "ok"
    if usedSingleArg then
        successReason = "ok_selected_channel"
    elseif usedFallbackOrder then
        successReason = "ok_arg_swap"
    end

    RecordMethodDiagnostic(
        "channel:GetChannelRosterInfo",
        true,
        successReason,
        summaryData.name
    )

    return summaryData
end

local function GetChannelMemberSummaryData(channelId, memberIndex)
    if type(GetChannelMemberInfo) == "function" then
        local memberInfoData = GetChannelMemberInfoSummaryData(channelId, memberIndex)
        if memberInfoData then
            return memberInfoData
        end
    end

    return GetChannelRosterInfoSummaryData(channelId, memberIndex)
end

local function GetChannelProbeLimit(channelId)
    local memberCount = nil

    if type(GetChannelNumMembers) == "function" then
        local ok, count = pcall(GetChannelNumMembers, channelId)
        if ok and type(count) == "number" and count > 0 then
            memberCount = count
        end
    end

    if (not memberCount or memberCount <= 0) and type(GetNumChannelMembers) == "function" then
        local ok, count = pcall(GetNumChannelMembers, channelId)
        if ok and type(count) == "number" and count > 0 then
            memberCount = count
        end
    end

    if not memberCount or memberCount <= 0 then
        memberCount = WhoDat.channelMaxProbeMembers
    end

    memberCount = math.floor(memberCount)
    if memberCount > WhoDat.channelMaxProbeMembers then
        memberCount = WhoDat.channelMaxProbeMembers
    end

    return memberCount
end

SyncWhodatPeerMembership = function(channelId, channelName)
    if not IsWhodatChannelName(channelName) then
        return
    end

    local numericChannelId = tonumber(channelId)
    if not numericChannelId or numericChannelId <= 0 then
        return
    end

    if type(ChannelRoster) == "function" then
        pcall(ChannelRoster, numericChannelId)
    end

    local now = GetTime()
    local seenMembers = {}
    local probeLimit = GetChannelProbeLimit(numericChannelId)
    local nilBreakThreshold = WhoDat.channelNilBreakThreshold or 15
    local missCount = 0
    local foundMemberCount = 0

    for memberIndex = 1, probeLimit do
        local memberData = GetChannelMemberSummaryData(numericChannelId, memberIndex)
        if memberData and memberData.name then
            missCount = 0
            foundMemberCount = foundMemberCount + 1

            local normalizedName = NormalizeName(memberData.name)
            if normalizedName then
                TrackPeerChannelMember(memberData.name, memberData.level)
                seenMembers[normalizedName] = true
            end
        else
            missCount = missCount + 1
            if missCount >= nilBreakThreshold then
                if foundMemberCount > 0 or memberIndex >= (nilBreakThreshold * 4) then
                    break
                end
            end
        end
    end

    -- Some cores return sparse/incomplete roster snapshots; use fresh channel cache entries as fallback member source.
    local cacheMaxAge = WhoDat.channelCacheMaxAgeSeconds or 900
    for normalizedName, cachedData in pairs(WhoDat.channelCache) do
        if cachedData and now - (cachedData.observedAt or 0) <= cacheMaxAge then
            local source = cachedData.source or ""
            local sourceChannel = string.match(source, "^channel:(.+)$")
            if sourceChannel and IsWhodatChannelName(sourceChannel) then
                local memberName = cachedData.name or normalizedName
                TrackPeerChannelMember(memberName, cachedData.level)
                seenMembers[normalizedName] = true
            end
        end
    end

    local maxMemberAge = WhoDat.peerMemberMaxAgeSeconds or 180
    for normalizedName, entry in pairs(WhoDat.peerChannelMembers) do
        local seenAt = entry and entry.seenAt or 0
        if now - seenAt > maxMemberAge then
            WhoDat.peerChannelMembers[normalizedName] = nil
            WhoDat.peerFactionCache[normalizedName] = nil
            WhoDat.peerAddonConfirmed[normalizedName] = nil
        end
    end
end

local function ScanChannelMembers(channelId, channelName, targetNormalizedName)
    local numericChannelId = tonumber(channelId)
    if not numericChannelId or numericChannelId <= 0 then
        return nil
    end

    if type(ChannelRoster) == "function" then
        pcall(ChannelRoster, numericChannelId)
    end

    local probeLimit = GetChannelProbeLimit(numericChannelId)
    local nilBreakThreshold = WhoDat.channelNilBreakThreshold or 15
    local missCount = 0
    local foundMemberCount = 0
    local matchedSummaryData = nil

    for memberIndex = 1, probeLimit do
        local summaryData = GetChannelMemberSummaryData(numericChannelId, memberIndex)
        if summaryData and summaryData.name then
            missCount = 0
            foundMemberCount = foundMemberCount + 1
            StoreChannelSummaryData(channelName, summaryData)

            if targetNormalizedName and NormalizeName(summaryData.name) == targetNormalizedName then
                matchedSummaryData = GetCachedChannelSummaryData(summaryData.name)
                if matchedSummaryData and (HasMinimumInfo(matchedSummaryData) or matchedSummaryData.online == false) then
                    return matchedSummaryData
                end
            end
        else
            missCount = missCount + 1
            if missCount >= nilBreakThreshold then
                if foundMemberCount > 0 or memberIndex >= (nilBreakThreshold * 4) then
                    break
                end
            end
        end
    end

    return matchedSummaryData
end

local function RefreshChannelCache(playerName, force)
    PruneExpiredChannelCache()

    local cachedSummaryData = GetCachedChannelSummaryData(playerName)
    local now = GetTime()
    if not force and now - (WhoDat.lastChannelScanAt or 0) < WhoDat.channelScanIntervalSeconds then
        return cachedSummaryData
    end

    if type(GetChannelList) ~= "function" then
        return cachedSummaryData
    end

    if type(GetChannelMemberInfo) ~= "function" and type(GetChannelRosterInfo) ~= "function" then
        return cachedSummaryData
    end

    local channels = { GetChannelList() }
    if #channels == 0 then
        return cachedSummaryData
    end

    WhoDat.lastChannelScanAt = now

    local targetNormalizedName = NormalizeName(playerName)
    local matchedSummaryData = nil
    for index = 1, #channels, 2 do
        local channelId = channels[index]
        local channelName = channels[index + 1]
        local channelData = ScanChannelMembers(channelId, channelName, targetNormalizedName)
        if channelData then
            matchedSummaryData = channelData
            if HasMinimumInfo(channelData) or channelData.online == false then
                break
            end
        end
    end

    if not matchedSummaryData then
        matchedSummaryData = GetCachedChannelSummaryData(playerName)
    end

    DebugPrint(string.format("channel scan complete; cache entries: %d", GetTableCount(WhoDat.channelCache)))
    return matchedSummaryData
end

local function GetChannelSummaryByName(playerName)
    local cachedSummaryData = GetCachedChannelSummaryData(playerName)
    if cachedSummaryData and (HasMinimumInfo(cachedSummaryData) or cachedSummaryData.online == false) then
        return cachedSummaryData
    end

    return RefreshChannelCache(playerName, false) or cachedSummaryData
end

local function PruneExpiredChatPresence()
    local now = GetTime()
    for normalizedName, entry in pairs(WhoDat.recentChatPresence) do
        if now - (entry.seenAt or 0) > WhoDat.chatPresenceMaxAgeSeconds then
            WhoDat.recentChatPresence[normalizedName] = nil
        end
    end
end

local function TrackChatPresence(name, source)
    local normalizedName = NormalizeName(name)
    if not normalizedName then
        return
    end

    WhoDat.recentChatPresence[normalizedName] = {
        name = string.match(name, "^([^%-]+)") or name,
        seenAt = GetTime(),
        source = source or "chat",
    }
end

local function GetRecentChatSummaryByName(playerName)
    PruneExpiredChatPresence()

    local normalizedName = NormalizeName(playerName)
    if not normalizedName then
        return nil
    end

    local entry = WhoDat.recentChatPresence[normalizedName]
    if not entry then
        return nil
    end

    return {
        name = entry.name or playerName,
        online = true,
        source = entry.source or "chat",
        extra = "seen in chat recently",
    }
end

local function IsOfflineStateReliable(summaryData, pending)
    if not summaryData or summaryData.online ~= false then
        return false
    end

    if HasMinimumInfo(summaryData) then
        return true
    end

    local source = summaryData.source or ""
    if string.find(source, "chat", 1, true) or string.find(source, "channel", 1, true) then
        return false
    end

    if string.find(source, "friends", 1, true) then
        return not (pending and pending.addedTemporarily)
    end

    if string.find(source, "guild", 1, true)
        or string.find(source, "raid", 1, true)
        or string.find(source, "party", 1, true)
        or string.find(source, "self", 1, true) then
        return true
    end

    return false
end

local function GetFriendSummaryData(requestName, index)
    local name, level, class, area, connected, status, note = GetFriendInfo(index)

    if connected == 1 or connected == true then
        connected = true
    else
        connected = false
    end

    local summaryData = {
        name = name or requestName or UNKNOWN,
        level = level,
        class = class,
        area = area,
        online = connected,
        status = status,
        note = note,
        source = "friends",
    }

    if name and name ~= "" then
        RecordMethodDiagnostic("friends:GetFriendInfo", true, "ok", summaryData.name)
    else
        RecordMethodDiagnostic("friends:GetFriendInfo", false, "no_name", summaryData.name)
    end

    return summaryData
end

local function GetFriendOfflineHintSummaryData(requestName, index)
    local name, level, class, area, connected, status, note = GetFriendInfo(index)
    local isOnline = connected == 1 or connected == true
    if isOnline then
        return nil
    end

    return {
        name = name or requestName or UNKNOWN,
        level = level,
        class = class,
        area = area,
        online = false,
        status = status,
        note = note,
        source = "friends",
    }
end

local function GetSelfSummaryByName(playerName)
    local ownName = UnitName("player")
    if not ownName then

        return nil
    end

    if NormalizeName(ownName) ~= NormalizeName(playerName) then

        return nil
    end

    local classLocalized = select(1, UnitClass("player"))
    local raceLocalized = select(1, UnitRace("player"))
    local factionName = GetFactionFromUnit("player") or InferFactionFromRace(raceLocalized)
    local summaryData = {
        name = ownName,
        level = UnitLevel("player"),
        class = classLocalized,
        race = raceLocalized,
        faction = factionName,
        area = GetRealZoneText(),
        online = true,
        source = "self",
    }

    return summaryData
end

local function GetRaidSummaryByName(playerName)
    if not UnitInRaid("player") then

        return nil
    end

    local lookup = NormalizeName(playerName)
    local totalRaid = GetNumRaidMembers() or 0
    for i = 1, totalRaid do
        local name, _, subgroup, level, class, _, zone, online, isDead = GetRaidRosterInfo(i)
        if name and NormalizeName(name) == lookup then
            local unit = "raid" .. i
            local raceLocalized = nil
            local factionName = nil
            if UnitExists(unit) then
                raceLocalized = select(1, UnitRace(unit))
                factionName = GetFactionFromUnit(unit) or InferFactionFromRace(raceLocalized)
            end

            local extra = nil
            if subgroup and subgroup > 0 then
                extra = "group: " .. subgroup
            end

            local status = nil
            if isDead then
                status = "dead"
            end

            local summaryData = {
                name = name,
                level = level,
                class = class,
                race = raceLocalized,
                faction = factionName,
                area = zone,
                online = online,
                status = status,
                source = "raid",
                extra = extra,
            }

            return summaryData
        end
    end

    return nil
end

local function GetPartySummaryByName(playerName)
    if (GetNumPartyMembers() or 0) == 0 then

        return nil
    end

    local lookup = NormalizeName(playerName)
    for i = 1, 4 do
        local unit = "party" .. i
        if UnitExists(unit) then
            local name = UnitName(unit)
            if name and NormalizeName(name) == lookup then
                local classLocalized = select(1, UnitClass(unit))
                local raceLocalized = select(1, UnitRace(unit))
                local factionName = GetFactionFromUnit(unit) or InferFactionFromRace(raceLocalized)
                local status = nil
                if UnitIsDeadOrGhost(unit) then
                    status = "dead"
                end

                local summaryData = {
                    name = name,
                    level = UnitLevel(unit),
                    class = classLocalized,
                    race = raceLocalized,
                    faction = factionName,
                    online = UnitIsConnected(unit),
                    status = status,
                    source = "party",
                    extra = "zone unavailable in party roster",
                }

                return summaryData
            end
        end
    end

    return nil
end

local function NormalizeGuildStatus(status)
    if type(status) == "number" then
        if status == 1 then
            return CHAT_FLAG_AFK or "AFK"
        elseif status == 2 then
            return CHAT_FLAG_DND or "DND"
        end
        return nil
    end

    return status
end

local function GetGuildSummaryByName(playerName)
    if not IsInGuild() then
        return nil
    end

    local now = GetTime()
    if now - (WhoDat.lastGuildRosterRefreshAt or 0) >= WhoDat.guildRosterRefreshIntervalSeconds then
        GuildRoster()
        WhoDat.lastGuildRosterRefreshAt = now
    end

    local lookup = NormalizeName(playerName)
    local totalMembers = GetNumGuildMembers(true)
    if not totalMembers or totalMembers <= 0 then
        totalMembers = GetNumGuildMembers() or 0
    end

    if totalMembers <= 0 then
        return nil
    end

    for i = 1, totalMembers do
        local name, rank, _, level, class, zone, publicNote, _, online, status = GetGuildRosterInfo(i)
        if name and NormalizeName(name) == lookup then
            local extra = nil
            if rank and rank ~= "" then
                extra = "rank: " .. rank
            end

            local summaryData = {
                name = name,
                level = level,
                class = class,
                area = zone,
                online = online,
                status = NormalizeGuildStatus(status),
                note = publicNote,
                source = "guild",
                extra = extra,
            }

            return summaryData
        end
    end

    return nil
end

local function GetDirectSummary(playerName)
    local selfData = GetSelfSummaryByName(playerName)
    if selfData then
        return selfData
    end

    local raidData = GetRaidSummaryByName(playerName)
    if raidData then
        return raidData
    end

    local partyData = GetPartySummaryByName(playerName)
    if partyData then
        return partyData
    end

    local guildData = GetGuildSummaryByName(playerName)
    if guildData then
        return guildData
    end

    return nil
end

local function HasPendingLookups()
    for _ in pairs(WhoDat.pending) do
        return true
    end

    return false
end

local function SetLookupTicker(enabled)
    if enabled then
        WhoDat:SetScript("OnUpdate", WhoDat.OnUpdate)
    else
        WhoDat:SetScript("OnUpdate", nil)
        WhoDat.scanElapsed = 0
    end
end

local function BuildPeerLookupSummaryByName(targetName)
    local directData = GetDirectSummary(targetName)
    local channelData = GetChannelSummaryByName(targetName)
    local chatData = GetRecentChatSummaryByName(targetName)
    local mergedData = MergeSummaryData(chatData, MergeSummaryData(directData, channelData))

    if not HasAnySummaryInfo(mergedData) then
        return nil
    end

    mergedData.name = mergedData.name or targetName
    mergedData.source = "peer:proxy"
    return mergedData
end

CompleteLookupFromSummary = function(normalizedName, pending, summaryData, completionReason)
    local finalData = summaryData or pending.partialData or pending.prefillData or {
        name = pending.requestName,
        online = true,
        source = "peer",
    }

    IncrementSummaryCounter("lookupsCompleted", 1)
    IncrementSummaryCounter("completedWithoutFriend", 1)
    RecordMethodDiagnostic("lookup:complete", true, completionReason or "peer", pending and pending.requestName)
    AddDebugEvent(string.format("lookup #%d completed via %s for %s", pending.lookupId or 0, completionReason or "peer", pending.requestName))

    ChatPrint(BuildSummary(finalData))

    if pending.addedTemporarily then
        local friendIndex = GetFriendIndexByName(pending.requestName)
        if friendIndex then
            local friendName = GetFriendInfo(friendIndex)
            QueueFriendSystemMessageSuppression(friendName or pending.requestName, "remove")
            RemoveFriend(friendIndex)
        end
    end

    WhoDat.pending[normalizedName] = nil
    if not HasPendingLookups() then
        SetLookupTicker(false)
    end
end

local function HandlePeerLookupRequest(senderName, requestId, targetName)
    if type(senderName) ~= "string" or senderName == "" then
        return
    end

    AddDebugEvent(string.format("peer request received from %s for %s", senderName, targetName or "unknown"))
    PeerDebugPrint(string.format(
        "request received from %s for %s requestId=%s",
        senderName,
        targetName or "unknown",
        requestId or "?"
    ))

    local responseData = BuildPeerLookupSummaryByName(targetName)
    if not responseData then
        AddDebugEvent(string.format("peer response skipped to %s for %s (no local data)", senderName, targetName or "unknown"))
        PeerDebugPrint(string.format(
            "response skipped to %s for %s requestId=%s (no local data)",
            senderName,
            targetName or "unknown",
            requestId or "?"
        ))
        return
    end

    local responseMessage = BuildPeerLookupResponseMessage(requestId, targetName, responseData)
    PeerDebugPrint(string.format(
        "sending response to %s for %s requestId=%s %s",
        senderName,
        targetName or "unknown",
        requestId or "?",
        FormatPeerSummaryDataForDebug(responseData)
    ))
    local sent, sendError = SendWhoDatAddonMessage(responseMessage, "WHISPER", senderName)
    if sent then
        AddDebugEvent(string.format("peer response sent to %s for %s", senderName, targetName))
        PeerDebugPrint(string.format("response sent to %s requestId=%s", senderName, requestId or "?"))
    else
        AddDebugEvent(string.format("peer response failed to %s for %s (%s)", senderName, targetName, sendError or "unknown"))
        PeerDebugPrint(string.format("response send failed to %s requestId=%s (%s)", senderName, requestId or "?", sendError or "unknown"))
    end
end

local function HandlePeerLookupResponse(senderName, requestId, targetName, summaryData)
    local normalizedTarget = NormalizeName(targetName)
    if not normalizedTarget then
        return
    end

    local pending = WhoDat.pending[normalizedTarget]
    if not pending or not pending.peerRequestId then
        return
    end

    if pending.peerRequestId ~= requestId then
        return
    end

    AddDebugEvent(string.format("peer response received from %s for %s", senderName, targetName))
    PeerDebugPrint(string.format(
        "response received from %s for %s requestId=%s %s",
        senderName,
        targetName,
        requestId or "?",
        FormatPeerSummaryDataForDebug(summaryData)
    ))

    pending.peerResponderName = senderName

    local mergedData = MergeSummaryData(summaryData, MergeSummaryData(pending.partialData, pending.prefillData))
    if HasMinimumInfo(mergedData) or HasPartialIdentityInfo(mergedData) or IsOfflineStateReliable(mergedData, pending) then
        CompleteLookupFromSummary(normalizedTarget, pending, mergedData, "peer")
        return
    end

    pending.partialData = mergedData
end

RequestPeerCrossFactionLookup = function(normalizedName, pending, fallbackData)
    if not normalizedName or not pending then
        return false, "invalid"
    end

    local now = GetTime()
    local lastLookupAt = WhoDat.lastPeerLookupAt[normalizedName] or 0
    if now - lastLookupAt < WhoDat.peerLookupCooldownSeconds then
        return false, "cooldown"
    end

    local targetFaction = nil
    local data = fallbackData or pending.prefillData or pending.partialData
    if data then
        targetFaction = NormalizeFaction(data.faction) or InferFactionFromRace(data.race)
    end

    local myFaction = GetPlayerFaction()
    if targetFaction and myFaction and targetFaction == myFaction then
        return false, "same_faction_target"
    end

    if not targetFaction and pending.lastFailureReason == "wrong_faction" and myFaction then
        if myFaction == "Horde" then
            targetFaction = "Alliance"
        elseif myFaction == "Alliance" then
            targetFaction = "Horde"
        end
    end

    if not targetFaction and myFaction then
        if myFaction == "Horde" then
            targetFaction = "Alliance"
        elseif myFaction == "Alliance" then
            targetFaction = "Horde"
        end

        if targetFaction then
            AddDebugEvent(string.format(
                "lookup #%d inferred opposite-faction peer target %s for %s",
                pending.lookupId or 0,
                targetFaction,
                pending.requestName
            ))
        end
    end

    if not targetFaction then
        return false, "unknown_target_faction"
    end

    local channelId, channelName = EnsureWhodatChannelJoined(false)
    if channelId then
        SyncWhodatPeerMembership(channelId, channelName)
    end

    local peerName = ChooseRandomPeerByFaction(targetFaction)
    if not peerName then
        return false, "no_peer_data"
    end

    local requestId = BuildPeerRequestId()
    local requestMessage = BuildPeerLookupRequestMessage(requestId, pending.requestName)
    PeerDebugPrint(string.format(
        "requesting lookup from %s for %s requestId=%s targetFaction=%s",
        peerName,
        pending.requestName,
        requestId,
        targetFaction or "?"
    ))
    local sent, sendError = SendWhoDatAddonMessage(requestMessage, "WHISPER", peerName)
    if not sent then
        AddDebugEvent(string.format(
            "lookup #%d peer request send failed to %s for %s (%s)",
            pending.lookupId or 0,
            peerName,
            pending.requestName,
            sendError or "unknown"
        ))
        PeerDebugPrint(string.format(
            "request send failed to %s for %s requestId=%s (%s)",
            peerName,
            pending.requestName,
            requestId,
            sendError or "unknown"
        ))
        return false, "send_failed"
    end

    pending.peerRequestId = requestId
    pending.peerRequestedAt = now
    pending.peerTargetName = peerName
    WhoDat.lastPeerLookupAt[normalizedName] = now

    AddDebugEvent(string.format(
        "lookup #%d peer request sent to %s for %s (%s)",
        pending.lookupId or 0,
        peerName,
        pending.requestName,
        targetFaction
    ))
    PeerDebugPrint(string.format(
        "request sent to %s for %s requestId=%s targetFaction=%s",
        peerName,
        pending.requestName,
        requestId,
        targetFaction or "?"
    ))

    return true, nil
end

local function GetMostRecentPendingLookup(maxAgeSeconds)
    local now = GetTime()
    local bestNormalizedName = nil
    local bestPending = nil
    local bestAge = nil

    for normalizedName, pending in pairs(WhoDat.pending) do
        if pending.addedTemporarily then
            local startedAt = pending.friendAddRequestedAt or pending.startedAt or now
            local age = now - startedAt
            if (not maxAgeSeconds or age <= maxAgeSeconds) and (not bestAge or age < bestAge) then
                bestNormalizedName = normalizedName
                bestPending = pending
                bestAge = age
            end
        end
    end

    return bestNormalizedName, bestPending
end

local function CompletePendingFailure(normalizedName, pending, reason)
    local fallbackData = CloneSummaryData(pending.partialData or pending.prefillData)
    local displayName = pending.requestName
    if fallbackData and fallbackData.name and fallbackData.name ~= "" then
        displayName = fallbackData.name
    end

    if reason == "wrong_faction" then
        local peerLookupSent, peerLookupReason = RequestPeerCrossFactionLookup(normalizedName, pending, fallbackData)
        if peerLookupSent then
            return
        end

        if peerLookupReason == "no_peer_data" then
            ChatPrint("There are currently not enough data to fetch player info.")
            WhoDat.pending[normalizedName] = nil
            if not HasPendingLookups() then
                SetLookupTicker(false)
            end
            return
        end

        if fallbackData and fallbackData.online == true then
            if HasPartialIdentityInfo(fallbackData) then
                ChatPrint(BuildSummary(fallbackData))
            else
                ChatPrint(string.format("%s is online, but cross-faction details are unavailable on this server.", displayName))
            end
        else
            ChatPrint(string.format("%s: cross-faction friend lookup is blocked on this server.", displayName))
        end
    elseif reason == "list_full" then
        if fallbackData and (HasMinimumInfo(fallbackData) or fallbackData.online == true) then
            ChatPrint(BuildSummary(fallbackData))
        end

        ChatPrint(string.format("%s: friend list is full, cannot run temporary friend lookup.", displayName))
    elseif reason == "already_friend" then
        local friendIndex = GetFriendIndexByName(displayName)
        if friendIndex then
            ChatPrint(BuildSummary(GetFriendSummaryData(displayName, friendIndex)))
        elseif fallbackData and fallbackData.online == true then
            ChatPrint(string.format("%s is online, but level/class/location are unavailable.", displayName))
        else
            ChatPrint(string.format("%s: already on friend list, but details are unavailable.", displayName))
        end
    elseif reason == "not_found" then
        if fallbackData and fallbackData.online == true then
            ChatPrint(string.format("%s is online, but server rejected friend lookup: player not found.", displayName))
        else
            ChatPrint(string.format("%s: player not found for friend lookup.", displayName))
        end
    elseif reason == "self" then
        ChatPrint("You cannot query yourself through temporary friend lookup.")
    else
        if fallbackData and fallbackData.online == true then
            ChatPrint(string.format("%s is online, but friend lookup failed on this server.", displayName))
        else
            ChatPrint(string.format("%s: friend lookup failed on this server.", displayName))
        end
    end

    WhoDat.pending[normalizedName] = nil
    if not HasPendingLookups() then
        SetLookupTicker(false)
    end
end

HandleFriendLookupFailureMessage = function(message)
    local reason, failureName = MatchFriendAddFailureReason(message)
    if not reason then
        return false
    end

    local normalizedName = failureName and NormalizeName(failureName) or nil
    local pending = normalizedName and WhoDat.pending[normalizedName] or nil

    if not pending then
        normalizedName, pending = GetMostRecentPendingLookup(WhoDat.addFriendFailureFallbackSeconds + 1.5)
    end

    if not pending or not normalizedName then
        return false
    end

    pending.lastFailureReason = reason

    DebugPrint(string.format("friend lookup failed early (%s): %s", reason, message))
    AddDebugEvent(string.format(
        "lookup #%d friend failure for %s: %s",
        pending.lookupId or 0,
        pending.requestName or "unknown",
        reason
    ))

    if reason == "wrong_faction" then
        pending.addedTemporarily = false
        CompletePendingFailure(normalizedName, pending, reason)
        return true
    end

    CompletePendingFailure(normalizedName, pending, reason)
    return true
end

local function GetMergedPendingSummaryData(pending, friendIndex)
    local friendData = GetFriendSummaryData(pending.requestName, friendIndex)
    if pending.prefillData then
        friendData = MergeSummaryData(friendData, pending.prefillData)
    end

    return friendData
end

local function CompleteLookup(normalizedName, pending, friendIndex, summaryData)
    local finalData = summaryData or GetMergedPendingSummaryData(pending, friendIndex)

    IncrementSummaryCounter("lookupsCompleted", 1)
    IncrementSummaryCounter("completedFriend", 1)
    RecordMethodDiagnostic("lookup:complete", true, "friend", pending and pending.requestName)
    AddDebugEvent(string.format("lookup #%d completed via friend for %s", pending.lookupId or 0, pending.requestName))

    ChatPrint(BuildSummary(finalData))

    if pending.addedTemporarily then
        local friendName = GetFriendInfo(friendIndex)
        QueueFriendSystemMessageSuppression(friendName or pending.requestName, "remove")
        RemoveFriend(friendIndex)
    end

    WhoDat.pending[normalizedName] = nil
    if not HasPendingLookups() then
        SetLookupTicker(false)
    end
end

local function TryCompleteLookup(normalizedName, pending, now)
    local friendIndex = GetFriendIndexByName(pending.requestName)
    if not friendIndex then
        return false
    end

    local mergedData = GetMergedPendingSummaryData(pending, friendIndex)
    if IsOfflineStateReliable(mergedData, pending) then
        CompleteLookup(normalizedName, pending, friendIndex, mergedData)
        return true
    end

    if HasMinimumInfo(mergedData) then
        CompleteLookup(normalizedName, pending, friendIndex, mergedData)
        return true
    end

    pending.partialData = mergedData

    if not pending.friendSeenAt then
        pending.friendSeenAt = now
        return false
    end

    if now - pending.friendSeenAt >= WhoDat.friendDataGraceSeconds then
        if HasMinimumInfo(mergedData) or IsOfflineStateReliable(mergedData, pending) then
            CompleteLookup(normalizedName, pending, friendIndex, mergedData)
            return true
        end
    end

    return false
end

local function StartLookup(playerName)
    local normalizedName = NormalizeName(playerName)
    if not normalizedName then
        return
    end

    if WhoDat.pending[normalizedName] then
        ChatPrint(string.format("%s: lookup already in progress.", playerName))
        AddDebugEvent(string.format("lookup skipped for %s (already pending)", playerName))
        return
    end

    local existingFriendIndex = GetFriendIndexByName(playerName)
    if existingFriendIndex then
        IncrementSummaryCounter("prefillCompleted", 1)
        RecordMethodDiagnostic("lookup:complete", true, "already_friend", playerName)
        AddDebugEvent(string.format("lookup immediate friend hit for %s", playerName))
        ChatPrint(BuildSummary(GetFriendSummaryData(playerName, existingFriendIndex)))
        return
    end

    local directData = GetDirectSummary(playerName)
    local channelData = GetChannelSummaryByName(playerName)
    local chatData = GetRecentChatSummaryByName(playerName)

    local prefillData = MergeSummaryData(chatData, MergeSummaryData(directData, channelData))
    if not HasAnySummaryInfo(prefillData) then
        prefillData = nil
    end

    if prefillData and IsOfflineStateReliable(prefillData, nil) then
        IncrementSummaryCounter("prefillCompleted", 1)
        RecordMethodDiagnostic("lookup:complete", true, "prefill_offline", playerName)
        AddDebugEvent(string.format("lookup prefill offline for %s", playerName))
        ChatPrint(BuildSummary(prefillData))
        return
    end

    if HasMinimumInfo(prefillData) then
        IncrementSummaryCounter("prefillCompleted", 1)
        RecordMethodDiagnostic("lookup:complete", true, "prefill_minimum", playerName)
        AddDebugEvent(string.format("lookup prefill complete for %s", playerName))
        ChatPrint(BuildSummary(prefillData))
        return
    end

    local totalFriends = GetNumFriends() or 0
    if MAX_FRIENDS and totalFriends >= MAX_FRIENDS then
        if prefillData then
            IncrementSummaryCounter("prefillCompleted", 1)
            AddDebugEvent(string.format("lookup prefill used for %s (friend list full)", playerName))
            ChatPrint(BuildSummary(prefillData))
            ChatPrint(string.format("%s: friend list is full, could not enrich with temporary friend lookup.", playerName))
        else
            AddDebugEvent(string.format("lookup failed for %s (friend list full)", playerName))
            ChatPrint(string.format("%s: friend list is full, cannot do temporary lookup.", playerName))
        end
        return
    end

    WhoDat.lookupSequence = (WhoDat.lookupSequence or 0) + 1
    local lookupId = WhoDat.lookupSequence

    WhoDat.pending[normalizedName] = {
        lookupId = lookupId,
        requestName = playerName,
        startedAt = GetTime(),
        friendAddRequestedAt = GetTime(),
        addedTemporarily = true,
        prefillData = prefillData,
        peerAttempted = false,
        timeoutLogged = false,
        lastFailureReason = nil,
    }

    IncrementSummaryCounter("lookupsStarted", 1)
    RecordMethodDiagnostic("lookup:start", true, "pending", playerName)
    AddDebugEvent(string.format("lookup #%d started for %s", lookupId, playerName))

    QueueFriendSystemMessageSuppression(playerName, "add")
    AddFriend(playerName)
    ShowFriends()
    SetLookupTicker(true)
end

function WhoDat:OnUpdate(elapsed)
    self.scanElapsed = self.scanElapsed + elapsed
    if self.scanElapsed < self.scanInterval then
        return
    end

    self.scanElapsed = 0

    local now = GetTime()

    for normalizedName, pending in pairs(self.pending) do
        local fastFallbackHandled = false
        if pending.addedTemporarily and not pending.friendSeenAt and pending.prefillData and pending.prefillData.online == true then
            local source = pending.prefillData.source or ""
            local age = now - (pending.friendAddRequestedAt or pending.startedAt)
            local fastFallbackAge = WhoDat.addFriendFailureFallbackSeconds
            if (string.find(source, "chat", 1, true) or string.find(source, "channel", 1, true))
                and age >= fastFallbackAge then
                if HasPartialIdentityInfo(pending.prefillData) then
                    ChatPrint(BuildSummary(pending.prefillData))
                else
                    RegisterLookupTimeout(pending, "fast_fallback_missing_identity")
                    ChatPrint(string.format(
                        "%s is online, but cross-faction details are unavailable on this server.",
                        pending.prefillData.name or pending.requestName
                    ))
                end

                self.pending[normalizedName] = nil
                fastFallbackHandled = true
            end
        end

        local timedOut = now - pending.startedAt >= self.lookupTimeoutSeconds
        if timedOut and pending.peerRequestedAt then
            local peerWaitRemaining = (pending.peerRequestedAt + WhoDat.peerLookupResponseWaitSeconds) - now
            if peerWaitRemaining > 0 then
                timedOut = false
            end
        end

        if (not fastFallbackHandled)
            and pending.addedTemporarily
            and (not pending.peerRequestedAt)
            and (not pending.peerAttempted) then
            local friendIndex = GetFriendIndexByName(pending.requestName)
            if friendIndex then
                local offlineHintData = GetFriendOfflineHintSummaryData(pending.requestName, friendIndex)
                if offlineHintData then
                    pending.peerAttempted = true
                    local peerLookupSent, peerLookupReason = RequestPeerCrossFactionLookup(normalizedName, pending, offlineHintData)
                    if peerLookupSent then
                        AddDebugEvent(string.format(
                            "lookup #%d peer request triggered by offline friend hint for %s",
                            pending.lookupId or 0,
                            pending.requestName
                        ))
                        PeerDebugPrint(string.format(
                            "request triggered by offline friend hint for %s lookupId=%s",
                            pending.requestName,
                            tostring(pending.lookupId or "?")
                        ))
                        timedOut = false
                    else
                        AddDebugEvent(string.format(
                            "lookup #%d peer offline-hint request skipped for %s (%s)",
                            pending.lookupId or 0,
                            pending.requestName,
                            peerLookupReason or "unknown"
                        ))
                        PeerDebugPrint(string.format(
                            "offline-hint request skipped for %s lookupId=%s (%s)",
                            pending.requestName,
                            tostring(pending.lookupId or "?"),
                            peerLookupReason or "unknown"
                        ))
                    end
                end
            end
        end

        if (not fastFallbackHandled)
            and (not pending.peerRequestedAt)
            and (not pending.peerAttempted)
            and now - pending.startedAt >= (WhoDat.addFriendFailureFallbackSeconds or 3.5) then
            local peerData = pending.partialData or pending.prefillData
            local peerTargetFaction = peerData and (NormalizeFaction(peerData.faction) or InferFactionFromRace(peerData.race)) or nil
            local myFaction = GetPlayerFaction()

            local shouldTryPeer = false
            if pending.lastFailureReason == "wrong_faction" then
                shouldTryPeer = true
            elseif peerTargetFaction and myFaction and peerTargetFaction ~= myFaction then
                shouldTryPeer = true
            end

            if shouldTryPeer then
                pending.peerAttempted = true
                local peerLookupSent, peerLookupReason = RequestPeerCrossFactionLookup(normalizedName, pending, peerData)
                if peerLookupSent then
                    timedOut = false
                else
                    AddDebugEvent(string.format(
                        "lookup #%d peer request skipped for %s (%s)",
                        pending.lookupId or 0,
                        pending.requestName,
                        peerLookupReason or "unknown"
                    ))
                    PeerDebugPrint(string.format(
                        "request skipped for %s lookupId=%s (%s)",
                        pending.requestName,
                        tostring(pending.lookupId or "?"),
                        peerLookupReason or "unknown"
                    ))
                end
            end
        end

        if (not fastFallbackHandled) and not TryCompleteLookup(normalizedName, pending, now) and timedOut then
            local fallbackData = pending.partialData or pending.prefillData
            local printedSummary = false

            if fallbackData then
                if HasMinimumInfo(fallbackData) or IsOfflineStateReliable(fallbackData, pending) then
                    ChatPrint(BuildSummary(fallbackData))
                    printedSummary = true
                elseif fallbackData.online == true then
                    if HasPartialIdentityInfo(fallbackData) then
                        ChatPrint(BuildSummary(fallbackData))
                    else
                        ChatPrint(string.format("%s is online, but level/class/location are unavailable.", fallbackData.name or pending.requestName))
                    end
                    printedSummary = true
                end
            end

            if not printedSummary then
                RegisterLookupTimeout(pending, "lookup_timeout_no_data")
                ChatPrint(string.format("%s: lookup timed out; player not found or server returned no data.", pending.requestName))
            end

            if pending.addedTemporarily then
                local friendIndex = GetFriendIndexByName(pending.requestName)
                if friendIndex then
                    local friendName = GetFriendInfo(friendIndex)
                    QueueFriendSystemMessageSuppression(friendName or pending.requestName, "remove")
                    RemoveFriend(friendIndex)
                end
            end

            self.pending[normalizedName] = nil
        end
    end

    if not HasPendingLookups() then
        SetLookupTicker(false)
    end
end

function WhoDat:HandleFriendListUpdate()
    local now = GetTime()

    for normalizedName, pending in pairs(self.pending) do
        TryCompleteLookup(normalizedName, pending, now)
    end

    if not HasPendingLookups() then
        SetLookupTicker(false)
    end
end

local function ParseSlashName(message)
    local trimmed = string.match(message or "", "^%s*(.-)%s*$")
    if not trimmed or trimmed == "" then
        return nil
    end

    return string.match(trimmed, "^([^%s]+)")
end

function WhoDat:HandleAddonMessageEvent(prefix, message, distribution, sender)
    if prefix ~= WhoDat.addonCommPrefix then
        return
    end

    if type(sender) ~= "string" or sender == "" then
        return
    end

    if type(distribution) ~= "string" or distribution ~= "WHISPER" then
        return
    end

    TrackPeerChannelMember(sender)

    local normalizedSender = NormalizeName(sender)
    if normalizedSender then
        WhoDat.peerAddonConfirmed[normalizedSender] = GetTime()
    end

    local faction = ParsePeerFactionMessage(message)
    if faction then
        if SetPeerFaction(sender, faction, "addon:faction") then
            AddDebugEvent(string.format("peer faction learned from %s (%s)", sender, faction))
        end
        return
    end

    local requestId, targetName = ParsePeerLookupRequestMessage(message)
    if requestId then
        HandlePeerLookupRequest(sender, requestId, targetName)
        return
    end

    local responseRequestId, responseTargetName, summaryData = ParsePeerLookupResponseMessage(message)
    if responseRequestId then
        HandlePeerLookupResponse(sender, responseRequestId, responseTargetName, summaryData)
    end
end

function WhoDat:HandleChatMessageEvent(event, ...)
    local messageText = select(1, ...)
    local sender = select(2, ...)
    if type(sender) ~= "string" or sender == "" then
        return
    end

    local source = "chat"
    if event == "CHAT_MSG_CHANNEL" then
        local channelName = select(4, ...)
        if type(channelName) ~= "string" or channelName == "" then
            channelName = select(9, ...)
        end

        if type(channelName) == "string" and channelName ~= "" then
            source = "chat:" .. channelName

            if IsWhodatChannelName(channelName) then
                TrackPeerChannelMember(sender)

                local senderNormalized = NormalizeName(sender)
                local selfNormalized = NormalizeName(UnitName("player") or "")

                local helloFaction = ParseWhodatChannelHelloMessage(messageText)
                if helloFaction then
                    if senderNormalized and selfNormalized and senderNormalized == selfNormalized then
                        local echoAt = GetTime()
                        WhoDat.lastPeerChannelHelloAt = echoAt
                        WhoDat.lastPeerChannelHelloEchoAt = echoAt
                        WhoDat.peerStartupHelloDone = true
                        WhoDat.peerStartupHelloDeadlineAt = 0
                        AddDebugEvent(string.format("peer hello echo confirmed (%s)", helloFaction))
                    else
                        if senderNormalized then
                            WhoDat.peerAddonConfirmed[senderNormalized] = GetTime()
                        end
                        if SetPeerFaction(sender, helloFaction, "channel:hello") then
                            AddDebugEvent(string.format("peer hello from %s (%s)", sender, helloFaction))
                        end
                    end
                end

            end
        end
    end

    TrackChatPresence(sender, source)

end

function WhoDat:HandleChannelRosterUpdate(channelId)
    local numericChannelId = tonumber(channelId)
    if not numericChannelId or numericChannelId <= 0 then
        return
    end

    local channelName = nil
    if type(GetChannelList) == "function" then
        local channels = { GetChannelList() }
        for index = 1, #channels, 2 do
            if tonumber(channels[index]) == numericChannelId then
                channelName = channels[index + 1]
                break
            end
        end
    end

    ScanChannelMembers(numericChannelId, channelName)

    if IsWhodatChannelName(channelName) then
        SyncWhodatPeerMembership(numericChannelId, channelName)
        TryStartupWhodatHello(numericChannelId, channelName)
    end
end

function WhoDat:HandleChannelSystemEvent(event, ...)
    local shouldRefresh = false

    if event == "CHANNEL_UI_UPDATE" then
        shouldRefresh = true
    elseif event == "CHAT_MSG_CHANNEL_NOTICE" then
        for i = 1, select("#", ...) do
            local value = select(i, ...)
            if type(value) == "string" and IsWhodatChannelName(value) then
                shouldRefresh = true
                break
            end
        end

    end

    if not shouldRefresh then
        return
    end

    local channelId, channelName = EnsureWhodatChannelJoined(false)
    if channelId then
        SyncWhodatPeerMembership(channelId, channelName)
        TryStartupWhodatHello(channelId, channelName)
    end
end

function WhoDat:HandleSlashCommand(message)
    local playerName = ParseSlashName(message)
    if not playerName then
        ChatPrint("Usage: /whodat PlayerName")
        ChatPrint("       /whodat hello")
        ChatPrint("       /whodat debug on|off|status|log|savedlog|clearlog|reset")
        return
    end

    local normalizedArg = NormalizeName(playerName)
    if normalizedArg == "hello" then
        local channelId, channelName = EnsureWhodatChannelJoined(true)
        if not channelId then
            ChatPrint(string.format("Could not find or join %s.", WhoDat.peerChannelName))
            return
        end

        SyncWhodatPeerMembership(channelId, channelName)
        local sent, reason = SendWhodatChannelHello(channelId, channelName, true)
        if sent then
            self.peerStartupHelloDone = true
            self.peerStartupHelloDeadlineAt = 0
            local myFaction = GetPlayerFaction() or "Unknown"
            ChatPrint(string.format("Hello attempt sent to %s.", channelName or WhoDat.peerChannelName))
            ChatPrint(string.format("If WDHELLO is still not visible, type this manually once: /%d WDHELLO %s", channelId, myFaction))
        else
            ChatPrint(string.format("Could not send hello to %s (%s).", channelName or WhoDat.peerChannelName, reason or "unknown"))
        end
        return
    end

    if normalizedArg == "debug" then
        local debugArg = string.match(message or "", "^%s*%S+%s+(%S+)")
        debugArg = NormalizeName(debugArg)

        if debugArg == "on" then
            self.debugEnabled = true
            ChatPrint("Debug mode enabled.")
        elseif debugArg == "off" then
            self.debugEnabled = false
            ChatPrint("Debug mode disabled.")
        elseif debugArg == "log" then
            PrintDebugLog()
        elseif debugArg == "savedlog" then
            PrintPersistentDebugLog()
        elseif debugArg == "clearlog" then
            ClearPersistentDebugLog()
        elseif debugArg == "reset" then
            ResetDebugTracking()
            ChatPrint("Debug log counters reset.")
        else
            local alliancePeerCount, hordePeerCount = GetPeerFactionCacheCounts()
            local peerTotalCount = alliancePeerCount + hordePeerCount
            local persistentLogCount = type(self.persistentDebugEvents) == "table" and #self.persistentDebugEvents or 0
            local whodatChannelId = select(1, GetWhodatChannelInfo())
            local channelStatus = whodatChannelId and "joined" or "missing"
            local playerLevel = type(UnitLevel) == "function" and (UnitLevel("player") or 0) or 0
            local channelSendStatus = CanSendPeerChannelMessages()
                and "allowed"
                or string.format("locked(<%d):%d", self.peerChannelMinLevel or 8, playerLevel)
            local whisperSendStatus = CanSendPeerWhisperMessages()
                and "allowed"
                or string.format("locked(<%d):%d", self.peerWhisperMinLevel or 9, playerLevel)
            local sendStatus = string.format("channel:%s whisper:%s", channelSendStatus, whisperSendStatus)
            local helloStatus = "never"
            local lastHelloAt = self.lastPeerChannelHelloAt or 0
            if lastHelloAt > 0 then
                local ageSeconds = math.floor(GetTime() - lastHelloAt)
                if ageSeconds < 0 then
                    ageSeconds = 0
                end

                local lastEchoAt = self.lastPeerChannelHelloEchoAt or 0
                if lastEchoAt > 0 and lastEchoAt >= lastHelloAt then
                    helloStatus = string.format("echo:%ds", ageSeconds)
                else
                    helloStatus = string.format("unconfirmed:%ds", ageSeconds)
                end
            end
            if not self.peerStartupHelloDone then
                local startupDeadlineAt = self.peerStartupHelloDeadlineAt or 0
                if startupDeadlineAt > 0 then
                    local startupRemaining = math.ceil(startupDeadlineAt - GetTime())
                    if startupRemaining > 0 then
                        helloStatus = string.format("startup:%ds,%s", startupRemaining, helloStatus)
                    end
                end
            end
            local probeStatus = "idle"
            local lastProbeAt = self.lastPeerRosterProbeAt or 0
            if lastProbeAt > 0 then
                local probeCooldown = self.peerRosterProbeCooldownSeconds or 30
                local probeRemaining = math.ceil((lastProbeAt + probeCooldown) - GetTime())
                if probeRemaining > 0 then
                    probeStatus = string.format("cooldown:%ds", probeRemaining)
                else
                    probeStatus = "ready"
                end
            end
            ChatPrint(string.format(
                "Debug status: %s; filter api: %s; added-pattern: %s; removed-pattern: %s; channel-member-api: %s; channel-roster-api: %s; channel-cache: %d; chat-presence: %d; whodat-channel: %s; peer-send: %s; hello: %s; probe: %s; peers-alliance: %d/%d; peers-horde: %d/%d; peers-total: %d; savedlog: %d.",
                self.debugEnabled and "on" or "off",
                ChatFrameAddMessageEventFilter and "available" or "missing",
                FRIEND_ADDED_PATTERN and "ok" or "missing",
                FRIEND_REMOVED_PATTERN and "ok" or "missing",
                type(GetChannelMemberInfo) == "function" and "available" or "missing",
                type(GetChannelRosterInfo) == "function" and "available" or "missing",
                GetTableCount(self.channelCache),
                GetTableCount(self.recentChatPresence),
                channelStatus,
                sendStatus,
                helloStatus,
                probeStatus,
                alliancePeerCount,
                self.peerFactionMaxPerFaction or 32,
                hordePeerCount,
                self.peerFactionMaxPerFaction or 32,
                peerTotalCount,
                persistentLogCount
            ))
        end
        return
    end

    StartLookup(playerName)
end

function WhoDat:HandlePlayerLogin()
    WhoDatDB = WhoDatDB or {}
    WhoDatDB.channelCache = WhoDatDB.channelCache or {}
    WhoDatDB.persistentDebugEvents = WhoDatDB.persistentDebugEvents or {}

    local preLoginPersistentEvents = self.persistentDebugEvents
    self.persistentDebugEvents = WhoDatDB.persistentDebugEvents
    if type(preLoginPersistentEvents) == "table" and preLoginPersistentEvents ~= self.persistentDebugEvents then
        for _, entry in ipairs(preLoginPersistentEvents) do
            table.insert(self.persistentDebugEvents, entry)
        end
    end

    local persistentMax = self.persistentDebugLogMaxEvents or 500
    while #self.persistentDebugEvents > persistentMax do
        table.remove(self.persistentDebugEvents, 1)
    end

    self.channelCache = WhoDatDB.channelCache
    self.peerChannelMembers = {}
    self.peerFactionCache = {}
    self.peerAddonConfirmed = {}
    self.lastPeerChannelHelloAt = 0
    self.lastPeerChannelHelloEchoAt = 0
    self.lastPeerChannelHelloAttemptAt = 0
    self.peerStartupHelloDeadlineAt = GetTime() + (self.peerStartupHelloWindowSeconds or 45)
    self.peerStartupHelloDone = false
    self.lastPeerStartupHelloTryAt = 0
    self.lastPeerRosterProbeAt = 0
    self.lastPeerLookupAt = {}
    ResetDebugTracking()

    RegisterWhoDatAddonPrefix()

    local channelId, channelName = EnsureWhodatChannelJoined(true)
    if channelId then
        SyncWhodatPeerMembership(channelId, channelName)
        TryStartupWhodatHello(channelId, channelName)
    end

    if ChatFrameAddMessageEventFilter then
        ChatFrameAddMessageEventFilter("CHAT_MSG_SYSTEM", SystemMessageFilter)
    else
        ChatPrint("System message filter API missing; temporary friend add/remove lines may be visible.")
    end

    hooksecurefunc("SetItemRef", function(link)
        if not IsShiftKeyDown() then
            return
        end

        local playerName = ExtractNameFromPlayerLink(link)
        if playerName then
            TrackChatPresence(playerName, "chat:link")
            StartLookup(playerName)
        end
    end)

    SLASH_WHODAT1 = "/whodat"
    SLASH_WHODAT2 = "/whodatt"
    SlashCmdList["WHODAT"] = function(message)
        WhoDat:HandleSlashCommand(message)
    end

    SetLookupTicker(true)

    ChatPrint("Loaded. Shift-click a player name or use /whodat Name to query level/class/location.")
end

WhoDat:SetScript("OnEvent", function(self, event, ...)
    if event == "PLAYER_LOGIN" then
        self:HandlePlayerLogin()
    elseif event == "FRIENDLIST_UPDATE" then
        self:HandleFriendListUpdate()
    elseif event == "CHANNEL_UI_UPDATE" or event == "CHAT_MSG_CHANNEL_NOTICE" then
        self:HandleChannelSystemEvent(event, ...)
    elseif event == "CHAT_MSG_ADDON" then
        self:HandleAddonMessageEvent(...)
    elseif event == "CHANNEL_ROSTER_UPDATE" then
        self:HandleChannelRosterUpdate(...)
    elseif string.find(event, "^CHAT_MSG_") then
        self:HandleChatMessageEvent(event, ...)
    end
end)

WhoDat:RegisterEvent("PLAYER_LOGIN")
WhoDat:RegisterEvent("FRIENDLIST_UPDATE")
WhoDat:RegisterEvent("CHANNEL_UI_UPDATE")
WhoDat:RegisterEvent("CHAT_MSG_CHANNEL_NOTICE")
WhoDat:RegisterEvent("CHAT_MSG_ADDON")
WhoDat:RegisterEvent("CHANNEL_ROSTER_UPDATE")
WhoDat:RegisterEvent("CHAT_MSG_CHANNEL")
WhoDat:RegisterEvent("CHAT_MSG_SAY")
WhoDat:RegisterEvent("CHAT_MSG_YELL")
WhoDat:RegisterEvent("CHAT_MSG_GUILD")
WhoDat:RegisterEvent("CHAT_MSG_PARTY")
WhoDat:RegisterEvent("CHAT_MSG_RAID")
WhoDat:RegisterEvent("CHAT_MSG_WHISPER")
