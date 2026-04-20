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
WhoDat.addonCommPrefix = "WHODATX1"
WhoDat.addonLookupCooldownSeconds = 20
WhoDat.peerLookupEnabled = true
WhoDat.peerRequestCooldownSeconds = 20
WhoDat.peerRecentRequestMaxAgeSeconds = 60
WhoDat.peerResponseWaitSeconds = 5
WhoDat.peerChannelName = "whodatdata"
WhoDat.peerChannelHints = { "world", "lookingforgroup", "lfg" }
WhoDat.peerChannelJoinRetrySeconds = 10
WhoDat.peerPresenceStartupMaxWaitSeconds = 12
WhoDat.peerPresenceStartupRetrySeconds = 1
WhoDat.peerSeenMaxAgeSeconds = 600
WhoDat.peerPresencePingWaitSeconds = 3
WhoDat.guildRosterRefreshIntervalSeconds = 10
WhoDat.debugEnabled = false
WhoDat.debugLogMaxEvents = 50
WhoDat.lastChannelScanAt = 0
WhoDat.lastGuildRosterRefreshAt = 0
WhoDat.peerRequestSequence = 0
WhoDat.peerPresencePingSequence = 0
WhoDat.lastPeerChannelJoinAttemptAt = 0
WhoDat.lastKnownPeerChannelName = nil
WhoDat.lastConnectedPeerCount = 0
WhoDat.pending = {}
WhoDat.channelCache = {}
WhoDat.recentChatPresence = {}
WhoDat.lastAddonLookupAt = {}
WhoDat.lastPeerLookupAt = {}
WhoDat.peerRecentRequests = {}
WhoDat.peerSeen = {}
WhoDat.peerPresencePending = nil
WhoDat.peerPresenceStartup = nil
WhoDat.methodStats = {}
WhoDat.methodStatsStartedAt = 0
WhoDat.debugEvents = {}
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
local SendWhoDatAddonMessage

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
    "guid:GetPlayerInfoByGUID",
    "addon:whisperRequest",
    "addon:whisperResponse",
    "peer:request",
    "peer:response",
    "peer:assist-out",
    "peer:assist-in",
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

local function TrackPeerSeen(name, source)
    local normalizedName = NormalizeName(name)
    if not normalizedName then
        return
    end

    WhoDat.peerSeen[normalizedName] = {
        name = string.match(name, "^([^%-]+)") or name,
        seenAt = GetTime(),
        source = source or "addon",
    }
end

local function GetConnectedPeerCount(maxAgeSeconds)
    local now = GetTime()
    local count = 0

    for normalizedName, entry in pairs(WhoDat.peerSeen) do
        local seenAt = entry.seenAt or 0
        if now - seenAt <= maxAgeSeconds then
            count = count + 1
        else
            WhoDat.peerSeen[normalizedName] = nil
        end
    end

    return count
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
        completedViaPeer = 0,
        completedViaWhisper = 0,
        prefillCompleted = 0,
        timeouts = 0,
        timeoutReasons = {},
        peerAssistSent = 0,
        peerAssistReceived = 0,
    }
    WhoDat.lookupSequence = 0
    WhoDat.peerSeen = {}
    WhoDat.peerPresencePending = nil
    WhoDat.peerPresenceStartup = nil
    WhoDat.lastConnectedPeerCount = 0
end

local function BuildTimeoutReason(pending, baseReason)
    local reason = baseReason or "timeout"

    if pending and pending.lastFailureReason then
        reason = reason .. "|failure:" .. pending.lastFailureReason
    end

    if pending and pending.addonLookupSentAt and not pending.addonResponseReceivedAt then
        reason = reason .. "|no_addon_reply"
    end

    if pending and pending.peerRequestedAt and not pending.peerResponseReceivedAt then
        reason = reason .. "|no_peer_reply"
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

    if string.find(reason, "no_peer_reply", 1, true) then
        RecordMethodDiagnostic("peer:response", false, "timeout_no_response", pending.requestName)
    end

    if string.find(reason, "no_addon_reply", 1, true) then
        RecordMethodDiagnostic("addon:whisperResponse", false, "timeout_no_response", pending.requestName)
    end

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

    local parts = {}
    for i = 1, math.min(4, #reasonItems) do
        table.insert(parts, string.format("%s=%d", reasonItems[i].reason, reasonItems[i].count))
    end

    return table.concat(parts, "; ")
end

local function PrintDebugLog()
    local elapsed = GetTime() - (WhoDat.methodStatsStartedAt or GetTime())
    local connectedPeers = GetConnectedPeerCount(WhoDat.peerSeenMaxAgeSeconds)

    ChatPrint(string.format("Debug log over %.1fs", elapsed))
    ChatPrint(string.format(
        "lookups started=%d completed=%d friend=%d whisper=%d peer=%d fallback=%d prefill=%d timeouts=%d; connected-addons=%d last-load=%d",
        WhoDat.debugSummary.lookupsStarted or 0,
        WhoDat.debugSummary.lookupsCompleted or 0,
        WhoDat.debugSummary.completedFriend or 0,
        WhoDat.debugSummary.completedViaWhisper or 0,
        WhoDat.debugSummary.completedViaPeer or 0,
        WhoDat.debugSummary.completedWithoutFriend or 0,
        WhoDat.debugSummary.prefillCompleted or 0,
        WhoDat.debugSummary.timeouts or 0,
        connectedPeers,
        WhoDat.lastConnectedPeerCount or 0
    ))
    ChatPrint(string.format(
        "peer-assist sent=%d received=%d; timeout-reasons: %s",
        WhoDat.debugSummary.peerAssistSent or 0,
        WhoDat.debugSummary.peerAssistReceived or 0,
        BuildReasonSummary(WhoDat.debugSummary.timeoutReasons)
    ))

    local activePendingCount = CountEntries(WhoDat.pending)
    ChatPrint(string.format("active-pending=%d", activePendingCount))
    if activePendingCount > 0 then
        local now = GetTime()
        local printed = 0
        for _, pending in pairs(WhoDat.pending) do
            printed = printed + 1
            ChatPrint(string.format(
                "pending #%d %s age=%.1fs addonSent=%s addonReply=%s peerSent=%s peerReply=%s lastFailure=%s prefill=%s",
                pending.lookupId or 0,
                pending.requestName or "unknown",
                now - (pending.startedAt or now),
                pending.addonLookupSentAt and "yes" or "no",
                pending.addonResponseReceivedAt and "yes" or "no",
                pending.peerRequestedAt and "yes" or "no",
                pending.peerResponseReceivedAt and "yes" or "no",
                pending.lastFailureReason or "none",
                (pending.prefillData and pending.prefillData.source) or "none"
            ))

            if printed >= 8 then
                break
            end
        end
    end

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

local function DebugPrint(message)
    if WhoDat.debugEnabled then
        ChatPrint("debug: " .. message)
    end
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

    local ok, name, level, class, area, connected = pcall(GetChannelMemberInfo, memberIndex, channelId)
    local usedFallbackOrder = false
    if (not ok) or type(name) ~= "string" or name == "" then
        usedFallbackOrder = true
        ok, name, level, class, area, connected = pcall(GetChannelMemberInfo, channelId, memberIndex)
    end

    if (not ok) or type(name) ~= "string" or name == "" then
        RecordMethodDiagnostic(
            "channel:GetChannelMemberInfo",
            false,
            usedFallbackOrder and "no_result_after_swap" or "no_result"
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

    RecordMethodDiagnostic(
        "channel:GetChannelMemberInfo",
        true,
        usedFallbackOrder and "ok_arg_swap" or "ok",
        summaryData.name
    )

    return summaryData
end

local function GetChannelRosterInfoSummaryData(channelId, memberIndex)
    if type(GetChannelRosterInfo) ~= "function" then
        RecordMethodDiagnostic("channel:GetChannelRosterInfo", false, "unavailable")
        return nil
    end

    local ok, a, b, c, d, e, f, g, h, i = pcall(GetChannelRosterInfo, channelId, memberIndex)
    local usedFallbackOrder = false
    if (not ok) or type(a) ~= "string" or a == "" then
        usedFallbackOrder = true
        ok, a, b, c, d, e, f, g, h, i = pcall(GetChannelRosterInfo, memberIndex, channelId)
    end

    if (not ok) or type(a) ~= "string" or a == "" then
        RecordMethodDiagnostic(
            "channel:GetChannelRosterInfo",
            false,
            usedFallbackOrder and "no_result_after_swap" or "no_result"
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

    RecordMethodDiagnostic(
        "channel:GetChannelRosterInfo",
        true,
        usedFallbackOrder and "ok_arg_swap" or "ok",
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

local function ScanChannelMembers(channelId, channelName, targetNormalizedName)
    local numericChannelId = tonumber(channelId)
    if not numericChannelId or numericChannelId <= 0 then
        return nil
    end

    if type(ChannelRoster) == "function" then
        pcall(ChannelRoster, numericChannelId)
    end

    local probeLimit = GetChannelProbeLimit(numericChannelId)
    local missCount = 0
    local matchedSummaryData = nil

    for memberIndex = 1, probeLimit do
        local summaryData = GetChannelMemberSummaryData(numericChannelId, memberIndex)
        if summaryData and summaryData.name then
            missCount = 0
            StoreChannelSummaryData(channelName, summaryData)

            if targetNormalizedName and NormalizeName(summaryData.name) == targetNormalizedName then
                matchedSummaryData = GetCachedChannelSummaryData(summaryData.name)
                if matchedSummaryData and (HasMinimumInfo(matchedSummaryData) or matchedSummaryData.online == false) then
                    return matchedSummaryData
                end
            end
        else
            missCount = missCount + 1
            if probeLimit == WhoDat.channelMaxProbeMembers and missCount >= WhoDat.channelNilBreakThreshold then
                break
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

local function LooksLikeGuid(value)
    if type(value) ~= "string" or value == "" then
        return false
    end

    if string.find(value, "^Player%-%d+%-%x+$") then
        return true
    end

    if string.find(value, "^0x%x+$") then
        return true
    end

    if string.find(value, "^%x+$") and string.len(value) >= 16 and string.find(value, "[%a]") then
        return true
    end

    return false
end

local function ExtractGuidFromEventArgs(...)
    for i = 1, select("#", ...) do
        local value = select(i, ...)
        if LooksLikeGuid(value) then
            return value
        end
    end

    return nil
end

local function GetSummaryFromGuidApi(playerGuid, nameHint)
    if type(GetPlayerInfoByGUID) ~= "function" then
        RecordMethodDiagnostic("guid:GetPlayerInfoByGUID", false, "unavailable", nameHint)
        return nil
    end

    local ok, localizedClass, englishClass, localizedRace, _, _, guidName = pcall(GetPlayerInfoByGUID, playerGuid)
    if not ok then
        RecordMethodDiagnostic("guid:GetPlayerInfoByGUID", false, "pcall_error", nameHint)
        return nil
    end

    if (not localizedClass or localizedClass == "") and type(englishClass) == "string" and englishClass ~= "" then
        localizedClass = LOCALIZED_CLASS_NAMES_MALE[englishClass]
            or LOCALIZED_CLASS_NAMES_FEMALE[englishClass]
            or englishClass
    end

    local resolvedName = guidName or nameHint
    if not resolvedName or resolvedName == "" then
        RecordMethodDiagnostic("guid:GetPlayerInfoByGUID", false, "no_name", nameHint)
        return nil
    end

    local summaryData = {
        name = string.match(resolvedName, "^([^%-]+)") or resolvedName,
        class = localizedClass,
        race = localizedRace,
        faction = InferFactionFromRace(localizedRace),
        online = true,
        source = "guid",
    }

    RecordMethodDiagnostic("guid:GetPlayerInfoByGUID", true, "ok", summaryData.name)
    return summaryData
end

local function TrackGuidSummary(nameHint, playerGuid, sourceHint)
    if not nameHint or nameHint == "" or not LooksLikeGuid(playerGuid) then
        return nil
    end

    local guidSummary = GetSummaryFromGuidApi(playerGuid, nameHint)
    local mergedSummary = MergeSummaryData(guidSummary, nil)

    if not HasAnySummaryInfo(mergedSummary) then
        return nil
    end

    if sourceHint and sourceHint ~= "" then
        if mergedSummary.source and mergedSummary.source ~= "" then
            mergedSummary.source = mergedSummary.source .. "+" .. sourceHint
        else
            mergedSummary.source = sourceHint
        end
    end

    StoreCachedSummaryData(nameHint, mergedSummary)
    return mergedSummary
end

local function BuildAddonLookupResponseMessage()
    local classLocalized = select(1, UnitClass("player")) or ""
    local areaName = GetRealZoneText() or ""
    local raceLocalized = select(1, UnitRace("player")) or ""
    local factionName = GetFactionFromUnit("player") or InferFactionFromRace(raceLocalized) or ""

    classLocalized = string.gsub(classLocalized, "[\t\r\n]", " ")
    areaName = string.gsub(areaName, "[\t\r\n]", " ")
    raceLocalized = string.gsub(raceLocalized, "[\t\r\n]", " ")
    factionName = string.gsub(factionName, "[\t\r\n]", " ")

    return string.format("R\t%d\t%s\t%s\t%s\t%s", UnitLevel("player") or 0, classLocalized, areaName, factionName, raceLocalized)
end

local function ParseAddonLookupResponseMessage(message)
    if type(message) ~= "string" then
        return nil
    end

    local levelText, className, areaName, factionText, raceText =
        string.match(message, "^R\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t(.*)$")
    if not levelText then
        levelText, className, areaName = string.match(message, "^R\t([^\t]*)\t([^\t]*)\t(.*)$")
    end

    if not levelText then
        return nil
    end

    local parsedLevel = ParseLevelValue(levelText)
    if className == "" then
        className = nil
    end

    if areaName == "" then
        areaName = nil
    end

    if factionText == "" then
        factionText = nil
    end

    if raceText == "" then
        raceText = nil
    end

    if className and not LooksLikeClassName(className) then
        className = nil
    end

    local factionName = NormalizeFaction(factionText) or InferFactionFromRace(raceText)

    return {
        level = parsedLevel,
        class = className,
        area = areaName,
        race = raceText,
        faction = factionName,
        online = true,
        source = "addon:whisper",
        extra = "reported by target addon",
    }
end

local function RequestAddonWhisperLookup(playerName, pending)
    local normalizedName = NormalizeName(playerName)
    if not normalizedName then
        RecordMethodDiagnostic("addon:whisperRequest", false, "invalid_name", playerName)
        return false
    end

    local now = GetTime()
    local lastQueryAt = WhoDat.lastAddonLookupAt[normalizedName] or 0
    if now - lastQueryAt < WhoDat.addonLookupCooldownSeconds then
        RecordMethodDiagnostic("addon:whisperRequest", false, "cooldown", playerName)
        return false
    end

    if type(SendAddonMessage) ~= "function" then
        RecordMethodDiagnostic("addon:whisperRequest", false, "unavailable", playerName)
        return false
    end

    WhoDat.lastAddonLookupAt[normalizedName] = now
    local sent = SendWhoDatAddonMessage("Q", "WHISPER", playerName)
    if sent and pending then
        pending.addonLookupSentAt = now
    end

    if sent then
        DebugPrint(string.format("sent addon lookup whisper to %s", playerName))
        RecordMethodDiagnostic("addon:whisperRequest", true, "sent", playerName)
        AddDebugEvent(string.format("addon whisper request sent for %s", playerName))
    else
        RecordMethodDiagnostic("addon:whisperRequest", false, "send_failed", playerName)
        AddDebugEvent(string.format("addon whisper request failed for %s", playerName))
    end

    return sent
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
    elseif value == "0" then
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

local function GetJoinedChannels()
    if type(GetChannelList) ~= "function" then
        return {}
    end

    local channelsRaw = { GetChannelList() }
    local channels = {}
    for index = 1, #channelsRaw, 2 do
        local channelId = tonumber(channelsRaw[index])
        local channelName = channelsRaw[index + 1]
        if channelId and channelId > 0 and type(channelName) == "string" and channelName ~= "" then
            table.insert(channels, {
                id = channelId,
                name = channelName,
            })
        end
    end

    return channels
end

local function GetDedicatedPeerChannel(channels)
    local configuredName = NormalizeChannelNameForMatch(WhoDat.peerChannelName)
    if configuredName == "" then
        return nil, nil
    end

    channels = channels or GetJoinedChannels()
    for _, channel in ipairs(channels) do
        if NormalizeChannelNameForMatch(channel.name) == configuredName then
            return channel.id, channel.name
        end
    end

    return nil, nil
end

local function EnsurePeerChannelJoined()
    local channels = GetJoinedChannels()
    local channelId, channelName = GetDedicatedPeerChannel(channels)
    if channelId then
        WhoDat.lastKnownPeerChannelName = channelName
        return true, channelId, channelName
    end

    local configuredName = WhoDat.peerChannelName
    if type(configuredName) ~= "string" or configuredName == "" then
        return false, nil, nil
    end

    if type(JoinChannelByName) ~= "function" then
        AddDebugEvent("peer channel auto-join unavailable")
        return false, nil, nil
    end

    local now = GetTime()
    local lastAttemptAt = WhoDat.lastPeerChannelJoinAttemptAt or 0
    if lastAttemptAt > 0 and now - lastAttemptAt < WhoDat.peerChannelJoinRetrySeconds then
        return false, nil, nil
    end

    WhoDat.lastPeerChannelJoinAttemptAt = now
    local joined = pcall(JoinChannelByName, configuredName)
    if joined then
        AddDebugEvent(string.format("peer channel join requested: %s", configuredName))
    else
        AddDebugEvent(string.format("peer channel join failed: %s", configuredName))
    end

    channels = GetJoinedChannels()
    channelId, channelName = GetDedicatedPeerChannel(channels)
    if channelId then
        WhoDat.lastKnownPeerChannelName = channelName
        return true, channelId, channelName
    end

    return joined, nil, configuredName
end

local function GetPreferredPeerChannel()
    local channels = GetJoinedChannels()

    local dedicatedChannelId, dedicatedChannelName = GetDedicatedPeerChannel(channels)
    if dedicatedChannelId then
        WhoDat.lastKnownPeerChannelName = dedicatedChannelName
        return dedicatedChannelId, dedicatedChannelName
    end

    local joinTriggered, joinedChannelId, joinedChannelName = EnsurePeerChannelJoined()
    if joinedChannelId then
        WhoDat.lastKnownPeerChannelName = joinedChannelName
        return joinedChannelId, joinedChannelName
    end

    channels = GetJoinedChannels()
    if #channels == 0 then
        WhoDat.lastKnownPeerChannelName = joinTriggered and joinedChannelName or nil
        return nil, nil
    end

    for _, hint in ipairs(WhoDat.peerChannelHints or {}) do
        local normalizedHint = string.lower(hint)
        for _, channel in ipairs(channels) do
            if string.find(NormalizeChannelNameForMatch(channel.name), normalizedHint, 1, true) then
                WhoDat.lastKnownPeerChannelName = channel.name
                return channel.id, channel.name
            end
        end
    end

    local fallbackChannel = channels[1]
    WhoDat.lastKnownPeerChannelName = fallbackChannel.name
    return fallbackChannel.id, fallbackChannel.name
end

SendWhoDatAddonMessage = function(message, distribution, target)
    if type(SendAddonMessage) ~= "function" then
        return false
    end

    local sent = pcall(SendAddonMessage, WhoDat.addonCommPrefix, message, distribution, target)
    return sent
end

local function BuildPeerRequestId()
    WhoDat.peerRequestSequence = (WhoDat.peerRequestSequence or 0) + 1
    return string.format("%d-%d", math.floor(GetTime() * 100), WhoDat.peerRequestSequence)
end

local function BuildPeerRequestKey(requesterName, requestId, targetName)
    local requesterKey = NormalizeName(requesterName) or ""
    local targetKey = NormalizeName(targetName) or ""
    return requesterKey .. "|" .. (requestId or "") .. "|" .. targetKey
end

local function PrunePeerRecentRequests()
    local now = GetTime()
    for requestKey, expiresAt in pairs(WhoDat.peerRecentRequests) do
        if now > expiresAt then
            WhoDat.peerRecentRequests[requestKey] = nil
        end
    end
end

local function IsPeerRequestRecentlyHandled(requesterName, requestId, targetName)
    local requestKey = BuildPeerRequestKey(requesterName, requestId, targetName)
    local expiresAt = WhoDat.peerRecentRequests[requestKey]
    if not expiresAt then
        return false
    end

    if GetTime() > expiresAt then
        WhoDat.peerRecentRequests[requestKey] = nil
        return false
    end

    return true
end

local function MarkPeerRequestHandled(requesterName, requestId, targetName)
    local requestKey = BuildPeerRequestKey(requesterName, requestId, targetName)
    WhoDat.peerRecentRequests[requestKey] = GetTime() + WhoDat.peerRecentRequestMaxAgeSeconds
end

local function BuildPeerLookupRequestMessage(requesterName, requestId, targetName)
    return string.format(
        "NQ\t%s\t%s\t%s",
        SanitizeAddonField(requesterName),
        SanitizeAddonField(requestId),
        SanitizeAddonField(targetName)
    )
end

local function ParsePeerLookupRequestMessage(message)
    if type(message) ~= "string" then
        return nil
    end

    local requesterName, requestId, targetName = string.match(message, "^NQ\t([^\t]*)\t([^\t]*)\t(.*)$")
    if not requesterName or requesterName == "" or not requestId or requestId == "" or not targetName or targetName == "" then
        return nil
    end

    return requesterName, requestId, targetName
end

local function BuildPeerPresencePingId()
    WhoDat.peerPresencePingSequence = (WhoDat.peerPresencePingSequence or 0) + 1
    return string.format("%d-%d", math.floor(GetTime() * 100), WhoDat.peerPresencePingSequence)
end

local function BuildPeerPresencePingMessage(requesterName, pingId)
    return string.format(
        "NP\t%s\t%s",
        SanitizeAddonField(requesterName),
        SanitizeAddonField(pingId)
    )
end

local function ParsePeerPresencePingMessage(message)
    if type(message) ~= "string" then
        return nil
    end

    local requesterName, pingId = string.match(message, "^NP\t([^\t]*)\t(.*)$")
    if not requesterName or requesterName == "" or not pingId or pingId == "" then
        return nil
    end

    return requesterName, pingId
end

local function BuildPeerPresenceAckMessage(requesterName, pingId)
    return string.format(
        "NA\t%s\t%s",
        SanitizeAddonField(requesterName),
        SanitizeAddonField(pingId)
    )
end

local function ParsePeerPresenceAckMessage(message)
    if type(message) ~= "string" then
        return nil
    end

    local requesterName, pingId = string.match(message, "^NA\t([^\t]*)\t(.*)$")
    if not requesterName or requesterName == "" or not pingId or pingId == "" then
        return nil
    end

    return requesterName, pingId
end

local function BuildPeerLookupResponseMessage(requesterName, requestId, targetName, summaryData)
    local levelText = ""
    if summaryData and summaryData.level and summaryData.level > 0 then
        levelText = tostring(summaryData.level)
    end

    local classText = summaryData and summaryData.class or ""
    local areaText = summaryData and summaryData.area or ""
    local onlineText = ""
    if summaryData and summaryData.online == true then
        onlineText = "1"
    elseif summaryData and summaryData.online == false then
        onlineText = "0"
    end

    local raceText = summaryData and summaryData.race or ""
    local factionText = summaryData and summaryData.faction or ""
    if factionText == "" then
        factionText = InferFactionFromRace(raceText) or ""
    end

    local extraText = summaryData and summaryData.extra or ""

    return string.format(
        "NR\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
        SanitizeAddonField(requesterName),
        SanitizeAddonField(requestId),
        SanitizeAddonField(targetName),
        SanitizeAddonField(levelText),
        SanitizeAddonField(classText),
        SanitizeAddonField(areaText),
        SanitizeAddonField(onlineText),
        SanitizeAddonField(factionText),
        SanitizeAddonField(raceText),
        SanitizeAddonField(extraText)
    )
end

local function ParsePeerLookupResponseMessage(message)
    if type(message) ~= "string" then
        return nil
    end

    local requesterName, requestId, targetName, levelText, classText, areaText, onlineText, factionText, raceText, extraText =
        string.match(message, "^NR\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t(.*)$")

    if not requesterName then
        requesterName, requestId, targetName, levelText, classText, areaText, onlineText, extraText =
            string.match(message, "^NR\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t(.*)$")
    end

    if not requesterName or requesterName == "" or not requestId or requestId == "" or not targetName or targetName == "" then
        return nil
    end

    if factionText == "" then
        factionText = nil
    end

    if raceText == "" then
        raceText = nil
    end

    if type(extraText) ~= "string" or extraText == "" then
        extraText = "reported by peer addon"
    end

    if not raceText then
        local raceFromExtra = string.match(extraText, "^race:%s*(.+)$")
        if raceFromExtra and raceFromExtra ~= "" then
            raceText = raceFromExtra
        end
    end

    local factionName = NormalizeFaction(factionText) or InferFactionFromRace(raceText)

    local summaryData = {
        name = targetName,
        level = ParseLevelValue(levelText),
        class = classText ~= "" and classText or nil,
        area = areaText ~= "" and areaText or nil,
        race = raceText,
        faction = factionName,
        online = ParseAddonOnlineField(onlineText),
        source = "addon:peer",
        extra = extraText,
    }

    if summaryData.class and not LooksLikeClassName(summaryData.class) then
        summaryData.class = nil
    end

    return requesterName, requestId, targetName, summaryData
end

local function ShouldUsePeerSummary(summaryData)
    if not HasAnySummaryInfo(summaryData) then
        return false
    end

    if HasMinimumInfo(summaryData) or HasPartialIdentityInfo(summaryData) then
        return true
    end

    return summaryData.online == true
end

local function BuildPeerLookupSummaryByName(targetName)
    local directData = GetDirectSummary(targetName)
    local cachedData = GetCachedChannelSummaryData(targetName)
    local chatData = GetRecentChatSummaryByName(targetName)
    local mergedData = MergeSummaryData(chatData, MergeSummaryData(directData, cachedData))

    if not HasAnySummaryInfo(mergedData) then
        return nil
    end

    if mergedData.online == false and not HasMinimumInfo(mergedData) then
        mergedData.online = nil
    end

    if not ShouldUsePeerSummary(mergedData) then
        return nil
    end

    return mergedData
end

local function SendPeerLookupRequest(targetName, pending)
    if not WhoDat.peerLookupEnabled then
        RecordMethodDiagnostic("peer:request", false, "disabled", targetName)
        return false
    end

    local normalizedName = NormalizeName(targetName)
    if not normalizedName then
        RecordMethodDiagnostic("peer:request", false, "invalid_name", targetName)
        return false
    end

    local now = GetTime()
    local lastLookupAt = WhoDat.lastPeerLookupAt[normalizedName] or 0
    if now - lastLookupAt < WhoDat.peerRequestCooldownSeconds then
        RecordMethodDiagnostic("peer:request", false, "cooldown", targetName)
        return false
    end

    if type(SendAddonMessage) ~= "function" then
        RecordMethodDiagnostic("peer:request", false, "unavailable", targetName)
        return false
    end

    local channelId, channelName = GetPreferredPeerChannel()
    if not channelId then
        RecordMethodDiagnostic("peer:request", false, "no_channel", targetName)
        return false
    end

    local requesterName = UnitName("player")
    if not requesterName or requesterName == "" then
        RecordMethodDiagnostic("peer:request", false, "no_requester", targetName)
        return false
    end

    local requestId = BuildPeerRequestId()
    local requestMessage = BuildPeerLookupRequestMessage(requesterName, requestId, targetName)
    local sent = SendWhoDatAddonMessage(requestMessage, "CHANNEL", channelId)
    if not sent then
        RecordMethodDiagnostic("peer:request", false, "send_failed", targetName)
        AddDebugEvent(string.format("peer request send failed for %s on %s", targetName, channelName or "channel"))
        return false
    end

    WhoDat.lastPeerLookupAt[normalizedName] = now

    if pending then
        pending.peerRequestId = requestId
        pending.peerRequestedAt = now
        pending.peerChannel = channelName
    end

    RecordMethodDiagnostic("peer:request", true, "sent", targetName)
    AddDebugEvent(string.format("peer request sent for %s on %s", targetName, channelName))
    DebugPrint(string.format("broadcast peer lookup for %s on %s", targetName, channelName))
    return true
end

local function SendPeerLookupResponse(requesterName, requestId, targetName, summaryData)
    if not WhoDat.peerLookupEnabled then
        return false
    end

    local channelId = select(1, GetPreferredPeerChannel())
    if not channelId then
        return false
    end

    local responseMessage = BuildPeerLookupResponseMessage(requesterName, requestId, targetName, summaryData)
    return SendWhoDatAddonMessage(responseMessage, "CHANNEL", channelId)
end

local function StartPeerPresencePing(suppressNoChannelMessage)
    if not WhoDat.peerLookupEnabled then
        WhoDat.lastConnectedPeerCount = 0
        ChatPrint("Connected WhoDat addons: peer mode disabled.")
        AddDebugEvent("presence ping skipped (peer mode disabled)")
        return false, "disabled"
    end

    if type(SendAddonMessage) ~= "function" then
        WhoDat.lastConnectedPeerCount = 0
        ChatPrint("Connected WhoDat addons: addon communication API unavailable.")
        AddDebugEvent("presence ping skipped (addon comm unavailable)")
        return false, "unavailable"
    end

    local requesterName = UnitName("player")
    if not requesterName or requesterName == "" then
        return false, "no_requester"
    end

    local channelId, channelName = GetPreferredPeerChannel()
    if not channelId then
        WhoDat.lastConnectedPeerCount = 0
        if not suppressNoChannelMessage then
            ChatPrint("Connected WhoDat addons: 0 (no shared channel).")
        end
        AddDebugEvent("presence ping skipped (no channel)")
        return false, "no_channel"
    end

    local pingId = BuildPeerPresencePingId()
    local pingMessage = BuildPeerPresencePingMessage(requesterName, pingId)
    local sent = SendWhoDatAddonMessage(pingMessage, "CHANNEL", channelId)
    if not sent then
        WhoDat.lastConnectedPeerCount = 0
        ChatPrint(string.format("Connected WhoDat addons on %s: 0 (ping send failed).", channelName))
        AddDebugEvent(string.format("presence ping send failed on %s", channelName))
        return false, "send_failed"
    end

    WhoDat.peerPresencePending = {
        id = pingId,
        startedAt = GetTime(),
        channelName = channelName,
        responders = {},
    }

    AddDebugEvent(string.format("presence ping sent on %s", channelName))
    return true, "sent"
end

local function BeginPeerPresenceStartupCheck()
    WhoDat.peerPresenceStartup = {
        startedAt = GetTime(),
        nextAttemptAt = 0,
    }
end

local function AdvancePeerPresenceStartupCheck(now)
    local startup = WhoDat.peerPresenceStartup
    if not startup or WhoDat.peerPresencePending then
        return
    end

    if now < (startup.nextAttemptAt or 0) then
        return
    end

    EnsurePeerChannelJoined()
    local sent, reason = StartPeerPresencePing(true)
    if sent then
        WhoDat.peerPresenceStartup = nil
        return
    end

    if reason ~= "no_channel" then
        WhoDat.peerPresenceStartup = nil
        return
    end

    if now - (startup.startedAt or now) >= WhoDat.peerPresenceStartupMaxWaitSeconds then
        WhoDat.lastConnectedPeerCount = 0
        ChatPrint(string.format("Connected WhoDat addons: 0 (no shared channel: %s).", WhoDat.peerChannelName or "channel"))
        AddDebugEvent(string.format("presence ping startup timeout waiting for %s", WhoDat.peerChannelName or "channel"))
        WhoDat.peerPresenceStartup = nil
        return
    end

    startup.nextAttemptAt = now + WhoDat.peerPresenceStartupRetrySeconds
end

local function HasPendingLookups()
    if WhoDat.peerPresenceStartup then
        return true
    end

    if WhoDat.peerPresencePending then
        return true
    end

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
        if WhoDat.peerLookupEnabled then
            pending.friendLookupBlockedAt = GetTime()

            if not pending.peerRequestedAt then
                SendPeerLookupRequest(pending.requestName, pending)
            end

            if pending.prefillData and HasPartialIdentityInfo(pending.prefillData) and not pending.blockedSummaryPrinted then
                ChatPrint(BuildSummary(pending.prefillData))
                pending.blockedSummaryPrinted = true
            end

            return true
        end

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

local function CompleteLookupWithoutFriend(normalizedName, pending, summaryData)
    local finalData = summaryData or pending.prefillData or {
        name = pending.requestName,
        online = true,
        source = "addon",
    }

    local completionReason = "without_friend"
    local source = finalData.source or ""
    if string.find(source, "addon:peer", 1, true) then
        completionReason = "peer"
        IncrementSummaryCounter("completedViaPeer", 1)
        if pending.peerResponderName then
            IncrementSummaryCounter("peerAssistReceived", 1)
            RecordMethodDiagnostic("peer:assist-in", true, "received", pending.requestName)
            AddDebugEvent(string.format(
                "lookup #%d requester got peer data for %s from %s",
                pending.lookupId or 0,
                pending.requestName,
                pending.peerResponderName
            ))
        else
            AddDebugEvent(string.format("lookup #%d completed via peer for %s", pending.lookupId or 0, pending.requestName))
        end
    elseif string.find(source, "addon:whisper", 1, true) then
        completionReason = "addon_whisper"
        IncrementSummaryCounter("completedViaWhisper", 1)
        AddDebugEvent(string.format("lookup #%d completed via addon whisper for %s", pending.lookupId or 0, pending.requestName))
    else
        IncrementSummaryCounter("completedWithoutFriend", 1)
        AddDebugEvent(string.format("lookup #%d completed without friend for %s", pending.lookupId or 0, pending.requestName))
    end

    IncrementSummaryCounter("lookupsCompleted", 1)
    RecordMethodDiagnostic("lookup:complete", true, completionReason, pending and pending.requestName)

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
        timeoutLogged = false,
        lastFailureReason = nil,
    }

    IncrementSummaryCounter("lookupsStarted", 1)
    RecordMethodDiagnostic("lookup:start", true, "pending", playerName)
    AddDebugEvent(string.format("lookup #%d started for %s", lookupId, playerName))

    local pending = WhoDat.pending[normalizedName]
    local addonSent = RequestAddonWhisperLookup(playerName, pending)
    pending.addonLookupAttempted = true
    pending.addonLookupSucceeded = addonSent

    if WhoDat.peerLookupEnabled then
        local peerSent = SendPeerLookupRequest(playerName, pending)
        pending.peerLookupAttempted = true
        pending.peerLookupSucceeded = peerSent
    else
        pending.peerLookupAttempted = false
        pending.peerLookupSucceeded = false
    end

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
    PrunePeerRecentRequests()

    local now = GetTime()
    AdvancePeerPresenceStartupCheck(now)

    if self.peerPresencePending and now - self.peerPresencePending.startedAt >= self.peerPresencePingWaitSeconds then
        local connectedCount = GetTableCount(self.peerPresencePending.responders)
        self.lastConnectedPeerCount = connectedCount
        ChatPrint(string.format("Connected WhoDat addons on %s: %d.", self.peerPresencePending.channelName or "channel", connectedCount))
        AddDebugEvent(string.format(
            "presence ping complete on %s: %d responders",
            self.peerPresencePending.channelName or "channel",
            connectedCount
        ))
        self.peerPresencePending = nil
    end

    for normalizedName, pending in pairs(self.pending) do
        local blockedPendingHandled = false
        if pending.friendLookupBlockedAt then
            blockedPendingHandled = true

            local blockedAge = now - pending.friendLookupBlockedAt
            if blockedAge >= WhoDat.peerResponseWaitSeconds then
                local fallbackData = pending.partialData or pending.prefillData
                local printedSummary = pending.blockedSummaryPrinted == true

                if not printedSummary and fallbackData and ShouldUsePeerSummary(fallbackData) then
                    ChatPrint(BuildSummary(fallbackData))
                    printedSummary = true
                end

                if not printedSummary then
                    if pending.peerRequestedAt then
                        RegisterLookupTimeout(pending, "peer_no_response_after_wrong_faction")

                        ChatPrint(string.format(
                            "%s is online, but cross-faction details are unavailable on this server: no peer response.",
                            pending.requestName
                        ))
                    else
                        RegisterLookupTimeout(pending, "wrong_faction_no_peer_channel")
                        ChatPrint(string.format(
                            "%s: cross-faction lookup is blocked and no shared peer channel is available.",
                            pending.requestName
                        ))
                    end
                end

                self.pending[normalizedName] = nil
            end
        end

        if not blockedPendingHandled then
        local fastFallbackHandled = false
        if pending.addedTemporarily and not pending.friendSeenAt and pending.prefillData and pending.prefillData.online == true then
            local source = pending.prefillData.source or ""
            local age = now - (pending.friendAddRequestedAt or pending.startedAt)
            local fastFallbackAge = WhoDat.addFriendFailureFallbackSeconds
            if pending.peerRequestedAt and WhoDat.peerResponseWaitSeconds > fastFallbackAge then
                fastFallbackAge = WhoDat.peerResponseWaitSeconds
            end
            if (string.find(source, "chat", 1, true) or string.find(source, "channel", 1, true))
                and age >= fastFallbackAge then
                if HasPartialIdentityInfo(pending.prefillData) then
                    ChatPrint(BuildSummary(pending.prefillData))
                else
                    RegisterLookupTimeout(pending, "fast_fallback_missing_identity")
                    local detailParts = {}
                    if pending.addonLookupSentAt then

                        table.insert(detailParts, "no addon whisper response")
                    end

                    if pending.peerRequestedAt then

                        table.insert(detailParts, "no peer response")
                    end

                    local detailNote = ""
                    if #detailParts > 0 then
                        detailNote = ": " .. table.concat(detailParts, ", ")
                    end

                    ChatPrint(string.format(
                        "%s is online, but cross-faction details are unavailable on this server%s.",
                        pending.prefillData.name or pending.requestName,
                        detailNote
                    ))
                end

                self.pending[normalizedName] = nil
                fastFallbackHandled = true
            end
        end

        if (not fastFallbackHandled) and not TryCompleteLookup(normalizedName, pending, now) and now - pending.startedAt >= self.lookupTimeoutSeconds then
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

function WhoDat:HandleChatMessageEvent(event, ...)
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
        end
    end

    TrackChatPresence(sender, source)

    local senderGuid = ExtractGuidFromEventArgs(...)
    if senderGuid then
        local guidSummary = TrackGuidSummary(sender, senderGuid, "chat:guid")
        if guidSummary then
            DebugPrint(string.format("tracked guid summary for %s", sender))
        end
    end
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
end

function WhoDat:HandleAddonMessageEvent(prefix, message, distribution, sender)
    if prefix ~= self.addonCommPrefix then
        return
    end

    local senderName = string.match(sender or "", "^([^%-]+)") or sender
    if type(senderName) ~= "string" or senderName == "" then
        return
    end

    local ownName = UnitName("player")
    if NormalizeName(senderName) == NormalizeName(ownName) then
        return
    end

    TrackPeerSeen(senderName, distribution or "addon")

    if self.peerLookupEnabled then
        PrunePeerRecentRequests()

        local pingRequesterName, pingId = ParsePeerPresencePingMessage(message)
        if pingRequesterName then
            if NormalizeName(pingRequesterName) ~= NormalizeName(ownName) then
                local channelId = select(1, GetPreferredPeerChannel())
                if channelId then
                    local ackMessage = BuildPeerPresenceAckMessage(pingRequesterName, pingId)
                    SendWhoDatAddonMessage(ackMessage, "CHANNEL", channelId)
                end
            end
            return
        end

        local ackRequesterName, ackId = ParsePeerPresenceAckMessage(message)
        if ackRequesterName then
            if NormalizeName(ackRequesterName) == NormalizeName(ownName)
                and self.peerPresencePending
                and self.peerPresencePending.id == ackId then
                self.peerPresencePending.responders[NormalizeName(senderName)] = senderName
            end
            return
        end

        local requesterName, requestId, targetName = ParsePeerLookupRequestMessage(message)
        if requesterName then
            if NormalizeName(requesterName) == NormalizeName(ownName) then
                return
            end

            if IsPeerRequestRecentlyHandled(requesterName, requestId, targetName) then
                return
            end

            MarkPeerRequestHandled(requesterName, requestId, targetName)

            local summaryData = BuildPeerLookupSummaryByName(targetName)
            if summaryData then
                local sent = SendPeerLookupResponse(requesterName, requestId, targetName, summaryData)
                if sent then
                    IncrementSummaryCounter("peerAssistSent", 1)
                    RecordMethodDiagnostic("peer:assist-out", true, "sent", targetName)
                    AddDebugEvent(string.format(
                        "peer assist sent for %s to %s (source=%s)",
                        targetName,
                        requesterName,
                        summaryData.source or "unknown"
                    ))
                    DebugPrint(string.format("answered peer lookup for %s", targetName))
                else
                    RecordMethodDiagnostic("peer:assist-out", false, "send_failed", targetName)
                    AddDebugEvent(string.format("peer assist send failed for %s to %s", targetName, requesterName))
                end
            end
            return
        end

        local responseRequester, responseRequestId, responseTargetName, responseData = ParsePeerLookupResponseMessage(message)
        if responseRequester then
            if NormalizeName(responseRequester) ~= NormalizeName(ownName) then
                return
            end

            if responseData then
                RecordMethodDiagnostic("peer:response", true, "received", responseTargetName)
                StoreCachedSummaryData(responseTargetName, responseData)
                TrackChatPresence(responseTargetName, "addon:peer")
            end

            local normalizedTarget = NormalizeName(responseTargetName)
            local pending = normalizedTarget and self.pending[normalizedTarget] or nil
            if pending and responseData then
                if pending.peerRequestId and pending.peerRequestId ~= responseRequestId then
                    return
                end

                pending.peerResponseReceivedAt = GetTime()
                pending.peerResponderName = senderName
                AddDebugEvent(string.format(
                    "peer response received for lookup #%d (%s) from %s",
                    pending.lookupId or 0,
                    pending.requestName,
                    senderName
                ))
                DebugPrint(string.format("peer lookup succeeded for %s via %s", pending.requestName, senderName))

                local mergedData = MergeSummaryData(responseData, pending.prefillData)
                pending.prefillData = mergedData
                if ShouldUsePeerSummary(mergedData) then
                    CompleteLookupWithoutFriend(normalizedTarget, pending, mergedData)
                end
            end
            return
        end
    end

    if message == "Q" then
        if distribution == "WHISPER" then
            local responseMessage = BuildAddonLookupResponseMessage()
            local sent = SendWhoDatAddonMessage(responseMessage, "WHISPER", senderName)
            if sent then
                DebugPrint(string.format("replied addon lookup to %s", senderName))
            end
        end
        return
    end

    local responseData = ParseAddonLookupResponseMessage(message)
    if not responseData then
        return
    end

    RecordMethodDiagnostic("addon:whisperResponse", true, "received", senderName)

    responseData.name = senderName
    StoreCachedSummaryData(senderName, responseData)
    TrackChatPresence(senderName, "addon:whisper")

    local normalizedName = NormalizeName(senderName)
    local pending = normalizedName and self.pending[normalizedName] or nil
    if pending then
        pending.addonResponseReceivedAt = GetTime()
        AddDebugEvent(string.format("addon whisper response received for lookup #%d (%s)", pending.lookupId or 0, pending.requestName))
        local mergedData = MergeSummaryData(responseData, pending.prefillData)
        CompleteLookupWithoutFriend(normalizedName, pending, mergedData)
    end
end

function WhoDat:HandleSlashCommand(message)
    local playerName = ParseSlashName(message)
    if not playerName then
        ChatPrint("Usage: /whodat PlayerName")
        ChatPrint("       /whodat debug on|off|status|log|reset")
        return
    end

    if NormalizeName(playerName) == "debug" then
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
        elseif debugArg == "reset" then
            ResetDebugTracking()
            ChatPrint("Debug log counters reset.")
        else
            local peerChannelName = "disabled"
            if self.peerLookupEnabled then
                _, peerChannelName = GetPreferredPeerChannel()
            end

            local connectedPeers = GetConnectedPeerCount(self.peerSeenMaxAgeSeconds)

            ChatPrint(string.format(
                "Debug status: %s; filter api: %s; added-pattern: %s; removed-pattern: %s; channel-member-api: %s; channel-roster-api: %s; guid-api: %s; addon-whisper-api: %s; prefix: %s; peer: %s; peer-channel: %s; peer-requests: %d; channel-cache: %d; chat-presence: %d; connected-addons: %d; load-connected: %d.",
                self.debugEnabled and "on" or "off",
                ChatFrameAddMessageEventFilter and "available" or "missing",
                FRIEND_ADDED_PATTERN and "ok" or "missing",
                FRIEND_REMOVED_PATTERN and "ok" or "missing",
                type(GetChannelMemberInfo) == "function" and "available" or "missing",
                type(GetChannelRosterInfo) == "function" and "available" or "missing",
                type(GetPlayerInfoByGUID) == "function" and "available" or "missing",
                type(SendAddonMessage) == "function" and "available" or "missing",
                self.addonCommPrefix,
                self.peerLookupEnabled and "enabled" or "disabled",
                peerChannelName or "missing",
                GetTableCount(self.peerRecentRequests),
                GetTableCount(self.channelCache),
                GetTableCount(self.recentChatPresence),
                connectedPeers,
                self.lastConnectedPeerCount or 0
            ))
        end
        return
    end

    StartLookup(playerName)
end

function WhoDat:HandlePlayerLogin()
    WhoDatDB = WhoDatDB or {}
    WhoDatDB.channelCache = WhoDatDB.channelCache or {}
    self.channelCache = WhoDatDB.channelCache
    ResetDebugTracking()

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

    EnsurePeerChannelJoined()
    BeginPeerPresenceStartupCheck()
    SetLookupTicker(true)

    ChatPrint("Loaded. Shift-click a player name or use /whodat Name to query level/class/location.")
end

WhoDat:SetScript("OnEvent", function(self, event, ...)
    if event == "PLAYER_LOGIN" then
        self:HandlePlayerLogin()
    elseif event == "FRIENDLIST_UPDATE" then
        self:HandleFriendListUpdate()
    elseif event == "CHANNEL_ROSTER_UPDATE" then
        self:HandleChannelRosterUpdate(...)
    elseif event == "CHAT_MSG_ADDON" then
        self:HandleAddonMessageEvent(...)
    elseif string.find(event, "^CHAT_MSG_") then
        self:HandleChatMessageEvent(event, ...)
    end
end)

WhoDat:RegisterEvent("PLAYER_LOGIN")
WhoDat:RegisterEvent("FRIENDLIST_UPDATE")
WhoDat:RegisterEvent("CHANNEL_ROSTER_UPDATE")
WhoDat:RegisterEvent("CHAT_MSG_CHANNEL")
WhoDat:RegisterEvent("CHAT_MSG_SAY")
WhoDat:RegisterEvent("CHAT_MSG_YELL")
WhoDat:RegisterEvent("CHAT_MSG_GUILD")
WhoDat:RegisterEvent("CHAT_MSG_PARTY")
WhoDat:RegisterEvent("CHAT_MSG_RAID")
WhoDat:RegisterEvent("CHAT_MSG_WHISPER")
WhoDat:RegisterEvent("CHAT_MSG_ADDON")
