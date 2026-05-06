-- Atomically stores a vitals reading in the sliding window and checks
-- whether any vital has crossed the patient's threshold.
--
-- KEYS[1]: "vitals:{patient_id}"   ZSET  — sliding 10-min window
-- KEYS[2]: "baseline:{patient_id}" Hash  — per-patient thresholds
--
-- ARGV[1]: timestamp (unix float, used as ZSET score)
-- ARGV[2]: full vitals JSON        (ZSET member)
-- ARGV[3]: heart_rate
-- ARGV[4]: spo2
-- ARGV[5]: bp_systolic
-- ARGV[6]: signal_quality_index
--
-- Returns 0 (no triage needed) or 1 (threshold crossed → send triage event)

local sqi = tonumber(ARGV[6])
if sqi < 0.6 then
    -- Low signal quality: sensor artifact, discard silently
    return 0
end

local ts = tonumber(ARGV[1])

-- Append to sliding window and evict readings older than 10 minutes
redis.call('ZADD', KEYS[1], ts, ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ts - 600)

-- Read patient-specific thresholds; fall back to safe clinical defaults
local hr_min   = tonumber(redis.call('HGET', KEYS[2], 'hr_min'))   or 50
local hr_max   = tonumber(redis.call('HGET', KEYS[2], 'hr_max'))   or 100
local spo2_min = tonumber(redis.call('HGET', KEYS[2], 'spo2_min')) or 94
local bp_min   = tonumber(redis.call('HGET', KEYS[2], 'bp_min'))   or 80
local bp_max   = tonumber(redis.call('HGET', KEYS[2], 'bp_max'))   or 140

local hr  = tonumber(ARGV[3])
local spo2 = tonumber(ARGV[4])
local bp  = tonumber(ARGV[5])

if hr > hr_max or hr < hr_min or spo2 < spo2_min or bp > bp_max or bp < bp_min then
    return 1
end

return 0
