// Pure pace-math for the proxy dashboard.
// Attaches to window.PaceMath for the dashboard; also importable from tests.
// No DOM access, no side effects, no network. Everything is a pure function of
// (keys snapshot, now).

(function (global) {
  'use strict';

  var WINDOW_DURATION_MS = {
    'unified-5h': 5 * 60 * 60 * 1000,          // 18_000_000
    'unified-7d': 7 * 24 * 60 * 60 * 1000,     // 604_800_000
  };

  // Tone buckets for pace ratio. Rank order is used for sorting.
  //   dim    — no signal / behind pace / early window (quiet)
  //   green  — on pace
  //   yellow — slightly ahead
  //   orange — noticeably ahead, will hit limit if trend holds
  //   red    — burning hot, intervene
  var TONE_RANK = { dim: 0, green: 1, yellow: 2, orange: 3, red: 4 };

  // Thresholds defining tone from the pace ratio (util / elapsed).
  // < 0.85       → dim (behind — wasted budget, safe)
  //   0.85–1.15  → green (on pace)
  //   1.15–1.5   → yellow
  //   1.5–2.0    → orange
  //   > 2.0      → red
  var PACE_THRESHOLDS = { onPace: 0.85, warn: 1.15, hot: 1.5, danger: 2.0 };

  // Dead-zone thresholds — force tone=dim when data is too thin to trust.
  // Right after a reset, elapsed ≈ 0 makes the ratio explode from a single
  // request. Suppress until we have real signal.
  var DEAD_ZONE = {
    minElapsedFrac: 0.02,         // 2% of the window must have passed
    minElapsedMs:   60 * 1000,    // and at least 60 seconds of wall time
    minUtilization: 0.01,         // util under 1% is noise
  };

  function toneFromPace(paceRatio) {
    if (paceRatio < PACE_THRESHOLDS.onPace) return 'dim';
    if (paceRatio < PACE_THRESHOLDS.warn)   return 'green';
    if (paceRatio < PACE_THRESHOLDS.hot)    return 'yellow';
    if (paceRatio < PACE_THRESHOLDS.danger) return 'orange';
    return 'red';
  }

  // computeWindowPace: the core per-window calculation.
  //   input:  { utilization, resetAt, windowName, now }
  //   output: {
  //     utilization:          clamped 0..1 (null if missing)
  //     elapsedFrac:          0..1, 1-(remaining/duration), null if no resetAt
  //     paceRatio:            util / elapsedFrac, null if either side missing
  //     projectedUtilAtReset: util * (1 / elapsedFrac), clamped to 0..2
  //     tone:                 'dim' | 'green' | 'yellow' | 'orange' | 'red'
  //     toneRank:             numeric tone rank
  //     deadZone:             true if tone was forced to dim by safety checks
  //   }
  function computeWindowPace(args) {
    args = args || {};
    var util        = (args.utilization === null || args.utilization === undefined) ? null : Number(args.utilization);
    var resetAt     = (args.resetAt === null || args.resetAt === undefined) ? null : Number(args.resetAt);
    var windowName  = args.windowName;
    var now         = Number(args.now);
    var duration    = WINDOW_DURATION_MS[windowName] || null;

    var utilization = (util === null || Number.isNaN(util)) ? null : Math.max(0, Math.min(1, util));

    var elapsedFrac = null;
    if (resetAt !== null && !Number.isNaN(resetAt) && duration !== null && duration > 0) {
      var remaining = resetAt - now;
      var elapsed   = duration - remaining;
      elapsedFrac = Math.max(0, Math.min(1, elapsed / duration));
    }

    var paceRatio = null;
    if (utilization !== null && elapsedFrac !== null && elapsedFrac > 0) {
      paceRatio = utilization / elapsedFrac;
    }

    // Projected final utilization at reset, assuming current pace holds.
    var projectedUtilAtReset = null;
    if (utilization !== null && elapsedFrac !== null && elapsedFrac > 0) {
      projectedUtilAtReset = Math.max(0, Math.min(2, utilization / elapsedFrac));
    }

    var deadZone = false;
    if (utilization === null || elapsedFrac === null) {
      deadZone = true;
    } else {
      var elapsedMs = duration * elapsedFrac;
      if (elapsedFrac < DEAD_ZONE.minElapsedFrac) deadZone = true;
      if (elapsedMs < DEAD_ZONE.minElapsedMs)     deadZone = true;
      if (utilization < DEAD_ZONE.minUtilization) deadZone = true;
    }

    var tone = (deadZone || paceRatio === null) ? 'dim' : toneFromPace(paceRatio);

    return {
      windowName: windowName || null,
      utilization: utilization,
      elapsedFrac: elapsedFrac,
      paceRatio: paceRatio,
      projectedUtilAtReset: projectedUtilAtReset,
      tone: tone,
      toneRank: TONE_RANK[tone],
      deadZone: deadZone,
      resetAt: resetAt,
    };
  }

  // Find a key's window snapshot by name.
  function findWindow(key, windowName) {
    var windows = (key && key.capacity && key.capacity.windows) || [];
    for (var i = 0; i < windows.length; i++) {
      if (windows[i].windowName === windowName) return windows[i];
    }
    return null;
  }

  // computePoolAggregate: fleet-level summary for a single window.
  //   summaryWindow: the CapacitySummaryWindow object from the server
  //                  (gives us maxUtilization, medianUtilization, nextResetAt)
  //   keys: full key list, used to derive a representative elapsed fraction
  //   returns: { meanUtil, maxUtil, medianUtil, aggregatePace, tone, nextResetAt, keysReporting }
  //
  // Elapsed fraction for the pool: we pick the MIN elapsed across keys that
  // have a resetAt for this window (i.e. the "most recently reset" key's
  // window). That's the least-elapsed perspective — conservative. It matches
  // operator intuition: "some keys just reset, so budget is fresh".
  function computePoolAggregate(summaryWindow, keys, windowName, now) {
    var duration = WINDOW_DURATION_MS[windowName] || null;
    var utilValues = [];
    var elapsedFracs = [];
    var keysArr = Array.isArray(keys) ? keys : [];

    for (var i = 0; i < keysArr.length; i++) {
      var w = findWindow(keysArr[i], windowName);
      if (!w) continue;
      if (w.utilization !== null && w.utilization !== undefined && !Number.isNaN(Number(w.utilization))) {
        utilValues.push(Math.max(0, Math.min(1, Number(w.utilization))));
      }
      if (w.resetAt !== null && w.resetAt !== undefined && duration !== null && duration > 0) {
        var ef = (duration - (Number(w.resetAt) - now)) / duration;
        elapsedFracs.push(Math.max(0, Math.min(1, ef)));
      }
    }

    var meanUtil   = utilValues.length > 0 ? utilValues.reduce(function (a, b) { return a + b; }, 0) / utilValues.length : null;
    var maxUtil    = (summaryWindow && summaryWindow.maxUtilization != null) ? Number(summaryWindow.maxUtilization)
                   : (utilValues.length > 0 ? Math.max.apply(null, utilValues) : null);
    var medianUtil = (summaryWindow && summaryWindow.medianUtilization != null) ? Number(summaryWindow.medianUtilization)
                   : (utilValues.length > 0 ? median(utilValues) : null);

    // Aggregate elapsed: MAX of elapsed fractions = the "oldest" window.
    // This is the key that's been sampling longest — most representative of
    // the fleet's current budget position. Min would bias toward fresh keys.
    var aggregateElapsed = elapsedFracs.length > 0 ? Math.max.apply(null, elapsedFracs) : null;

    var aggregatePace = null;
    if (meanUtil !== null && aggregateElapsed !== null && aggregateElapsed > 0) {
      aggregatePace = meanUtil / aggregateElapsed;
    }

    // Dead-zone check on the aggregate too.
    var deadZone = (aggregateElapsed === null || aggregateElapsed < DEAD_ZONE.minElapsedFrac || meanUtil === null || meanUtil < DEAD_ZONE.minUtilization);
    var tone = (deadZone || aggregatePace === null) ? 'dim' : toneFromPace(aggregatePace);

    return {
      windowName: windowName,
      meanUtil: meanUtil,
      maxUtil: maxUtil,
      medianUtil: medianUtil,
      aggregateElapsed: aggregateElapsed,
      aggregatePace: aggregatePace,
      tone: tone,
      toneRank: TONE_RANK[tone],
      deadZone: deadZone,
      nextResetAt: summaryWindow ? (summaryWindow.nextResetAt || null) : null,
      keysReporting: utilValues.length,
    };
  }

  function median(sortedOrUnsorted) {
    var arr = sortedOrUnsorted.slice().sort(function (a, b) { return a - b; });
    var n = arr.length;
    if (n === 0) return null;
    var mid = Math.floor(n / 2);
    return n % 2 === 0 ? (arr[mid - 1] + arr[mid]) / 2 : arr[mid];
  }

  // Worst (highest) toneRank across the primary windows for a key.
  // Cooling/unavailable keys are given their own -1 rank so they don't
  // masquerade as "healthy dim" — downstream sort treats them specifically.
  function worstToneRank(key, now) {
    var windowNames = ['unified-5h', 'unified-7d'];
    var worst = 0;
    for (var i = 0; i < windowNames.length; i++) {
      var w = findWindow(key, windowNames[i]);
      if (!w) continue;
      var pace = computeWindowPace({
        utilization: w.utilization,
        resetAt: w.resetAt,
        windowName: windowNames[i],
        now: now,
      });
      if (pace.toneRank > worst) worst = pace.toneRank;
    }
    return worst;
  }

  // Earliest resetAt across windows for a key. Used as a tiebreaker within
  // a priority tier — keys whose capacity refreshes soon rank earlier,
  // since their budget is about to lift.
  function earliestResetAt(key) {
    var windows = (key && key.capacity && key.capacity.windows) || [];
    var earliest = Infinity;
    for (var i = 0; i < windows.length; i++) {
      var r = windows[i].resetAt;
      if (r !== null && r !== undefined && r < earliest) earliest = r;
    }
    return earliest === Infinity ? null : earliest;
  }

  // sortKeysForDisplay: priority-preserving display sort.
  //   1. priority ASC  — preferred (1) before normal (2) before fallback (3).
  //      Mirrors key-manager.ts selectLeastLoadedAvailableKey (Math.min on
  //      priority, fully exhausting the preferred tier before touching the
  //      next). This is LOAD-BEARING — do not let urgency override priority.
  //   2. worstToneRank DESC — within tier, keys in trouble float to the top.
  //      Quantized to tone buckets (not raw pace ratio) so rows don't jitter
  //      as pace ratios tick every second.
  //   3. earliestResetAt ASC — within same tier+tone, keys about to refresh
  //      surface first (they'll be next to receive traffic).
  //   4. label ASC — deterministic final tiebreaker.
  function sortKeysForDisplay(keys, now) {
    var arr = Array.isArray(keys) ? keys.slice() : [];
    var nowMs = Number(now);
    return arr.sort(function (a, b) {
      var pa = Number((a && a.priority) || 2);
      var pb = Number((b && b.priority) || 2);
      if (pa !== pb) return pa - pb;

      var ta = worstToneRank(a, nowMs);
      var tb = worstToneRank(b, nowMs);
      if (ta !== tb) return tb - ta; // worst tone first

      var ra = earliestResetAt(a);
      var rb = earliestResetAt(b);
      if (ra !== null && rb !== null && ra !== rb) return ra - rb;
      if (ra !== null && rb === null) return -1;
      if (ra === null && rb !== null) return 1;

      return String(a && a.label || '').localeCompare(String(b && b.label || ''));
    });
  }

  // upcomingResets: flattens per-key windows into a single timeline.
  // Auto-zooms the horizon: max(next 5h reset + 1h, furthest reset in 24h),
  // but always include the next 2 resets per window even if outside.
  // Returns ticks ordered by resetAt ASC.
  function upcomingResets(keys, now) {
    var nowMs = Number(now);
    var keysArr = Array.isArray(keys) ? keys : [];
    var allTicks = [];

    for (var i = 0; i < keysArr.length; i++) {
      var k = keysArr[i];
      var windows = (k && k.capacity && k.capacity.windows) || [];
      for (var j = 0; j < windows.length; j++) {
        var w = windows[j];
        if (w.windowName !== 'unified-5h' && w.windowName !== 'unified-7d') continue;
        if (w.resetAt === null || w.resetAt === undefined) continue;
        if (Number(w.resetAt) <= nowMs) continue; // already reset

        var pace = computeWindowPace({
          utilization: w.utilization,
          resetAt: w.resetAt,
          windowName: w.windowName,
          now: nowMs,
        });

        allTicks.push({
          keyLabel: String((k && k.label) || ''),
          windowName: w.windowName,
          resetAt: Number(w.resetAt),
          utilization: w.utilization === null || w.utilization === undefined ? null : Number(w.utilization),
          projectedUtilAtReset: pace.projectedUtilAtReset,
          tone: pace.tone,
          toneRank: pace.toneRank,
        });
      }
    }

    allTicks.sort(function (a, b) { return a.resetAt - b.resetAt; });

    // Horizon: max(next 5h reset + 1h, furthest reset in next 24h).
    var ONE_HOUR = 60 * 60 * 1000;
    var TWENTY_FOUR = 24 * ONE_HOUR;
    var next5h = null;
    var furthestIn24h = nowMs + TWENTY_FOUR;
    var furthestWithin = nowMs;
    for (var t = 0; t < allTicks.length; t++) {
      var tick = allTicks[t];
      if (tick.windowName === 'unified-5h' && next5h === null) next5h = tick.resetAt;
      if (tick.resetAt <= furthestIn24h && tick.resetAt > furthestWithin) furthestWithin = tick.resetAt;
    }
    var horizonFromResets = next5h !== null ? next5h + ONE_HOUR : nowMs + 5 * ONE_HOUR;
    var horizon = Math.max(horizonFromResets, furthestWithin);

    // Always include the next 2 resets per window (even beyond horizon).
    var counts = { 'unified-5h': 0, 'unified-7d': 0 };
    var ticks = [];
    for (var m = 0; m < allTicks.length; m++) {
      var cur = allTicks[m];
      var withinHorizon = cur.resetAt <= horizon;
      var countForcedInclude = counts[cur.windowName] < 2;
      if (withinHorizon || countForcedInclude) {
        ticks.push(cur);
        counts[cur.windowName]++;
      }
    }

    // Compute each tick's x position (0..1) within the displayed range.
    var displayStart = nowMs;
    var displayEnd = ticks.length > 0 ? Math.max(horizon, ticks[ticks.length - 1].resetAt) : horizon;
    var span = Math.max(1, displayEnd - displayStart);
    for (var n = 0; n < ticks.length; n++) {
      ticks[n].position = (ticks[n].resetAt - displayStart) / span;
    }

    return {
      ticks: ticks,
      horizonAt: horizon,
      displayStart: displayStart,
      displayEnd: displayEnd,
    };
  }

  // demoFixture: seven synthetic keys covering every tone bucket and a
  // reset-soon case, for visual QA via ?pace-demo=1. Not used by tests
  // directly — tests use purpose-built fixtures — but kept here so the
  // demo mode is self-contained.
  function demoFixture(now) {
    var nowMs = Number(now);
    function mk(label, priority, windows) {
      return {
        maskedKey: 'sk-ant-***demo-' + label,
        label: label,
        priority: priority,
        isAvailable: true,
        availableAt: 0,
        allowedDays: [0, 1, 2, 3, 4, 5, 6],
        stats: {
          totalRequests: 1234,
          successfulRequests: 1200,
          rateLimitHits: 0,
          errors: 0,
          lastUsedAt: nowMs,
          addedAt: nowMs - 86400000,
          totalTokensIn: 5000000,
          totalTokensOut: 200000,
          totalCacheRead: 0,
          totalCacheCreation: 0,
        },
        capacity: {
          responseCount: 100,
          normalizedHeaderCount: 80,
          lastResponseAt: nowMs,
          lastHeaderAt: nowMs,
          lastUpstreamStatus: 200,
          lastRequestId: 'demo',
          organizationId: 'demo-org',
          representativeClaim: 'unified',
          retryAfterSecs: null,
          shouldRetry: null,
          fallbackAvailable: null,
          fallbackPercentage: null,
          overageStatus: null,
          overageDisabledReason: null,
          latencyMs: 500,
          signalCoverage: [],
          windows: windows,
        },
        capacityHealth: 'healthy',
        recentErrors: 0,
        recentSessions15m: [],
      };
    }
    function win(name, util, resetIn) {
      return {
        windowName: name,
        status: 'allowed',
        utilization: util,
        resetAt: nowMs + resetIn,
        surpassedThreshold: name === 'unified-5h' ? 0.9 : 0.75,
        lastSeenAt: nowMs,
      };
    }
    var H = 60 * 60 * 1000, D = 24 * H;

    return [
      // priority 1 — cold (dim): low util early in window
      mk('demo-dim',     1, [win('unified-5h', 0.05, 4.5 * H), win('unified-7d', 0.10, 5 * D)]),
      // priority 1 — green: on pace
      mk('demo-green',   1, [win('unified-5h', 0.50, 2.5 * H), win('unified-7d', 0.50, 3.5 * D)]),
      // priority 1 — yellow: slightly ahead (1.3x)
      mk('demo-yellow',  1, [win('unified-5h', 0.65, 2.5 * H), win('unified-7d', 0.30, 4 * D)]),
      // priority 2 — orange worst (ratio ~1.7×)
      mk('demo-orange',  2, [win('unified-5h', 0.70, 2.75 * H), win('unified-7d', 0.30, 4 * D)]),
      // priority 2 — red worst (ratio >2×)
      mk('demo-red',     2, [win('unified-5h', 0.90, 4 * H),    win('unified-7d', 0.30, 6 * D)]),
      // priority 3 — reset imminent (fresh budget soon)
      mk('demo-soon',    3, [win('unified-5h', 0.40, 10 * 60 * 1000), win('unified-7d', 0.20, 2 * D)]),
      // priority 3 — behind pace (wasting)
      mk('demo-behind',  3, [win('unified-5h', 0.05, 1 * H),   win('unified-7d', 0.08, 6 * D)]),
    ];
  }

  var api = {
    WINDOW_DURATION_MS: WINDOW_DURATION_MS,
    TONE_RANK: TONE_RANK,
    PACE_THRESHOLDS: PACE_THRESHOLDS,
    DEAD_ZONE: DEAD_ZONE,
    computeWindowPace: computeWindowPace,
    computePoolAggregate: computePoolAggregate,
    sortKeysForDisplay: sortKeysForDisplay,
    upcomingResets: upcomingResets,
    demoFixture: demoFixture,
    worstToneRank: worstToneRank,
    earliestResetAt: earliestResetAt,
    toneFromPace: toneFromPace,
  };

  // Browser: attach to window. Tests: ES-module import via
  // `import { PaceMath } from "../public/pace.js"` or read via IIFE returning
  // an object (handled below for test harnesses).
  if (typeof global !== 'undefined') {
    global.PaceMath = api;
  }
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { PaceMath: api };
  }
})(typeof window !== 'undefined' ? window : (typeof globalThis !== 'undefined' ? globalThis : this));
