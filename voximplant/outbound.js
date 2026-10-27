/** AI Calls — outbound VoxEngine scenario. Secrets are set in the application.
 * Original implementation. No recordings, transfers, or browser-side API keys.
 */
require(Modules.ElevenLabs);

(function () {
  "use strict";
  var started = false;
  var claimed = false;
  var finishing = false;
  var terminated = false;
  var call = null;
  var agent = null;
  var jobId = "";
  var dispatchToken = "";
  var baseUrl = "";
  var internalSecret = "";
  var apiKey = "";
  var config = null;
  var answeredAt = null;
  var sequence = 0;
  var outcome = "unknown";
  var conversationId = "";
  var transcript = [];
  var transcriptChars = 0;
  var transcriptTruncated = false;
  var timers = {};
  var controlFailures = 0;
  var eventChain = Promise.resolve();
  var toolTasks = Object.create(null);
  var optOutTask = null;
  var closeAfterSpeech = false;
  var endReason = "agent_end";

  function log(code) { Logger.write("AI_CALLS " + code); }
  function object(value) { return value && typeof value === "object" && !Array.isArray(value); }
  function text(value, limit) { return typeof value === "string" ? value.slice(0, limit) : ""; }
  function clearTimer(name) {
    if (timers[name] !== undefined) { clearTimeout(timers[name]); delete timers[name]; }
  }
  function timer(name, ms, handler) {
    clearTimer(name);
    timers[name] = setTimeout(function () { delete timers[name]; handler(); }, ms);
  }
  function wait(ms) { return new Promise(function (resolve) { setTimeout(resolve, ms); }); }
  function terminate() {
    if (!terminated) { terminated = true; VoxEngine.terminate(); }
  }
  function safeClose() {
    try { if (agent) agent.close(); } catch (_) {}
  }
  function unpack(event, field) {
    var data = object(event) && object(event.data) ? event.data : event;
    if (object(data) && object(data.payload)) data = data.payload;
    if (object(data) && object(data[field])) data = data[field];
    return object(data) ? data : {};
  }
  function parseJSON(raw) {
    try { return JSON.parse(raw); } catch (_) { return null; }
  }
  function url(suffix) { return baseUrl + "/internal/calls/" + encodeURIComponent(jobId) + suffix; }
  async function request(method, suffix, body) {
    var options = {
      method: method,
      timeout: 5,
      connectionTimeout: 3,
      headers: {
        "Content-Type": "application/json",
        "X-Internal-Secret": internalSecret,
        "X-Dispatch-Token": dispatchToken,
      },
      enableSystemLog: false,
      externalLogging: false,
    };
    if (body !== undefined) options.postData = JSON.stringify(body);
    try {
      var response = await Net.httpRequestAsync(url(suffix), options);
      return { code: Number(response.code), body: parseJSON(response.text || "") };
    } catch (_) { return { code: -1, body: null }; }
  }
  // Exactly the same envelope is retried; the server deduplicates event_id.
  async function deliver(envelope) {
    for (var attempt = 0; attempt < 3; attempt += 1) {
      var response = await request("POST", "/events", envelope);
      if (response.code >= 200 && response.code < 300 && response.body && response.body.stored === true) return true;
      if (response.code >= 400 && response.code < 500 && response.code !== 429) break;
      if (attempt < 2) await wait(250 * (attempt + 1));
    }
    log("event_delivery_failed");
    return false;
  }
  function sendEvent(type, payload) {
    // Snapshot nested transcript data now: retries must have an identical fingerprint.
    var snapshot = JSON.parse(JSON.stringify(payload || {}));
    var envelope = { event_id: jobId + ":" + (++sequence), event_type: type, payload: snapshot };
    var pending = eventChain.then(function () { return deliver(envelope); });
    eventChain = pending.then(function () {}, function () {});
    return pending;
  }
  function duration() { return answeredAt === null ? 0 : Math.max(0, Math.floor((Date.now() - answeredAt) / 1000)); }
  // Stop billable media now; wait for in-flight tools and the final receipt before terminating.
  async function finish(status, errorCode) {
    if (finishing) return;
    finishing = true;
    var endedDuration = duration();
    Object.keys(timers).forEach(clearTimer);
    try { if (call) call.hangup(); } catch (_) {}
    safeClose();
    await Promise.allSettled(Object.keys(toolTasks).map(function (key) { return toolTasks[key]; }));
    if (optOutTask) await optOutTask;
    if (claimed) {
      var payload = {
        status: status,
        outcome: outcome,
        duration_sec: endedDuration,
        transcript: transcript,
        conversation_id: conversationId || null,
        error_code: errorCode || null,
        transcript_truncated: transcriptTruncated,
      };
      // An absent receipt remains uncertain in the backend; do not invent success.
      var stored = await sendEvent("ended", payload);
      if (!stored) log("final_receipt_missing_check_provider");
    }
    terminate();
  }
  function failClosed(status, code) { void finish(status, code); }
  function toolResult(id, value, failed) {
    if (!agent || finishing) return;
    try {
      agent.clientToolResult({ tool_call_id: id, result: JSON.stringify(value), is_error: !!failed });
    } catch (_) { failClosed("failed", "tool_ack_failed"); }
  }
  function endWhenSpoken(reason) {
    if (finishing) return;
    closeAfterSpeech = true;
    endReason = reason;
    timer("goodbye", 4000, function () { failClosed("completed", reason); });
  }
  async function persistOptOut() {
    if (optOutTask) return optOutTask;
    optOutTask = (async function () {
      var stored = await sendEvent("opt_out", { reason: "user_request", outcome: "not_interested" });
      if (stored) outcome = "not_interested";
      return stored;
    })();
    return optOutTask;
  }
  function appendTranscript(role, value) {
    if (finishing) return;
    if (typeof value === "string" && value.length > 1000) transcriptTruncated = true;
    var line = text(value, 1000).trim();
    if (!line) return;
    if (transcript.length >= 100 || transcriptChars + line.length > 30000) { transcriptTruncated = true; return; }
    transcript.push({ role: role, text: line });
    transcriptChars += line.length;
  }
  function explicitOptOut(value) {
    // Narrow phrases only; the agent handles less literal requests with opt_out.
    return /\b(do not call|don't call|stop calling|remove me from (your )?(call )?list)\b/i.test(value) ||
      /(?:не\s+звоните|больше\s+не\s+звонить|перестаньте\s+звонить|удалите\s+мой\s+номер)/i.test(value);
  }
  function onTool(event) {
    if (finishing) return;
    var payload = unpack(event, "client_tool_call");
    var name = text(payload.tool_name || payload.toolName || payload.name, 64);
    var id = text(payload.tool_call_id || payload.toolCallId || payload.id, 256);
    // A made-up ID cannot acknowledge a provider request. Fail closed without doing an action.
    if (!name || !id) { failClosed("failed", "malformed_tool_event"); return; }
    if (Object.prototype.hasOwnProperty.call(toolTasks, id)) return;
    if (Object.keys(toolTasks).length >= 32) { failClosed("failed", "tool_limit"); return; }
    if (optOutTask && name !== "opt_out" && name !== "end_call") {
      toolResult(id, { error: "opt_out_in_progress" }, true); return;
    }
    var args = payload.parameters || payload.arguments || payload.args || {};
    if (typeof args === "string") args = parseJSON(args);
    if (!object(args)) { toolResult(id, { error: "invalid_arguments" }, true); return; }
    toolTasks[id] = (async function () {
      if (name === "opt_out") {
        var saved = await persistOptOut();
        toolResult(id, saved ? { saved: true, do_not_call: true } : { error: "not_saved" }, !saved);
        if (saved) endWhenSpoken("opt_out");
        else failClosed("failed", "opt_out_persistence_failed");
      } else if (name === "save_outcome") {
        var allowed = ["interested", "not_interested", "callback", "wrong_number", "unknown"];
        if (allowed.indexOf(args.outcome) === -1) { toolResult(id, { error: "invalid_outcome" }, true); return; }
        var savedOutcome = await sendEvent("heartbeat", { outcome: args.outcome });
        if (savedOutcome) outcome = args.outcome;
        toolResult(id, savedOutcome ? { saved: true, outcome: outcome } : { error: "not_saved" }, !savedOutcome);
        if (!savedOutcome) failClosed("failed", "outcome_persistence_failed");
      } else if (name === "end_call") {
        toolResult(id, { ending: true }, false);
        endWhenSpoken("agent_end");
      } else {
        toolResult(id, { error: "unknown_tool" }, true);
      }
    })().catch(function () { failClosed("failed", "tool_handler_failed"); });
  }
  async function checkControl() {
    if (finishing) return;
    var response = await request("GET", "/control");
    if (finishing) return;
    if (response.code === 200 && response.body && typeof response.body.stop === "boolean") {
      controlFailures = 0;
      if (response.body.stop) { failClosed("cancelled", "stop_requested"); return; }
      if (!(await sendEvent("heartbeat", { duration_sec: duration() }))) {
        failClosed("failed", "heartbeat_delivery_failed"); return;
      }
    } else {
      controlFailures += 1;
      if (controlFailures >= 2) { failClosed("failed", "control_unavailable"); return; }
    }
    if (!finishing) timer("control", 5000, function () { void checkControl(); });
  }
  async function onConnected() {
    if (finishing || answeredAt !== null) return;
    answeredAt = Date.now();
    clearTimer("ringing");
    timer("duration", config.max_duration_sec * 1000, function () { failClosed("completed", "duration_limit"); });
    try {
      if (!(await sendEvent("answered", {}))) { failClosed("failed", "answered_delivery_failed"); return; }
      if (finishing) return;
      timer("ai_connect", 15000, function () { failClosed("failed", "ai_connect_timeout"); });
      var connectedAgent = await ElevenLabs.createAgentsClient({
        xiApiKey: apiKey,
        agentId: config.agent_id,
        includeConversationId: true,
        privacy: true,
        trace: false,
        onWebSocketClose: function () {
          if (!finishing) failClosed("failed", "ai_connection_closed");
        },
      });
      if (finishing) { try { connectedAgent.close(); } catch (_) {} return; }
      agent = connectedAgent;
      clearTimer("ai_connect");
      agent.addEventListener(ElevenLabs.AgentsEvents.Interruption, function () {
        try { agent.clearMediaBuffer(); } catch (_) { failClosed("failed", "barge_in_failed"); }
      });
      agent.addEventListener(ElevenLabs.AgentsEvents.ClientToolCall, onTool);
      agent.addEventListener(ElevenLabs.AgentsEvents.ConversationInitiationMetadata, function (event) {
        if (finishing) return;
        var data = unpack(event, "conversation_initiation_metadata_event");
        conversationId = text(data.conversation_id || data.conversationId, 160);
      });
      agent.addEventListener(ElevenLabs.AgentsEvents.UserTranscript, function (event) {
        var data = unpack(event, "user_transcription_event");
        var value = data.user_transcript || data.transcript || data.text;
        appendTranscript("user", value);
        if (!finishing && explicitOptOut(text(value, 4000))) {
          void persistOptOut().then(function (saved) {
            if (saved) endWhenSpoken("opt_out");
            else failClosed("failed", "opt_out_persistence_failed");
          });
        }
      });
      agent.addEventListener(ElevenLabs.AgentsEvents.AgentResponse, function (event) {
        var data = unpack(event, "agent_response_event");
        appendTranscript("assistant", data.agent_response || data.response || data.text);
      });
      if (ElevenLabs.AgentsEvents.AgentResponseCorrection) {
        agent.addEventListener(ElevenLabs.AgentsEvents.AgentResponseCorrection, function (event) {
          if (finishing) return;
          var data = unpack(event, "agent_response_correction_event");
          var original = data.corrected_agent_response || data.agent_response || data.text;
          var corrected = text(original, 1000);
          if (typeof original === "string" && original.length > 1000) transcriptTruncated = true;
          if (!corrected) return;
          for (var i = transcript.length - 1; i >= 0; i -= 1) {
            if (transcript[i].role === "assistant") {
              var available = Math.max(0, 30000 - transcriptChars + transcript[i].text.length);
              if (corrected.length > available) { corrected = corrected.slice(0, available); transcriptTruncated = true; }
              transcriptChars += corrected.length - transcript[i].text.length;
              transcript[i].text = corrected; break;
            }
          }
        });
      }
      agent.addEventListener(ElevenLabs.AgentsEvents.WebSocketError, function () { failClosed("failed", "ai_websocket_error"); });
      if (ElevenLabs.Events && ElevenLabs.Events.WebSocketMediaEnded) {
        agent.addEventListener(ElevenLabs.Events.WebSocketMediaEnded, function () {
          if (closeAfterSpeech) failClosed("completed", endReason);
        });
      }
      // This native method sends conversation_initiation_client_data, not session.update.
      // Pass business values only; never include API keys or the dispatch token.
      agent.conversationInitiationClientData({ dynamic_variables: config.dynamic_variables });
      VoxEngine.sendMediaBetween(call, agent);
    } catch (_) { failClosed("failed", "ai_setup_failed"); }
  }
  function validConfig(value) {
    if (!object(value) || value.job_id !== jobId) return false;
    if (!/^\+[1-9]\d{7,14}$/.test(value.phone) || !/^\+[1-9]\d{7,14}$/.test(value.caller_id)) return false;
    if (!/^agent_[A-Za-z0-9_-]+$/.test(value.agent_id)) return false;
    if (!Number.isInteger(value.max_duration_sec) || value.max_duration_sec < 10 || value.max_duration_sec > 3600) return false;
    if (!Number.isInteger(value.ringing_timeout_sec) || value.ringing_timeout_sec < 10 || value.ringing_timeout_sec > 120) return false;
    if (!object(value.dynamic_variables)) return false;
    var allowed = ["product_name", "product_facts", "call_goal", "contact_name"];
    if (Object.keys(value.dynamic_variables).some(function (key) { return allowed.indexOf(key) === -1; })) return false;
    return allowed.every(function (key) { return typeof value.dynamic_variables[key] === "string" && value.dynamic_variables[key].length <= 12000; });
  }
  VoxEngine.addEventListener(AppEvents.Started, async function () {
    if (started) return;
    started = true;
    try {
      var data = parseJSON(VoxEngine.customData() || "");
      baseUrl = text(VoxEngine.getSecretValue("AI_CALLS_BASE_URL"), 2048).replace(/\/$/, "");
      internalSecret = text(VoxEngine.getSecretValue("AI_CALLS_INTERNAL_SECRET"), 512);
      apiKey = text(VoxEngine.getSecretValue("ELEVENLABS_API_KEY"), 512);
      if (!object(data) || typeof data.job_id !== "string" || !/^[A-Za-z0-9_-]{1,100}$/.test(data.job_id) ||
          typeof data.dispatch_token !== "string" || !/^[A-Za-z0-9_-]{16,200}$/.test(data.dispatch_token) ||
          !/^https:\/\/[^/?#@\s]+(?::\d+)?(?:\/[^?#\s]*)?$/.test(baseUrl) || !internalSecret || !apiKey) {
        log("invalid_bootstrap"); terminate(); return;
      }
      jobId = data.job_id;
      dispatchToken = data.dispatch_token;
      // Claim is intentionally NEVER retried. A lost response is not permission to dial.
      var claim = await request("POST", "/claim", { dispatch_token: dispatchToken });
      if (claim.code !== 200) { log("claim_rejected_or_uncertain_no_dial"); terminate(); return; }
      claimed = true;
      if (!validConfig(claim.body)) { await finish("failed", "invalid_claim_config"); return; }
      config = claim.body;
      // Re-check pause/DNC after claiming and immediately before the only dial operation.
      var control = await request("GET", "/control");
      if (control.code !== 200 || !control.body || control.body.stop !== false) {
        await finish("cancelled", "pre_dial_stop_or_unavailable"); return;
      }
      call = VoxEngine.callPSTN(config.phone, config.caller_id);
      call.addEventListener(CallEvents.Connected, function () { void onConnected(); });
      call.addEventListener(CallEvents.Disconnected, function () {
        failClosed(answeredAt === null ? "failed" : "completed", answeredAt === null ? "disconnected_before_answer" : "remote_hangup");
      });
      call.addEventListener(CallEvents.Failed, function (event) {
        var code = Number(event && event.code);
        var status = code === 486 || code === 600 ? "busy" : code === 408 ? "no_answer" : "failed";
        failClosed(status, Number.isFinite(code) ? "sip_" + code : "call_failed");
      });
      timer("ringing", config.ringing_timeout_sec * 1000, function () { failClosed("no_answer", "ringing_timeout"); });
      timer("control", 5000, function () { void checkControl(); });
      if (!(await sendEvent("ringing", {}))) failClosed("failed", "ringing_delivery_failed");
    } catch (_) {
      if (claimed) await finish("failed", "scenario_start_failed");
      else { log("startup_failed_no_dial"); terminate(); }
    }
  });
})();
