const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../voximplant/outbound.js'), 'utf8');
const defer = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return {promise, resolve}; };
const flush = async () => { for (let i = 0; i < 40; i++) await Promise.resolve(); };
function emitter() {
  const events = new Map();
  return {
    addEventListener(name, fn) { const listeners = events.get(name) || []; listeners.push(fn); events.set(name, listeners); },
    async emit(name, data = {}) { await Promise.all((events.get(name) || []).map(fn => fn(data))); await flush(); },
  };
}
function harness(options = {}) {
  const requests = [], dials = [], sent = [], logs = [], timers = new Map(), order = [];
  let terminated = false, now = 10000, nextTimer = 0, closes = 0, clears = 0, media = 0, hangups = 0;
  const app = emitter(), phone = emitter(), ai = emitter();
  const config = { job_id: 'job-123', phone: '+15551234567', caller_id: '+15557654321', agent_id: 'agent_test', max_duration_sec: 180, ringing_timeout_sec: 30,
    dynamic_variables: { product_name:'Тест', product_facts:'Тестовые сведения', call_goal:'Проверка', contact_name:'Тест' }, ...options.config };
  const reply = (code, body) => ({code, text: JSON.stringify(body)});
  const defaultResponse = req => req.suffix === '/claim' ? reply(200, config) : req.suffix === '/control' ? reply(200, {stop:false}) : reply(200, {stored:true});
  let createOptions;
  Object.assign(phone, { hangup() { hangups++; order.push('hangup'); } });
  Object.assign(ai, {
    close() { closes++; }, clearMediaBuffer() { clears++; },
    conversationInitiationClientData(value) { order.push('variables'); sent.push({kind:'variables', value: JSON.parse(JSON.stringify(value))}); },
    clientToolResult(value) { order.push('tool_ack'); sent.push({kind:'tool', value:JSON.parse(JSON.stringify(value))}); },
  });
  const names = new Proxy({}, {get: (_, name) => String(name)});
  const context = {
    Modules: {ElevenLabs:'elevenlabs'},
    require(name) { assert.equal(name, 'elevenlabs'); },
    Logger: {write(value) { logs.push(value); }},
    AppEvents: names, CallEvents: names,
    ElevenLabs: { AgentsEvents:names, Events:names, async createAgentsClient(value) { createOptions=value; return options.createAgent ? options.createAgent(ai) : ai; } },
    Net: { async httpRequestAsync(url, value) {
      assert.equal(typeof value, 'object'); assert.equal(value.headers['X-Dispatch-Token'], 'dispatch_token_1234567890');
      assert.equal(value.headers['X-Internal-Secret'], 'internal_secret');
      assert(!url.includes('token='));
      const req = {url, suffix:url.slice(url.lastIndexOf('/')), method:value.method, body:value.postData ? JSON.parse(value.postData) : undefined};
      requests.push(req);
      order.push(req.body?.event_type || req.suffix);
      const result = options.respond ? await options.respond(req, reply, defaultResponse) : defaultResponse(req);
      return result === undefined ? defaultResponse(req) : result;
    } },
    VoxEngine: {
      addEventListener: app.addEventListener,
      customData: () => options.customData || JSON.stringify({job_id:'job-123',dispatch_token:'dispatch_token_1234567890'}),
      getSecretValue(name) { return {AI_CALLS_BASE_URL:'https://calls.example.test',AI_CALLS_INTERNAL_SECRET:'internal_secret',ELEVENLABS_API_KEY:'eleven_secret'}[name]; },
      callPSTN(...args) { order.push('dial'); dials.push(args); return phone; },
      sendMediaBetween() { order.push('media'); media++; },
      terminate() { order.push('terminate'); terminated=true; },
    },
    Date: class extends Date { static now() { return now; } },
    setTimeout(fn, ms) { const id=++nextTimer; timers.set(id,{fn,at:now+ms,ms}); return id; },
    clearTimeout(id) { timers.delete(id); },
  };
  vm.createContext(context); vm.runInContext(source, context);
  return {
    requests, dials, sent, logs, timers, order, phone, ai, config,
    get terminated() {return terminated;}, get hangups(){return hangups;}, get closes(){return closes;}, get clears(){return clears;}, get media(){return media;}, get createOptions(){return createOptions;},
    start: () => app.emit('Started'),
    async connected() { await phone.emit('Connected'); },
    async tool(name,id,args={}) { await ai.emit('ClientToolCall',{data:{client_tool_call:{tool_name:name,tool_call_id:id,parameters:args}}}); },
    events(type) {return requests.filter(r=>r.body?.event_type === type);},
    async advance(ms) {
      now += ms;
      for (const [id, entry] of [...timers]) if (entry.at <= now && timers.has(id)) {timers.delete(id);entry.fn();}
      await flush();
    },
  };
}

test('rejected or uncertain claim never dials or retries claim', async () => {
  for (const code of [409,401,503,-7]) {
    const h=harness({respond:(req,reply)=>req.suffix==='/claim'?reply(code,{}):undefined});
    await h.start();
    assert.equal(h.dials.length,0); assert.equal(h.terminated,true);
    assert.equal(h.requests.filter(r=>r.suffix==='/claim').length,1);
    assert.equal(h.events('ended').length,0);
  }
});

test('invalid bootstrap and invalid claim config cannot dial', async () => {
  const bad=harness({customData:'not-json'}); await bad.start(); assert.equal(bad.requests.length,0);assert.equal(bad.dials.length,0);
  const invalid=harness({config:{phone:'001234',dynamic_variables:{secret__key:'forbidden'}}});
  await invalid.start();assert.equal(invalid.dials.length,0);assert.equal(invalid.events('ended')[0].body.payload.status,'failed');
});

test('pause between claim and dial cancels without calling', async () => {
  const h=harness({respond:(req,reply)=>req.suffix==='/control'?reply(200,{stop:true}):undefined});
  await h.start();assert.equal(h.dials.length,0);assert.equal(h.events('ended')[0].body.payload.status,'cancelled');
});

test('duplicate Started cannot claim or dial twice; initialization precedes media', async () => {
  const h=harness();await Promise.all([h.start(),h.start()]);await h.connected();await h.connected();
  assert.equal(h.dials.length,1);assert.equal(h.requests.filter(r=>r.suffix==='/claim').length,1);
  assert.equal(h.media,1);assert(h.order.indexOf('variables')<h.order.indexOf('media'));
  assert.deepEqual(h.sent[0].value.dynamic_variables,h.config.dynamic_variables);
  assert.equal(h.createOptions.includeConversationId,true);
  assert(h.logs.every(x=>!x.includes('eleven_secret')&&!x.includes('internal_secret')));
});

test('ring timeout is no_answer and only one final event', async () => {
  const h=harness();await h.start();await h.advance(30000);await h.phone.emit('Failed',{code:487});await h.phone.emit('Disconnected');
  assert.equal(h.events('ended').length,1);assert.equal(h.events('ended')[0].body.payload.status,'no_answer');assert(h.terminated);
});

test('busy does not become completed', async () => {
  const h=harness();await h.start();await h.phone.emit('Failed',{code:486});await h.phone.emit('Disconnected');
  assert.equal(h.events('ended')[0].body.payload.status,'busy');assert.equal(h.events('ended')[0].body.payload.outcome,'unknown');
});

test('metadata, transcript, correction, barge-in and actual outcome survive finalization', async () => {
  const h=harness();await h.start();await h.connected();
  await h.ai.emit('ConversationInitiationMetadata',{data:{payload:{conversation_initiation_metadata_event:{conversation_id:'conv_real'}}}});
  await h.ai.emit('UserTranscript',{data:{user_transcription_event:{user_transcript:'Мне интересно'}}});
  await h.ai.emit('AgentResponse',{data:{agent_response_event:{agent_response:'Вот длинный ответ'}}});
  await h.ai.emit('AgentResponseCorrection',{data:{agent_response_correction_event:{corrected_agent_response:'Вот'}}});
  await h.ai.emit('Interruption');assert.equal(h.clears,1);
  await h.tool('save_outcome','tool-1',{outcome:'interested'});
  assert.equal(JSON.parse(h.sent.find(x=>x.kind==='tool').value.result).saved,true);
  await h.phone.emit('Disconnected');
  const result=h.events('ended')[0].body.payload;
  assert.equal(result.conversation_id,'conv_real');assert.equal(result.outcome,'interested');
  assert.deepEqual(result.transcript,[{role:'user',text:'Мне интересно'},{role:'assistant',text:'Вот'}]);
});

test('final HTTP is awaited after hanging up and before terminate', async () => {
  const final=defer();
  const h=harness({respond:(req)=>req.body?.event_type==='ended'?final.promise:undefined});
  await h.start();await h.connected();await h.phone.emit('Disconnected');
  assert.equal(h.hangups,1);assert.equal(h.terminated,false);
  final.resolve({code:200,text:'{"stored":true}'});await flush();assert.equal(h.terminated,true);
  assert(h.order.indexOf('hangup')<h.order.indexOf('ended'));assert(h.order.indexOf('ended')<h.order.indexOf('terminate'));
});

test('temporary final failure retries same event id without second dial', async () => {
  let count=0;
  const h=harness({respond:(req,reply)=>req.body?.event_type==='ended'?reply(++count===1?503:200,{stored:count>1}):undefined});
  await h.start();await h.connected();await h.phone.emit('Disconnected');
  assert.equal(h.terminated,false);await h.advance(250);
  assert.equal(h.events('ended').length,2);
  assert.deepEqual(h.events('ended')[0].body,h.events('ended')[1].body);
  assert.equal(h.dials.length,1);assert.equal(h.terminated,true);
});

test('opt_out persists before success acknowledgement and duplicate tool executes once', async () => {
  const saved=defer();
  const h=harness({respond:req=>req.body?.event_type==='opt_out'?saved.promise:undefined});
  await h.start();await h.connected();await h.tool('opt_out','opt-1');await h.tool('opt_out','opt-1');
  assert.equal(h.events('opt_out').length,1);assert.equal(h.sent.filter(x=>x.kind==='tool').length,0);
  saved.resolve({code:200,text:'{"stored":true}'});await flush();
  assert.equal(h.sent.filter(x=>x.kind==='tool').length,1);
  assert.equal(JSON.parse(h.sent.find(x=>x.kind==='tool').value.result).do_not_call,true);
  await h.ai.emit('WebSocketMediaEnded');assert.equal(h.events('ended')[0].body.payload.outcome,'not_interested');
});

test('failed opt_out does not acknowledge success', async () => {
  const h=harness({respond:(req,reply)=>req.body?.event_type==='opt_out'?reply(401,{}):undefined});
  await h.start();await h.connected();await h.tool('opt_out','opt-2');await flush();
  const ack=h.sent.find(x=>x.kind==='tool').value;assert.equal(ack.is_error,true);assert.equal(JSON.parse(ack.result).saved,undefined);
  assert.equal(h.events('ended')[0].body.payload.status,'failed');assert(h.terminated);
});

test('missing tool id never creates fabricated id or mutates suppression', async () => {
  const h=harness();await h.start();await h.connected();await h.tool('opt_out',undefined);
  assert.equal(h.events('opt_out').length,0);assert.equal(h.sent.filter(x=>x.kind==='tool').length,0);
  assert.equal(h.events('ended')[0].body.payload.error_code,'malformed_tool_event');assert(h.terminated);
});

test('stop works while AI connects and late AI client is closed without audio', async () => {
  const pending=defer();let checks=0;
  const h=harness({createAgent:()=>pending.promise,respond:(req,reply)=>req.suffix==='/control'?reply(200,{stop:++checks>1}):undefined});
  await h.start();await h.connected();await h.advance(5000);
  assert.equal(h.events('ended')[0].body.payload.status,'cancelled');assert.equal(h.hangups,1);
  pending.resolve(h.ai);await flush();assert.equal(h.media,0);assert.equal(h.closes,1);
});

test('unknown tools and invalid outcomes cannot claim success', async () => {
  const h=harness();await h.start();await h.connected();await h.tool('charge_card','bad-1');await h.tool('save_outcome','bad-2',{outcome:'sale_guaranteed'});
  assert.equal(h.sent.filter(x=>x.kind==='tool').length,2);
  assert(h.sent.filter(x=>x.kind==='tool').every(x=>x.value.is_error));
  await h.phone.emit('Disconnected');assert.equal(h.events('ended')[0].body.payload.outcome,'unknown');
});

test('literal opt-out in transcript persists without depending on LLM tool choice', async () => {
  const h=harness();await h.start();await h.connected();
  await h.ai.emit('UserTranscript',{data:{user_transcription_event:{user_transcript:'Больше не звоните, пожалуйста'}}});
  assert.equal(h.events('opt_out').length,1);await h.tool('opt_out','opt-after-transcript');assert.equal(h.events('opt_out').length,1);
});

test('two failed controls stop the call; no continued dialing', async () => {
  let reads=0;
  const h=harness({respond:(req,reply)=>req.suffix==='/control'&&++reads>1?reply(503,{}):undefined});
  await h.start();await h.connected();await h.advance(5000);assert.equal(h.terminated,false);
  await h.advance(5000);assert.equal(h.events('ended')[0].body.payload.error_code,'control_unavailable');assert(h.terminated);assert.equal(h.dials.length,1);
});

test('duration limit closes connected call without inventing a sales outcome', async () => {
  const h=harness({config:{max_duration_sec:10}});await h.start();await h.connected();await h.advance(10000);
  assert.equal(h.events('ended')[0].body.payload.error_code,'duration_limit');assert.equal(h.events('ended')[0].body.payload.outcome,'unknown');assert(h.terminated);
});

test('final delivery is bounded when backend remains unavailable', async () => {
  const h=harness({respond:(req,reply)=>req.body?.event_type==='ended'?reply(503,{}):undefined});
  await h.start();await h.connected();await h.phone.emit('Disconnected');await h.advance(250);await h.advance(500);
  assert.equal(h.events('ended').length,3);assert(h.terminated);
  assert.equal(new Set(h.events('ended').map(x=>x.body.event_id)).size,1);
  assert(h.logs.includes('AI_CALLS final_receipt_missing_check_provider'));
});

test('transcript cap is explicit and no text is logged', async () => {
  const h=harness();await h.start();await h.connected();
  await h.ai.emit('UserTranscript',{data:{user_transcription_event:{user_transcript:'я'.repeat(1500)}}});
  for(let i=0;i<105;i++) await h.ai.emit('AgentResponse',{data:{agent_response_event:{agent_response:'Тест '+i}}});
  await h.phone.emit('Disconnected');const p=h.events('ended')[0].body.payload;
  assert.equal(p.transcript_truncated,true);assert.equal(p.transcript.length,100);assert.equal(p.transcript[0].text.length,1000);
  assert(h.logs.every(x=>!x.includes('Тест')));
});

test('opt-out still finishes persistence when peer disconnects before its receipt', async () => {
  const saved=defer();const h=harness({respond:req=>req.body?.event_type==='opt_out'?saved.promise:undefined});
  await h.start();await h.connected();await h.tool('opt_out','race-opt');await h.phone.emit('Disconnected');
  assert.equal(h.terminated,false);assert.equal(h.events('ended').length,0);
  saved.resolve({code:200,text:'{"stored":true}'});await flush();
  assert(h.terminated);assert.equal(h.events('ended')[0].body.payload.outcome,'not_interested');
  assert.equal(h.sent.filter(x=>x.kind==='tool').length,0);
});

test('late transcript events cannot change an already-sent final retry envelope', async () => {
  let attempts=0;
  const h=harness({respond:(req,reply)=>req.body?.event_type==='ended'?reply(++attempts===1?503:200,{stored:attempts>1}):undefined});
  await h.start();await h.connected();
  await h.ai.emit('AgentResponse',{data:{agent_response_event:{agent_response:'Исходная реплика'}}});
  await h.phone.emit('Disconnected');
  await h.ai.emit('AgentResponseCorrection',{data:{agent_response_correction_event:{corrected_agent_response:'Позднее исправление'}}});
  await h.ai.emit('UserTranscript',{data:{user_transcription_event:{user_transcript:'Поздняя реплика'}}});
  await h.advance(250);
  assert.deepEqual(h.events('ended')[0].body,h.events('ended')[1].body);
  assert.deepEqual(h.events('ended')[0].body.payload.transcript,[{role:'assistant',text:'Исходная реплика'}]);
});

test('final duration excludes waiting for a pending opt-out receipt after hangup', async () => {
  const saved=defer();const h=harness({respond:req=>req.body?.event_type==='opt_out'?saved.promise:undefined});
  await h.start();await h.connected();await h.advance(2000);
  await h.tool('opt_out','duration-race');await h.phone.emit('Disconnected');
  await h.advance(8000);assert.equal(h.terminated,false);
  saved.resolve({code:200,text:'{"stored":true}'});await flush();
  assert.equal(h.events('ended')[0].body.payload.duration_sec,2);assert(h.terminated);
});
