"use strict";
let token = "",
  pollTimer,
  config,
  refreshInProgress = false,
  sessionGeneration = 0;
const $ = (id) => document.getElementById(id);
const statusNames = {
  queued: "В очереди",
  dispatching: "Запускается",
  starting: "Подключается",
  ringing: "Идёт вызов",
  answered: "Разговор",
  completed: "Завершён",
  busy: "Занято",
  no_answer: "Нет ответа",
  failed: "Ошибка",
  cancelled: "Остановлен",
  unknown: "Неизвестно",
};
const outcomeNames = {
  interested: "Есть интерес",
  not_interested: "Не интересно",
  callback: "Нужен перезвон",
  wrong_number: "Не тот человек",
  unknown: "Не определён",
};
function el(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}
function message(text) {
  $("message").textContent = text;
  $("message").hidden = false;
  clearTimeout(message.timer);
  message.timer = setTimeout(() => ($("message").hidden = true), 6500);
}
async function api(path, options = {}) {
  const requestToken = token;
  const response = await fetch(path, {
    ...options,
    headers: {
      Authorization: "Bearer " + requestToken,
      "Content-Type": "application/json",
      ...options.headers,
    },
  });
  let data;
  try {
    data = await response.json();
  } catch {
    throw new Error("Сервер вернул неожиданный ответ");
  }
  if (!response.ok) {
    if (response.status === 401 && requestToken === token) logout();
    throw new Error(
      typeof data.detail === "string"
        ? data.detail
        : data.errors
          ? "CSV не импортирован: проверьте строки " +
            data.errors.map((x) => x.row).join(", ")
          : "Проверьте заполнение полей",
    );
  }
  return data;
}
const post = (url, body) =>
  api(url, { method: "POST", body: JSON.stringify(body) });
function logout() {
  sessionGeneration++;
  token = "";
  clearInterval(pollTimer);
  $("workspace").hidden = true;
  $("login").hidden = false;
  $("token").value = "";
  $("contacts").replaceChildren();
  $("calls").replaceChildren();
  $("detail-content").replaceChildren();
  $("suppressions").replaceChildren();
  $("detail").close();
}
async function refresh() {
  if (refreshInProgress || !token) return;
  refreshInProgress = true;
  const generation = sessionGeneration;
  try {
    const [settings, contacts, calls, suppressions] = await Promise.all([
      api("/api/config"),
      api("/api/contacts"),
      api("/api/calls"),
      api("/api/suppressions"),
    ]);
    if (generation !== sessionGeneration || !token) return;
    config = settings;
    const live = settings.settings.mode === "live";
    $("mode").textContent = live
      ? "LIVE · реальные звонки"
      : "MOCK · без звонков";
    $("mode").className = "tag " + (live ? "live" : "mock");
    $("mode-notice").textContent = live
      ? "Реальный режим: запуск очереди инициирует платные звонки вашим контактам."
      : "Тестовый режим. Телефоны не набираются, разговоры и результаты ниже — симуляция.";
    $("product-name").textContent =
      settings.product?.name || "Настройте ваш продукт";
    $("limits").textContent =
      `До ${settings.settings.daily_attempt_limit} попыток за сутки UTC · ${settings.settings.max_concurrent} одновременно · окно ${settings.settings.call_window_start}:00–${settings.settings.call_window_end}:00 по времени контакта`;
    $("config-notice").hidden = !settings.config_error;
    $("config-notice").textContent = settings.config_error || "";
    const paused = calls.queue.paused;
    $("queue-title").textContent = paused
      ? "Очередь на паузе"
      : "Помощник работает";
    $("queue-note").textContent = paused
      ? "Новые вызовы не начинаются. Активные останавливаются при ближайшей проверке связи."
      : "Звонки выполняются автоматически в пределах окна и лимитов.";
    $("resume").disabled = !paused || !!settings.config_error;
    $("pause").disabled = paused;
    renderContacts(contacts.items);
    renderCalls(calls.items);
    $("suppressions").replaceChildren(
      ...suppressions.items.map((x) => el("p", `${x.phone} — ${x.reason}`)),
    );
  } catch (e) {
    message(e.message);
  } finally {
    refreshInProgress = false;
  }
}
function renderContacts(items) {
  $("contacts").replaceChildren();
  if (!items.length) {
    $("contacts").append(
      el(
        "p",
        "Контактов пока нет. Добавьте своего тестового собеседника.",
        "hint",
      ),
    );
    return;
  }
  for (const c of items) {
    const row = el("div", undefined, "contact"),
      copy = el("div");
    copy.append(
      el("strong", c.name || "Без имени"),
      el("small", c.phone + " · " + c.timezone),
    );
    const b = el(
      "button",
      c.suppressed ? "Не звонить" : "В очередь",
      "secondary",
    );
    b.disabled = !!c.suppressed;
    b.onclick = async () => {
      b.disabled = true;
      try {
        const scheduled = $("schedule").value;
        await post("/api/calls", {
          contact_id: c.id,
          idempotency_key: crypto.randomUUID(),
          not_before: scheduled ? new Date(scheduled).toISOString() : null,
        });
        message("Звонок добавлен в очередь");
        await refresh();
      } catch (e) {
        message(e.message);
        b.disabled = false;
      }
    };
    row.append(copy, b);
    $("contacts").append(row);
  }
}
function renderCalls(items) {
  $("calls").replaceChildren();
  if (!items.length) {
    const row = el("tr"),
      cell = el("td", "Звонков пока нет");
    cell.colSpan = 5;
    row.append(cell);
    $("calls").append(row);
    return;
  }
  for (const c of items) {
    const row = el("tr"),
      person = el("td");
    person.append(
      el("strong", c.name || c.phone),
      el("small", c.phone + (c.mode === "mock" ? " · тест" : "")),
    );
    const status = el("td");
    status.append(
      el("span", statusNames[c.status] || c.status, "status " + c.status),
    );
    const actions = el("div", undefined, "actions"),
      more = el("button", "Открыть", "secondary");
    more.onclick = () => showDetail(c.id);
    actions.append(more);
    if (
      ["queued", "dispatching", "starting", "ringing", "answered"].includes(
        c.status,
      )
    ) {
      const stop = el("button", "Стоп", "danger");
      stop.onclick = () => act(() => post(`/api/calls/${c.id}/stop`, {}));
      actions.append(stop);
    }
    const last = el("td");
    last.append(actions);
    row.append(
      person,
      status,
      el("td", outcomeNames[c.outcome] || "—"),
      el("td", c.duration_sec == null ? "—" : c.duration_sec + " с"),
      last,
    );
    $("calls").append(row);
  }
}
async function act(fn) {
  try {
    await fn();
    await refresh();
  } catch (e) {
    message(e.message);
  }
}
async function showDetail(id) {
  const generation = sessionGeneration;
  try {
    const c = await api(`/api/calls/${id}`),
      container = $("detail-content");
    if (generation !== sessionGeneration || !token) return;
    container.replaceChildren(
      el("p", `${c.name || c.phone} · ${statusNames[c.status] || c.status}`),
    );
    container.append(
      el(
        "p",
        `ID: ${c.id}\nVoximplant: ${c.provider_session_id || "—"}\nElevenLabs: ${c.conversation_id || "—"}`,
        "meta",
      ),
    );
    if (c.reason) container.append(el("p", "Причина: " + c.reason, "hint"));
    if (c.provider_final_status)
      container.append(
        el(
          "p",
          "Финальное событие провайдера: " +
            (statusNames[c.provider_final_status] || c.provider_final_status),
          "hint",
        ),
      );
    if (c.not_before)
      container.append(
        el(
          "p",
          "Не раньше: " + new Date(c.not_before).toLocaleString(),
          "hint",
        ),
      );
    if (c.error_code)
      container.append(el("p", "Код: " + c.error_code, "notice warning"));
    if (c.transcript_truncated)
      container.append(
        el(
          "p",
          "Транскрипт сокращён из-за ограничения размера.",
          "notice warning",
        ),
      );
    const transcript = Array.isArray(c.transcript) ? c.transcript : [];
    if (!transcript.length)
      container.append(el("p", "Транскрипт пока не получен.", "hint"));
    for (const t of transcript)
      container.append(
        el(
          "div",
          (t.role === "user" ? "Собеседник: " : "ИИ: ") + t.text,
          "transcript-line " + t.role,
        ),
      );
    if (c.status === "unknown") {
      const form = el("form"),
        label = el("label", "Проверено в журнале Voximplant: итог"),
        select = el("select");
      for (const s of [
        "failed",
        "cancelled",
        "completed",
        "busy",
        "no_answer",
      ]) {
        const opt = el("option", statusNames[s]);
        opt.value = s;
        select.append(opt);
      }
      label.append(select);
      const note = el("input");
      note.placeholder = "Что проверили и какой результат (от 10 символов)";
      note.minLength = 10;
      note.required = true;
      const button = el("button", "Сохранить сверенный итог");
      form.append(label, note, button);
      form.onsubmit = async (e) => {
        e.preventDefault();
        try {
          await post(`/api/calls/${id}/resolve`, {
            status: select.value,
            note: note.value,
          });
          $("detail").close();
          await refresh();
        } catch (err) {
          message(err.message);
        }
      };
      container.append(form);
    }
    $("detail").showModal();
  } catch (e) {
    message(e.message);
  }
}
$("login-form").onsubmit = async (e) => {
  e.preventDefault();
  token = $("token").value.trim();
  try {
    const initial = await api("/api/config");
    const zone = document.querySelector('[name="timezone"]');
    zone.value = initial.settings.default_timezone;
    zone.defaultValue = initial.settings.default_timezone;
    $("token").value = "";
    $("login").hidden = true;
    $("workspace").hidden = false;
    await refresh();
    pollTimer = setInterval(refresh, 4000);
  } catch (err) {
    token = "";
    message(err.message);
  }
};
$("logout").onclick = logout;
$("refresh").onclick = refresh;
$("resume").onclick = () => act(() => post("/api/queue", { paused: false }));
$("pause").onclick = () => act(() => post("/api/queue", { paused: true }));
$("close-detail").onclick = () => $("detail").close();
$("contact-form").onsubmit = async (e) => {
  e.preventDefault();
  const form = new FormData(e.target);
  await act(async () => {
    await post("/api/contacts", {
      phone: form.get("phone"),
      name: form.get("name"),
      timezone: form.get("timezone"),
      consent_source: form.get("consent_source"),
      consent_at: new Date(form.get("consent_at")).toISOString(),
    });
    e.target.reset();
    message("Контакт сохранён. Добавьте его в очередь, когда будете готовы.");
  });
};
$("suppress-form").onsubmit = async (e) => {
  e.preventDefault();
  const form = new FormData(e.target);
  await act(async () => {
    await post("/api/suppressions", {
      phone: form.get("phone"),
      reason: form.get("reason"),
    });
    e.target.reset();
    message("Номер добавлен в список «не звонить»");
  });
};
$("csv-file").onchange = async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  try {
    if (file.size > 262144) throw new Error("Максимум 256 КБ");
    const result = await api("/api/contacts/import", {
      method: "POST",
      headers: { "Content-Type": "text/csv" },
      body: await file.text(),
    });
    message(
      `Импортировано: ${result.imported}. Ошибок: ${result.errors.length}`,
    );
    await refresh();
  } catch (err) {
    message(err.message);
  } finally {
    e.target.value = "";
  }
};
