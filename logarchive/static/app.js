"use strict";

const state = {
  page: 0,
  pageSize: 20,
  total: 0,
  busy: false,
};

const $ = (id) => document.getElementById(id);

function fmtBytes(n) {
  if (n === null || n === undefined) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = Number(n);
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
}

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const pad = (x) => String(x).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function toast(message, kind = "") {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = message;
  $("toasts").appendChild(el);
  setTimeout(() => el.remove(), kind === "err" ? 7000 : 3800);
}

async function api(path, options = {}) {
  const response = await fetch(path, options);
  const contentType = response.headers.get("Content-Type") || "";
  let payload = null;
  if (contentType.includes("application/json")) {
    payload = await response.json();
  }
  if (!response.ok) {
    const err = (payload && payload.error) || {};
    const detail = err.details && err.details.segment_count
      ? `（依赖 ${err.details.segment_count} 个段）`
      : "";
    const error = new Error((err.message || response.statusText) + detail);
    error.code = err.code;
    throw error;
  }
  return payload;
}

function uploadWithProgress(url, formData, onProgress, headers = {}) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url);
    for (const [name, value] of Object.entries(headers)) {
      xhr.setRequestHeader(name, value);
    }
    xhr.responseType = "json";
    xhr.upload.addEventListener("progress", (event) => {
      if (event.lengthComputable && onProgress) {
        onProgress(event.loaded / event.total);
      }
    });
    xhr.addEventListener("load", () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(xhr.response);
      } else {
        const err = (xhr.response && xhr.response.error) || {};
        const detail = err.details && err.details.segment_count
          ? `（依赖 ${err.details.segment_count} 个段）`
          : "";
        reject(new Error((err.message || `HTTP ${xhr.status}`) + detail));
      }
    });
    xhr.addEventListener("error", () => reject(new Error("网络错误，上传未完成")));
    xhr.addEventListener("abort", () => reject(new Error("上传已取消")));
    xhr.send(formData);
  });
}

/* ------------------------------------------------------------------ status */

function renderStatus(status) {
  const active = status.active_dictionary;
  $("m-active").textContent = active ? active.name : "未启用";
  $("m-active-note").textContent = active
    ? `SHA ${active.sha256.slice(0, 12)}… · ${fmtBytes(active.size)}`
    : "所有段以无字典模式压缩";
  $("m-segments").textContent = String(status.segments);
  $("m-bytes").textContent = `原始 ${fmtBytes(status.original_bytes)} · 存储 ${fmtBytes(status.stored_bytes)}`;
  $("m-backlog").textContent = String(status.backlog);
  $("m-states").textContent = `待处理 ${status.states.pending} · 失败 ${status.states.failed}`;

  const worker = status.worker;
  $("m-worker").textContent = worker.enabled ? (worker.state === "converting" ? `#${worker.current_segment}` : "空闲") : "已停用";
  $("m-current").textContent = worker.enabled ? "后台分批重压" : "WORKER_ENABLED=0";

  const dot = $("worker-dot");
  const text = $("worker-text");
  dot.className = "dot";
  if (!worker.enabled) {
    dot.classList.add("ok");
    text.textContent = "后台换版已停用";
  } else if (status.states.failed > 0) {
    dot.classList.add("bad");
    text.textContent = `${status.states.failed} 个段转换失败`;
  } else if (worker.state === "converting" || status.states.pending > 0) {
    dot.classList.add("busy");
    text.textContent = worker.state === "converting"
      ? `正在换版 #${worker.current_segment} · 待处理 ${status.states.pending}`
      : `待换版 ${status.states.pending}`;
  } else {
    dot.classList.add("ok");
    text.textContent = "后台换版空闲";
  }

  const banner = $("error-banner");
  if (status.last_recompression_error) {
    banner.classList.remove("hidden");
    $("error-text").textContent =
      `段 #${status.last_recompression_error.segment}：${status.last_recompression_error.error}（原段保留，可下载）`;
  } else {
    banner.classList.add("hidden");
  }
}

/* ------------------------------------------------------------ dictionaries */

function renderDictionaries(items) {
  const tbody = $("dict-table").querySelector("tbody");
  tbody.replaceChildren();
  if (!items.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 6;
    td.className = "hint";
    td.textContent = "还没有字典。上传一个后启用，新段就会开始使用它。";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  for (const item of items) {
    const tr = document.createElement("tr");

    const nameCell = document.createElement("td");
    nameCell.textContent = item.name;
    nameCell.className = "nowrap";

    const sizeCell = document.createElement("td");
    sizeCell.textContent = fmtBytes(item.size);

    const shaCell = document.createElement("td");
    const sha = document.createElement("span");
    sha.className = "mono sha";
    sha.textContent = item.sha256;
    sha.title = item.sha256;
    shaCell.appendChild(sha);

    const refCell = document.createElement("td");
    refCell.textContent = String(item.segment_count ?? 0);

    const statusCell = document.createElement("td");
    const badge = document.createElement("span");
    badge.className = `badge ${item.active ? "active" : ""}`;
    badge.textContent = item.active ? "生效中" : "已归档版本";
    statusCell.appendChild(badge);

    const actions = document.createElement("td");
    actions.className = "nowrap";
    if (!item.active) {
      const activate = document.createElement("button");
      activate.className = "small";
      activate.textContent = "启用";
      activate.addEventListener("click", () => activateDictionary(item));
      actions.appendChild(activate);

      const del = document.createElement("button");
      del.className = "small";
      del.textContent = "删除";
      del.style.marginLeft = "6px";
      del.addEventListener("click", () => deleteDictionary(item));
      actions.appendChild(del);
    }

    tr.append(nameCell, sizeCell, shaCell, refCell, statusCell, actions);
    tbody.appendChild(tr);
  }
}

async function loadDictionaries() {
  const data = await api("/api/dictionaries");
  renderDictionaries(data.items || []);
}

async function activateDictionary(item) {
  if (!confirm(`启用字典「${item.name}」？\n\n新入库的段将立即使用它；已有段会在后台分批重压，期间仍可入库和下载。`)) {
    return;
  }
  try {
    await api(`/api/dictionaries/${item.id}/activate`, { method: "POST" });
    toast(`已启用「${item.name}」，后台开始换版`, "ok");
    await refreshAll();
  } catch (error) {
    toast(`启用失败：${error.message}`, "err");
  }
}

async function deleteDictionary(item) {
  if (!confirm(`删除字典「${item.name}」？\n\n仍有段依赖它时会被拒绝。删除后已换版的段不受影响。`)) {
    return;
  }
  try {
    await api(`/api/dictionaries/${item.id}`, { method: "DELETE" });
    toast(`已删除「${item.name}」`, "ok");
    await refreshAll();
  } catch (error) {
    toast(`删除失败：${error.message}`, "err");
  }
}

/* --------------------------------------------------------------- ingestion */

function renderSegments(data) {
  const tbody = $("seg-table").querySelector("tbody");
  tbody.replaceChildren();
  state.total = data.total;
  if (!data.items.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 8;
    td.className = "hint";
    td.textContent = "暂无归档段，先在上方上传日志段。";
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
  for (const seg of data.items) {
    const tr = document.createElement("tr");

    const num = document.createElement("td");
    num.className = "mono nowrap";
    num.textContent = `#${seg.number}`;

    const filename = document.createElement("td");
    filename.textContent = seg.filename;
    filename.title = seg.filename;

    const origSize = document.createElement("td");
    origSize.textContent = fmtBytes(seg.original_size);
    origSize.className = "nowrap";

    const compSize = document.createElement("td");
    compSize.textContent = fmtBytes(seg.compressed_size);
    compSize.className = "nowrap";

    const dict = document.createElement("td");
    dict.textContent = seg.dictionary_name || "（无字典）";
    dict.className = "nowrap";

    const stateCell = document.createElement("td");
    const badge = document.createElement("span");
    const status = seg.recompress_state || "current";
    badge.className = `badge ${status}`;
    const labels = {
      current: "当前版本",
      pending: "待换版",
      running: "转换中",
      failed: "转换失败",
    };
    badge.textContent = labels[status] || status;
    if (seg.recompress_error) badge.title = seg.recompress_error;
    stateCell.appendChild(badge);
    if (status === "failed") {
      const retry = document.createElement("button");
      retry.className = "small link";
      retry.textContent = "重试";
      retry.addEventListener("click", () => retrySegment(seg.number));
      stateCell.appendChild(retry);
    }

    const created = document.createElement("td");
    created.textContent = fmtTime(seg.created_at);
    created.className = "nowrap";

    const actions = document.createElement("td");
    actions.className = "nowrap";
    const link = document.createElement("a");
    link.href = `/api/segments/${seg.number}/download`;
    link.textContent = "下载原文";
    link.className = "download";
    link.setAttribute("download", seg.filename);
    actions.appendChild(link);

    tr.append(num, filename, origSize, compSize, dict, stateCell, created, actions);
    tbody.appendChild(tr);
  }
  const pages = Math.max(1, Math.ceil(state.total / state.pageSize));
  const current = Math.min(state.page + 1, pages);
  $("page-info").textContent = `第 ${current} / ${pages} 页 · 共 ${state.total} 段`;
  $("prev-page").disabled = state.page <= 0;
  $("next-page").disabled = current >= pages;
}

async function loadSegments() {
  const params = new URLSearchParams({
    limit: String(state.pageSize),
    offset: String(state.page * state.pageSize),
  });
  const data = await api(`/api/segments?${params}`);
  renderSegments(data);
}

async function retrySegment(number) {
  try {
    await api("/api/recompression/retry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ segment: number }),
    });
    toast(`段 #${number} 已重新排队`, "ok");
    await refreshAll();
  } catch (error) {
    toast(`重试失败：${error.message}`, "err");
  }
}

async function refreshAll() {
  try {
    const [status] = await Promise.all([api("/api/status"), loadDictionaries(), loadSegments()]);
    renderStatus(status);
  } catch (error) {
    toast(`刷新失败：${error.message}`, "err");
  }
}

async function refreshStatusOnly() {
  try {
    const status = await api("/api/status");
    renderStatus(status);
    return status;
  } catch {
    return null;
  }
}

/* ------------------------------------------------------------------ uploads */

function setupDropZone(zoneId, inputId, labelId, onFiles) {
  const zone = $(zoneId);
  const input = $(inputId);
  const label = $(labelId);

  input.addEventListener("change", () => {
    label.textContent = describeFiles(input.files);
    if (onFiles) onFiles(input.files);
  });
  ["dragenter", "dragover"].forEach((type) =>
    zone.addEventListener(type, (event) => {
      event.preventDefault();
      zone.classList.add("hover");
    })
  );
  ["dragleave", "drop"].forEach((type) =>
    zone.addEventListener(type, (event) => {
      event.preventDefault();
      zone.classList.remove("hover");
    })
  );
  zone.addEventListener("drop", (event) => {
    const files = event.dataTransfer.files;
    if (!files || !files.length) return;
    input.files = files;
    label.textContent = describeFiles(files);
    if (onFiles) onFiles(files);
  });
}

function describeFiles(list) {
  if (!list || !list.length) return "点击选择或拖入文件";
  if (list.length === 1) return `${list[0].name}（${fmtBytes(list[0].size)}）`;
  let total = 0;
  for (const file of list) total += file.size;
  return `${list.length} 个文件，合计 ${fmtBytes(total)}`;
}

function logLine(text, kind = "") {
  const line = document.createElement("div");
  if (kind) line.className = kind;
  line.textContent = `${new Date().toLocaleTimeString()}  ${text}`;
  $("upload-log").prepend(line);
}

const MAX_HASH_BYTES = 64 * 1024 * 1024;

async function sha256(file) {
  // Hashing needs the whole file in memory; skip it for large segments rather
  // than risk the tab running out of memory.
  if (!window.crypto || !window.crypto.subtle) return null;
  if (file.size > MAX_HASH_BYTES) return null;
  try {
    const buffer = await file.arrayBuffer();
    const digest = await window.crypto.subtle.digest("SHA-256", buffer);
    return Array.from(new Uint8Array(digest))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  } catch {
    return null;
  }
}

function setProgress(fraction, text) {
  $("seg-progress").classList.remove("hidden");
  $("seg-bar").style.width = `${Math.round(fraction * 100)}%`;
  $("seg-progress-text").textContent = text;
}

function resetSegForm() {
  $("seg-form").reset();
  $("seg-file-label").textContent = "点击选择或拖入文件（可多选）";
  $("seg-progress").classList.add("hidden");
  $("seg-bar").style.width = "0";
}

async function uploadSegments(files) {
  const list = Array.from(files);
  if (!list.length) return;
  const submit = $("seg-submit");
  submit.disabled = true;
  const results = [];
  for (let i = 0; i < list.length; i += 1) {
    const file = list[i];
    const form = new FormData();
    form.append("file", file, file.name);
    const prefix = list.length > 1 ? `[${i + 1}/${list.length}] ` : "";
    setProgress(0, `${prefix}${file.name} 上传中…`);
    try {
      const digest = await sha256(file);
      const headers = digest ? { "X-Content-SHA256": digest } : {};
      const data = await uploadWithProgress("/api/segments", form, (fraction) => {
        setProgress(fraction, `${prefix}${file.name} 上传中… ${Math.round(fraction * 100)}%`);
      }, headers);
      results.push(data);
      logLine(`${prefix}${file.name} → 段 #${data.number}（字典 ${data.dictionary_name || "无"}）`, "ok");
    } catch (error) {
      logLine(`${prefix}${file.name} 失败：${error.message}`, "err");
    }
  }
  setProgress(1, `完成 ${results.length}/${list.length}`);
  if (results.length) toast(`已入库 ${results.length} 个段`, "ok");
  if (results.length < list.length) toast(`${list.length - results.length} 个文件上传失败，见日志`, "err");
  await refreshAll();
  submit.disabled = false;
  setTimeout(resetSegForm, 2500);
}

/* ------------------------------------------------------------------- events */

function bind() {
  setupDropZone("dict-drop", "dict-file", "dict-file-label");
  setupDropZone("seg-drop", "seg-file", "seg-file-label");

  $("dict-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const file = $("dict-file").files[0];
    if (!file) {
      toast("请先选择字典文件", "err");
      return;
    }
    if (file.size > 32768) {
      toast("字典文件不能超过 32 KiB", "err");
      return;
    }
    const form = new FormData();
    form.append("dictionary", file, file.name);
    const nameInput = $("dict-form").querySelector('input[name="name"]');
    const name = nameInput ? nameInput.value.trim() : "";
    if (name) form.append("name", name);
    if ($("dict-activate").checked) form.append("activate", "1");
    const submit = $("dict-submit");
    submit.disabled = true;
    try {
      const data = await uploadWithProgress("/api/dictionaries", form);
      toast(
        $("dict-activate").checked ? `字典「${data.name}」已启用` : `字典「${data.name}」已上传`,
        "ok"
      );
      $("dict-form").reset();
      $("dict-file-label").textContent = "点击选择或拖入文件（≤ 32 KiB）";
      await refreshAll();
    } catch (error) {
      toast(`上传字典失败：${error.message}`, "err");
    } finally {
      submit.disabled = false;
    }
  });

  $("seg-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const files = $("seg-file").files;
    if (!files || !files.length) {
      toast("请先选择日志段文件", "err");
      return;
    }
    await uploadSegments(files);
  });

  $("refresh-btn").addEventListener("click", refreshAll);
  $("retry-failed").addEventListener("click", async () => {
    try {
      const result = await api("/api/recompression/retry", { method: "POST" });
      toast(`已重新排队 ${result.requeued} 个段`, "ok");
      await refreshAll();
    } catch (error) {
      toast(`重试失败：${error.message}`, "err");
    }
  });

  $("prev-page").addEventListener("click", () => {
    if (state.page > 0) {
      state.page -= 1;
      loadSegments().catch((error) => toast(error.message, "err"));
    }
  });
  $("next-page").addEventListener("click", () => {
    if ((state.page + 1) * state.pageSize < state.total) {
      state.page += 1;
      loadSegments().catch((error) => toast(error.message, "err"));
    }
  });

  let lastBacklog = null;
  setInterval(async () => {
    if (!$("auto-refresh").checked || document.hidden) return;
    const status = await refreshStatusOnly();
    if (!status) return;
    const backlog = status.states.pending + status.states.running;
    const changed = backlog !== lastBacklog;
    lastBacklog = backlog;
    if (changed || status.states.failed > 0) {
      loadSegments().catch(() => {});
      loadDictionaries().catch(() => {});
    }
  }, 2500);
}

bind();
refreshAll();
