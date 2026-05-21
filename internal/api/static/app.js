const state = {
  runs: [],
  selectedRunId: null,
  selectedRun: null,
  selectedHistory: [],
  selectedReplay: null,
  lastError: null,
};

const els = {
  connectionPill: document.getElementById("connection-pill"),
  refreshButton: document.getElementById("refresh-button"),
  runCount: document.getElementById("run-count"),
  runsList: document.getElementById("runs-list"),
  runTitle: document.getElementById("run-title"),
  runSubtitle: document.getElementById("run-subtitle"),
  runActions: document.getElementById("run-actions"),
  summaryGrid: document.getElementById("summary-grid"),
  replayPill: document.getElementById("replay-pill"),
  replayPanel: document.getElementById("replay-panel"),
  tasksTable: document.getElementById("tasks-table"),
  historyPanel: document.getElementById("history-panel"),
  graphPanel: document.getElementById("graph-panel"),
  graphCaption: document.getElementById("graph-caption"),
};

function statusClass(status) {
  switch ((status || "").toLowerCase()) {
    case "running":
    case "ok":
      return "pill-running";
    case "completed":
    case "true":
      return "pill-completed";
    case "failed":
    case "cancelled":
      return "pill-failed";
    case "compensating":
    case "cancelling":
    case "leased":
      return "pill-compensating";
    case "available":
    case "timer":
      return "pill-available";
    default:
      return "pill-muted";
  }
}

function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function formatRelative(raw) {
  if (!raw) return "unknown";
  const date = new Date(raw);
  const diffMs = Date.now() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHours = Math.floor(diffMin / 60);
  if (diffHours < 24) return `${diffHours}h ago`;
  return `${Math.floor(diffHours / 24)}d ago`;
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `request failed with ${response.status}`);
  }
  return response.json();
}

async function loadRuns() {
  const data = await request("/v1/runs?limit=40");
  state.runs = data.runs || [];
  if (!state.selectedRunId && state.runs.length > 0) {
    state.selectedRunId = state.runs[0].id;
  }
  if (state.selectedRunId && !state.runs.find((run) => run.id === state.selectedRunId)) {
    state.selectedRunId = state.runs[0]?.id || null;
  }
}

async function loadSelectedRun() {
  if (!state.selectedRunId) {
    state.selectedRun = null;
    state.selectedHistory = [];
    state.selectedReplay = null;
    return;
  }
  const [runView, history, replay] = await Promise.all([
    request(`/v1/runs/${state.selectedRunId}`),
    request(`/v1/runs/${state.selectedRunId}/history`),
    request(`/v1/runs/${state.selectedRunId}/replay`, { method: "POST", body: "{}" }),
  ]);
  state.selectedRun = runView;
  state.selectedHistory = history.events || [];
  state.selectedReplay = replay;
}

function setConnected(connected, message = "") {
  els.connectionPill.className = `pill ${connected ? "pill-completed" : "pill-failed"}`;
  els.connectionPill.textContent = connected ? "api healthy" : message || "disconnected";
}

function renderRuns() {
  els.runCount.textContent = String(state.runs.length);
  if (state.runs.length === 0) {
    els.runsList.innerHTML = '<div class="empty-state">No runs yet. Start one with <span class="mono">orchctl start</span>.</div>';
    return;
  }
  els.runsList.innerHTML = state.runs.map((run) => `
    <article class="run-card ${run.id === state.selectedRunId ? "active" : ""}" data-run-id="${escapeHTML(run.id)}">
      <div class="run-card-title">
        <span>${escapeHTML(run.workflow_name)}</span>
        <span class="pill ${statusClass(run.status)}">${escapeHTML(run.status)}</span>
      </div>
      <div class="run-card-subtitle">
        <span class="mono">${escapeHTML(run.id.slice(0, 8))}</span>
        <span>${escapeHTML(formatRelative(run.updated_at))}</span>
      </div>
    </article>
  `).join("");
  els.runsList.querySelectorAll("[data-run-id]").forEach((node) => {
    node.addEventListener("click", async () => {
      state.selectedRunId = node.getAttribute("data-run-id");
      await refresh();
    });
  });
}

function renderSummary() {
  if (!state.selectedRun) {
    els.runTitle.textContent = "Select a run";
    els.runSubtitle.textContent = "The inspector polls the API and replay view automatically.";
    els.runActions.innerHTML = "";
    els.summaryGrid.className = "summary-grid empty-state";
    els.summaryGrid.textContent = "No run selected.";
    return;
  }

  const run = state.selectedRun.run;
  els.runTitle.textContent = run.workflow_name;
  els.runSubtitle.textContent = `${run.id} · created ${formatRelative(run.created_at)} · updated ${formatRelative(run.updated_at)}`;
  els.runActions.innerHTML = `
    <span class="pill ${statusClass(run.status)}">${escapeHTML(run.status)}</span>
  `;

  const tasks = state.selectedRun.tasks || [];
  const retryCount = tasks.filter((task) => task.attempt > 1).length;
  const compensationCount = tasks.filter((task) => task.phase === "compensation").length;
  els.summaryGrid.className = "summary-grid";
  els.summaryGrid.innerHTML = `
    ${summaryStat("Tasks", tasks.length)}
    ${summaryStat("Retries", retryCount)}
    ${summaryStat("Compensations", compensationCount)}
    ${summaryStat("Last Update", formatRelative(run.updated_at))}
  `;
}

function summaryStat(label, value) {
  return `
    <div class="summary-stat">
      <span class="summary-label">${escapeHTML(label)}</span>
      <span class="summary-value">${escapeHTML(value)}</span>
    </div>
  `;
}

function renderTasks() {
  if (!state.selectedRun?.tasks?.length) {
    els.tasksTable.className = "table-shell empty-state";
    els.tasksTable.textContent = "No tasks yet.";
    return;
  }
  els.tasksTable.className = "table-shell";
  els.tasksTable.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Step</th>
          <th>Status</th>
          <th>Phase</th>
          <th>Kind</th>
          <th>Attempt</th>
          <th>Queue</th>
          <th>Last Error</th>
        </tr>
      </thead>
      <tbody>
        ${state.selectedRun.tasks.map((task) => `
          <tr>
            <td><span class="mono">${escapeHTML(task.step_id)}</span></td>
            <td><span class="pill ${statusClass(task.status)}">${escapeHTML(task.status)}</span></td>
            <td>${escapeHTML(task.phase)}</td>
            <td>${escapeHTML(task.kind)}</td>
            <td>${escapeHTML(task.attempt)}</td>
            <td>${escapeHTML(task.queue_name)}</td>
            <td>${escapeHTML(task.last_error || "—")}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderHistory() {
  if (!state.selectedHistory.length) {
    els.historyPanel.className = "history-panel empty-state";
    els.historyPanel.textContent = "No history yet.";
    return;
  }
  els.historyPanel.className = "history-panel";
  els.historyPanel.innerHTML = state.selectedHistory.map((event) => `
    <div class="history-row">
      <div class="history-seq">${escapeHTML(event.sequence)}</div>
      <div class="history-body">
        <div class="run-meta-row">
          <strong>${escapeHTML(event.type)}</strong>
          <span class="meta">${escapeHTML(new Date(event.created_at).toLocaleTimeString())}</span>
        </div>
        <div class="tag-list">
          ${event.step_id ? `<span class="tag">step: ${escapeHTML(event.step_id)}</span>` : ""}
          ${event.task_id ? `<span class="tag">task: ${escapeHTML(event.task_id.slice(0, 8))}</span>` : ""}
        </div>
        <div class="history-payload mono">${escapeHTML(JSON.stringify(event.payload, null, 2))}</div>
      </div>
    </div>
  `).join("");
}

function renderReplay() {
  if (!state.selectedReplay) {
    els.replayPill.className = "pill pill-muted";
    els.replayPill.textContent = "unknown";
    els.replayPanel.className = "replay-panel empty-state";
    els.replayPanel.textContent = "No replay data loaded.";
    return;
  }
  const replay = state.selectedReplay;
  els.replayPill.className = `pill ${replay.summary_matches ? "pill-completed" : "pill-failed"}`;
  els.replayPill.textContent = replay.summary_matches ? "summary matches" : "summary mismatch";
  els.replayPanel.className = "replay-panel";
  els.replayPanel.innerHTML = `
    <div class="replay-grid">
      <div class="replay-card">
        <div class="summary-label">History Length</div>
        <div class="summary-value">${escapeHTML(replay.history_length)}</div>
      </div>
      <div class="replay-card">
        <div class="summary-label">Completed Order</div>
        <div class="tag-list">
          ${((replay.state?.completed_order) || []).map((step) => `<span class="tag">${escapeHTML(step)}</span>`).join("") || '<span class="muted">none</span>'}
        </div>
      </div>
    </div>
    <div class="replay-card">
      <div class="summary-label">Run State</div>
      <div class="tag-list">
        <span class="tag">cancel_requested: ${escapeHTML(Boolean(replay.state?.cancel_requested))}</span>
        <span class="tag">compensating: ${escapeHTML(Boolean(replay.state?.compensating))}</span>
        <span class="tag">workflow_finished: ${escapeHTML(Boolean(replay.state?.workflow_finished))}</span>
      </div>
    </div>
  `;
}

function stepVisualStatus(step) {
  if (step.compensation_failed) return "failed";
  if (step.compensation_completed) return "completed";
  if (step.compensation_scheduled) return "compensating";
  if (step.forward_status) return String(step.forward_status);
  if (!step.dependencies_satisfied) return "waiting";
  return "pending";
}

function renderGraph() {
  const steps = state.selectedReplay?.state?.steps;
  if (!steps || Object.keys(steps).length === 0) {
    els.graphPanel.className = "graph-panel empty-state";
    els.graphPanel.textContent = "Run replay will render here.";
    return;
  }

  const nodes = Object.entries(steps).map(([id, step]) => ({
    id,
    label: id,
    status: stepVisualStatus(step),
    deps: step.definition?.dependencies || [],
    kind: step.definition?.kind || "activity",
  }));
  const levels = computeLevels(nodes);
  const grouped = new Map();
  for (const node of nodes) {
    const level = levels.get(node.id) || 0;
    if (!grouped.has(level)) grouped.set(level, []);
    grouped.get(level).push(node);
  }

  const columnGap = 220;
  const rowGap = 110;
  const nodeWidth = 170;
  const nodeHeight = 64;
  const levelKeys = [...grouped.keys()].sort((a, b) => a - b);
  const width = Math.max(720, levelKeys.length * columnGap + 180);
  const height = Math.max(260, ...levelKeys.map((level) => grouped.get(level).length * rowGap + 120));

  const positions = new Map();
  levelKeys.forEach((level, colIndex) => {
    const items = grouped.get(level);
    items.sort((left, right) => left.label.localeCompare(right.label));
    const startY = Math.max(50, (height - items.length * rowGap) / 2);
    items.forEach((node, rowIndex) => {
      positions.set(node.id, {
        x: 50 + colIndex * columnGap,
        y: startY + rowIndex * rowGap,
      });
    });
  });

  const edges = nodes.flatMap((node) => node.deps.map((dep) => [dep, node.id]));
  const svg = [];
  svg.push(`<svg class="graph-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Workflow dependency graph">`);
  svg.push(`<defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L10,3 L0,6 z" fill="#64748b"></path></marker></defs>`);

  for (const [from, to] of edges) {
    const start = positions.get(from);
    const end = positions.get(to);
    if (!start || !end) continue;
    const x1 = start.x + nodeWidth;
    const y1 = start.y + nodeHeight / 2;
    const x2 = end.x;
    const y2 = end.y + nodeHeight / 2;
    const mid = (x1 + x2) / 2;
    svg.push(`<path d="M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}" stroke="#64748b" stroke-width="2" fill="none" marker-end="url(#arrow)"></path>`);
  }

  for (const node of nodes) {
    const pos = positions.get(node.id);
    const colors = graphNodeColors(node.status);
    svg.push(`<rect x="${pos.x}" y="${pos.y}" width="${nodeWidth}" height="${nodeHeight}" rx="16" fill="${colors.fill}" stroke="${colors.stroke}" stroke-width="2"></rect>`);
    svg.push(`<text class="node-label" x="${pos.x + 14}" y="${pos.y + 26}">${escapeHTML(node.label)}</text>`);
    svg.push(`<text class="node-sub" x="${pos.x + 14}" y="${pos.y + 46}">${escapeHTML(`${node.kind} · ${node.status}`)}</text>`);
  }
  svg.push(`</svg>`);

  els.graphPanel.className = "graph-panel";
  els.graphCaption.textContent = `${nodes.length} steps · replay-backed`;
  els.graphPanel.innerHTML = svg.join("");
}

function graphNodeColors(status) {
  switch (status) {
    case "completed":
      return { fill: "rgba(34, 197, 94, 0.16)", stroke: "#22c55e" };
    case "failed":
    case "cancelled":
      return { fill: "rgba(239, 68, 68, 0.16)", stroke: "#ef4444" };
    case "leased":
    case "compensating":
      return { fill: "rgba(167, 139, 250, 0.16)", stroke: "#a78bfa" };
    case "available":
    case "pending":
    case "waiting":
      return { fill: "rgba(245, 158, 11, 0.16)", stroke: "#f59e0b" };
    default:
      return { fill: "rgba(96, 165, 250, 0.16)", stroke: "#60a5fa" };
  }
}

function computeLevels(nodes) {
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const memo = new Map();

  function visit(id) {
    if (memo.has(id)) return memo.get(id);
    const node = byId.get(id);
    if (!node || !node.deps.length) {
      memo.set(id, 0);
      return 0;
    }
    const level = Math.max(...node.deps.map((dep) => visit(dep))) + 1;
    memo.set(id, level);
    return level;
  }

  for (const node of nodes) {
    visit(node.id);
  }
  return memo;
}

function render() {
  renderRuns();
  renderSummary();
  renderTasks();
  renderHistory();
  renderReplay();
  renderGraph();
}

async function refresh() {
  try {
    await loadRuns();
    await loadSelectedRun();
    setConnected(true);
    state.lastError = null;
  } catch (error) {
    state.lastError = error;
    setConnected(false, "api error");
    console.error(error);
  }
  render();
}

els.refreshButton.addEventListener("click", refresh);

refresh();
window.setInterval(refresh, 2500);
