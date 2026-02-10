// ── State ────────────────────────────────────────────────────────────────────

const state = {
    currentView: "dashboard",
    changelog: { page: 1, perPage: 50 },
    charts: { active: null, changes: null },
};

// ── API Helpers ──────────────────────────────────────────────────────────────

async function api(path, params = {}) {
    const url = new URL(`/api${path}`, location.origin);
    for (const [k, v] of Object.entries(params)) {
        if (v != null && v !== "") url.searchParams.set(k, v);
    }
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`API error: ${resp.status}`);
    return resp.json();
}

function fmt(n) {
    return n != null ? Number(n).toLocaleString() : "—";
}

// ── Navigation ──────────────────────────────────────────────────────────────

document.querySelectorAll(".nav-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
        const view = btn.dataset.view;
        switchView(view);
    });
});

function switchView(view) {
    state.currentView = view;
    document.querySelectorAll(".nav-btn").forEach((b) => b.classList.toggle("active", b.dataset.view === view));
    document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === `view-${view}`));

    if (view === "dashboard") loadDashboard();
    else if (view === "changelog") loadChangelog();
    else if (view === "timeline") loadTimeline();
}

// ── Dashboard ───────────────────────────────────────────────────────────────

async function loadDashboard() {
    try {
        const [stats, snapshots] = await Promise.all([
            api("/stats"),
            api("/snapshots"),
        ]);

        document.getElementById("stat-total-codes").textContent = fmt(stats.total_codes);
        document.getElementById("stat-snapshots").textContent = fmt(stats.total_snapshots);
        document.getElementById("stat-added").textContent = fmt(stats.changes_by_type?.added || 0);
        document.getElementById("stat-removed").textContent = fmt(stats.changes_by_type?.removed || 0);

        // Changes by type bar chart
        renderBarChart("changes-by-type", stats.changes_by_type || {}, {
            added: "var(--green)",
            removed: "var(--red)",
            city_changed: "var(--orange)",
            csd_changed: "var(--blue)",
            location_shifted: "var(--accent)",
        });

        // Codes by province
        renderBarChart("codes-by-province", stats.codes_by_province || {}, {}, "var(--accent)");

        // Snapshots table
        const tbody = document.querySelector("#snapshots-table tbody");
        tbody.innerHTML = snapshots
            .map((s) => `<tr><td>${s.source_type}</td><td>${s.snapshot_date}</td><td>${fmt(s.postal_code_count)}</td></tr>`)
            .join("");
    } catch (e) {
        console.error("Dashboard error:", e);
    }
}

function renderBarChart(containerId, data, colorMap = {}, defaultColor = "var(--accent)") {
    const container = document.getElementById(containerId);
    const entries = Object.entries(data);
    if (!entries.length) {
        container.innerHTML = '<div class="loading">No data</div>';
        return;
    }
    const max = Math.max(...entries.map(([, v]) => v));
    container.innerHTML = `<ul class="bar-list">${entries
        .map(([label, value]) => {
            const pct = max > 0 ? (value / max) * 100 : 0;
            const color = colorMap[label] || defaultColor;
            return `<li class="bar-item">
                <span class="bar-label">${label}</span>
                <div class="bar-fill" style="width:${pct}%;background:${color}"></div>
                <span class="bar-value">${fmt(value)}</span>
            </li>`;
        })
        .join("")}</ul>`;
}

// ── Change Log ──────────────────────────────────────────────────────────────

const filterSearch = document.getElementById("filter-search");
const filterType = document.getElementById("filter-type");
const filterProvince = document.getElementById("filter-province");
const filterSource = document.getElementById("filter-source");

[filterType, filterProvince, filterSource].forEach((el) => {
    el.addEventListener("change", () => { state.changelog.page = 1; loadChangelog(); });
});

let searchTimeout;
filterSearch.addEventListener("input", () => {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => { state.changelog.page = 1; loadChangelog(); }, 300);
});

document.getElementById("btn-export").addEventListener("click", exportCSV);

async function loadChangelog() {
    // Load provinces for filter if empty
    if (filterProvince.options.length <= 1) {
        try {
            const provinces = await api("/provinces");
            provinces.forEach((p) => {
                const opt = document.createElement("option");
                opt.value = p.province_abbr;
                opt.textContent = `${p.province_abbr} (${fmt(p.code_count)})`;
                filterProvince.appendChild(opt);
            });
        } catch (e) { /* ok */ }
    }

    try {
        const data = await api("/changes", {
            page: state.changelog.page,
            per_page: state.changelog.perPage,
            change_type: filterType.value,
            province: filterProvince.value,
            source: filterSource.value,
            search: filterSearch.value,
        });

        document.getElementById("changelog-info").textContent =
            `${fmt(data.total)} changes found — page ${data.page} of ${data.pages}`;

        const tbody = document.querySelector("#changelog-table tbody");
        tbody.innerHTML = data.items
            .map((c) => `<tr>
                <td><a class="pc-link" data-code="${c.postal_code}">${formatPC(c.postal_code)}</a></td>
                <td><span class="badge badge-${c.change_type}">${c.change_type}</span></td>
                <td>${c.province_abbr || ""}</td>
                <td>${c.fsa || ""}</td>
                <td>${c.snapshot_before} &rarr; ${c.snapshot_after}</td>
                <td>${c.old_value || ""}</td>
                <td>${c.new_value || ""}</td>
            </tr>`)
            .join("");

        // Postal code links
        tbody.querySelectorAll(".pc-link").forEach((a) => {
            a.addEventListener("click", (e) => {
                e.preventDefault();
                showPostalCodeDetail(a.dataset.code);
            });
        });

        renderPagination(data.page, data.pages);
    } catch (e) {
        console.error("Changelog error:", e);
    }
}

function renderPagination(current, total) {
    const container = document.getElementById("pagination");
    if (total <= 1) { container.innerHTML = ""; return; }

    let html = "";
    html += `<button ${current === 1 ? "disabled" : ""} data-page="${current - 1}">&laquo; Prev</button>`;

    const pages = paginationRange(current, total);
    for (const p of pages) {
        if (p === "...") {
            html += `<button disabled>...</button>`;
        } else {
            html += `<button class="${p === current ? "active" : ""}" data-page="${p}">${p}</button>`;
        }
    }

    html += `<button ${current === total ? "disabled" : ""} data-page="${current + 1}">Next &raquo;</button>`;
    container.innerHTML = html;

    container.querySelectorAll("button[data-page]").forEach((btn) => {
        btn.addEventListener("click", () => {
            state.changelog.page = parseInt(btn.dataset.page);
            loadChangelog();
        });
    });
}

function paginationRange(current, total) {
    if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);
    const pages = [1];
    if (current > 3) pages.push("...");
    for (let i = Math.max(2, current - 1); i <= Math.min(total - 1, current + 1); i++) {
        pages.push(i);
    }
    if (current < total - 2) pages.push("...");
    pages.push(total);
    return pages;
}

function formatPC(code) {
    if (code && code.length === 6) return `${code.slice(0, 3)} ${code.slice(3)}`;
    return code;
}

async function exportCSV() {
    try {
        const data = await api("/changes", {
            per_page: 500,
            change_type: filterType.value,
            province: filterProvince.value,
            source: filterSource.value,
            search: filterSearch.value,
        });

        const header = "postal_code,change_type,province,fsa,snapshot_before,snapshot_after,old_value,new_value";
        const rows = data.items.map((c) =>
            [c.postal_code, c.change_type, c.province_abbr, c.fsa,
             c.snapshot_before, c.snapshot_after, c.old_value || "", c.new_value || ""].join(",")
        );

        const blob = new Blob([header + "\n" + rows.join("\n")], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "postal_code_changes.csv";
        a.click();
        URL.revokeObjectURL(url);
    } catch (e) {
        console.error("Export error:", e);
    }
}

// ── Timeline ────────────────────────────────────────────────────────────────

async function loadTimeline() {
    try {
        const data = await api("/changes/timeline");

        // Active codes over time
        const snapshots = data.snapshots || [];
        if (snapshots.length) {
            const ctx1 = document.getElementById("chart-active").getContext("2d");
            if (state.charts.active) state.charts.active.destroy();
            state.charts.active = new Chart(ctx1, {
                type: "line",
                data: {
                    labels: snapshots.map((s) => s.snapshot_date),
                    datasets: [{
                        label: "Active Postal Codes",
                        data: snapshots.map((s) => s.active_count),
                        borderColor: "#6c8cff",
                        backgroundColor: "rgba(108,140,255,0.1)",
                        fill: true,
                        tension: 0.3,
                    }],
                },
                options: {
                    responsive: true,
                    plugins: { legend: { labels: { color: "#8b8fa3" } } },
                    scales: {
                        x: { ticks: { color: "#8b8fa3" }, grid: { color: "#2e3142" } },
                        y: { ticks: { color: "#8b8fa3" }, grid: { color: "#2e3142" } },
                    },
                },
            });
        }

        // Changes per period
        const changes = data.changes || [];
        if (changes.length) {
            // Group by period
            const periods = [...new Set(changes.map((c) => `${c.snapshot_before} → ${c.snapshot_after}`))];
            const added = periods.map((p) => {
                const match = changes.find((c) => `${c.snapshot_before} → ${c.snapshot_after}` === p && c.change_type === "added");
                return match ? match.count : 0;
            });
            const removed = periods.map((p) => {
                const match = changes.find((c) => `${c.snapshot_before} → ${c.snapshot_after}` === p && c.change_type === "removed");
                return match ? match.count : 0;
            });

            const ctx2 = document.getElementById("chart-changes").getContext("2d");
            if (state.charts.changes) state.charts.changes.destroy();
            state.charts.changes = new Chart(ctx2, {
                type: "bar",
                data: {
                    labels: periods,
                    datasets: [
                        { label: "Added", data: added, backgroundColor: "#4caf50" },
                        { label: "Removed", data: removed, backgroundColor: "#ef5350" },
                    ],
                },
                options: {
                    responsive: true,
                    plugins: { legend: { labels: { color: "#8b8fa3" } } },
                    scales: {
                        x: { ticks: { color: "#8b8fa3" }, grid: { color: "#2e3142" } },
                        y: { ticks: { color: "#8b8fa3" }, grid: { color: "#2e3142" } },
                    },
                },
            });
        }
    } catch (e) {
        console.error("Timeline error:", e);
    }
}

// ── Lookup ───────────────────────────────────────────────────────────────────

document.getElementById("lookup-btn").addEventListener("click", doLookup);
document.getElementById("lookup-input").addEventListener("keypress", (e) => {
    if (e.key === "Enter") doLookup();
});

async function doLookup() {
    const input = document.getElementById("lookup-input").value.trim().toUpperCase().replace(/\s/g, "");
    const container = document.getElementById("lookup-result");

    if (!input) return;

    if (input.length <= 3) {
        // FSA lookup
        try {
            const data = await api(`/fsa/${input}`);
            container.innerHTML = renderFSADetail(data);
            attachPCLinks(container);
        } catch (e) {
            container.innerHTML = '<div class="loading">FSA not found</div>';
        }
    } else {
        // Full postal code
        showPostalCodeDetail(input);
    }
}

// ── Postal Code Detail Modal ────────────────────────────────────────────────

async function showPostalCodeDetail(code) {
    const overlay = document.getElementById("modal-overlay");
    const content = document.getElementById("modal-content");
    overlay.classList.remove("hidden");

    content.innerHTML = '<div class="loading">Loading...</div>';

    try {
        const data = await api(`/postal-code/${code}`);
        content.innerHTML = renderPCDetail(data);
    } catch (e) {
        content.innerHTML = `<div class="loading">Error loading ${formatPC(code)}</div>`;
    }
}

document.querySelector(".modal-close").addEventListener("click", closeModal);
document.getElementById("modal-overlay").addEventListener("click", (e) => {
    if (e.target === e.currentTarget) closeModal();
});

function closeModal() {
    document.getElementById("modal-overlay").classList.add("hidden");
}

function renderPCDetail(data) {
    const pc = data.postal_code;
    const s = data.summary;
    let html = `<h2>${formatPC(pc)}</h2>`;

    if (s) {
        html += `<div class="stats-grid" style="margin-bottom:16px">
            <div class="stat-card">
                <div class="stat-value" style="font-size:18px">${s.province_abbr || "—"}</div>
                <div class="stat-label">Province</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="font-size:18px">${s.city_name || "—"}</div>
                <div class="stat-label">City</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="font-size:18px">${s.is_active ? "Active" : "Inactive"}</div>
                <div class="stat-label">Status</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="font-size:18px">${s.total_changes}</div>
                <div class="stat-label">Changes</div>
            </div>
        </div>
        <p style="color:var(--text-muted);font-size:13px;margin-bottom:16px">
            First seen: ${s.first_seen} | Last seen: ${s.last_seen} | FSA: ${s.fsa}
            ${s.is_rural ? " | Rural" : ""} | Sources: ${s.sources || "—"}
        </p>`;
    }

    if (data.snapshots.length) {
        html += `<h3>Snapshot History</h3>
        <table><thead><tr><th>Date</th><th>Source</th><th>City</th><th>Province</th><th>Addresses</th></tr></thead><tbody>`;
        for (const snap of data.snapshots) {
            html += `<tr>
                <td>${snap.snapshot_date}</td><td>${snap.source_type}</td>
                <td>${snap.city_name || ""}</td><td>${snap.province_abbr || ""}</td>
                <td>${fmt(snap.address_count)}</td>
            </tr>`;
        }
        html += `</tbody></table>`;
    }

    if (data.changes.length) {
        html += `<h3>Changes</h3>
        <table><thead><tr><th>Type</th><th>Period</th><th>Old</th><th>New</th></tr></thead><tbody>`;
        for (const c of data.changes) {
            html += `<tr>
                <td><span class="badge badge-${c.change_type}">${c.change_type}</span></td>
                <td>${c.snapshot_before} &rarr; ${c.snapshot_after}</td>
                <td>${c.old_value || ""}</td><td>${c.new_value || ""}</td>
            </tr>`;
        }
        html += `</tbody></table>`;
    }

    return html;
}

function renderFSADetail(data) {
    let html = `<div class="card"><h2>FSA: ${data.fsa}</h2>
        <p style="color:var(--text-muted);margin-bottom:16px">
            ${data.total_codes} postal codes | ${data.total_changes} total changes
            ${data.snapshot_date ? ` | Snapshot: ${data.snapshot_date}` : ""}
        </p>`;

    if (data.postal_codes.length) {
        html += `<h3>Postal Codes</h3>
        <table><thead><tr><th>Code</th><th>City</th><th>Province</th><th>Addresses</th></tr></thead><tbody>`;
        for (const pc of data.postal_codes.slice(0, 100)) {
            html += `<tr>
                <td><a class="pc-link" data-code="${pc.postal_code}">${formatPC(pc.postal_code)}</a></td>
                <td>${pc.city_name || ""}</td><td>${pc.province_abbr || ""}</td>
                <td>${fmt(pc.address_count)}</td>
            </tr>`;
        }
        if (data.postal_codes.length > 100) {
            html += `<tr><td colspan="4" style="color:var(--text-muted)">... and ${data.postal_codes.length - 100} more</td></tr>`;
        }
        html += `</tbody></table>`;
    }

    if (data.changes.length) {
        html += `<h3>Changes</h3>
        <table><thead><tr><th>Code</th><th>Type</th><th>Period</th><th>Old</th><th>New</th></tr></thead><tbody>`;
        for (const c of data.changes.slice(0, 100)) {
            html += `<tr>
                <td><a class="pc-link" data-code="${c.postal_code}">${formatPC(c.postal_code)}</a></td>
                <td><span class="badge badge-${c.change_type}">${c.change_type}</span></td>
                <td>${c.snapshot_before} &rarr; ${c.snapshot_after}</td>
                <td>${c.old_value || ""}</td><td>${c.new_value || ""}</td>
            </tr>`;
        }
        html += `</tbody></table>`;
    }

    html += `</div>`;
    return html;
}

function attachPCLinks(container) {
    container.querySelectorAll(".pc-link").forEach((a) => {
        a.addEventListener("click", (e) => {
            e.preventDefault();
            showPostalCodeDetail(a.dataset.code);
        });
    });
}

// ── Init ─────────────────────────────────────────────────────────────────────

loadDashboard();
