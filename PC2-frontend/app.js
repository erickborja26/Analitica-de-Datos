function detectDefaultBaseUrl() {
  const stored = localStorage.getItem('pc2-base-url');
  if (stored) return stored;
  try {
    const { protocol, hostname, port } = window.location;
    if (protocol.startsWith('http')) {
      if (!port || port === '80' || port === '443') {
        return `${protocol}//${hostname || 'localhost'}:5000`;
      }
      if (port === '5000') {
        return `${protocol}//${hostname}:5000`;
      }
      return `${protocol}//${hostname}:5000`;
    }
  } catch (error) {
    console.warn('No se pudo detectar origen actual, usando localhost:5000', error);
  }
  return 'http://localhost:5000';
}

function normalizeBaseUrl(value) {
  if (!value) return '';
  const trimmed = String(value).trim();
  if (!trimmed) return '';
  return trimmed.replace(/\/$/, '');
}

const state = {
  baseUrl: normalizeBaseUrl(detectDefaultBaseUrl()),
  apiKey: localStorage.getItem('pc2-api-key') || '',
  history: [],
  stationIds: null
};

const POLLUTANTS = [
  { key: 'pm25', label: 'PM 2.5' },
  { key: 'pm10', label: 'PM 10' },
  { key: 'so2', label: 'SO₂' },
  { key: 'no2', label: 'NO₂' },
  { key: 'o3', label: 'O₃' },
  { key: 'co', label: 'CO' }
];

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const statusEl = $('#status');
const historyEl = $('#request-history');
const historyPreviewEl = $('#history-preview');
const loadingTemplate = $('#loading-template');

if (statusEl) {
  statusEl.hidden = true;
}

function setStatus(message, type = 'info') {
  if (!statusEl) return;
  if (!message) {
    statusEl.textContent = '';
    statusEl.className = 'status';
    statusEl.hidden = true;
    return;
  }
  statusEl.hidden = false;
  statusEl.textContent = message;
  statusEl.className = `status ${type}`;
}

function showLoading(el) {
  if (!el) return;
  el.textContent = loadingTemplate.content.textContent;
}

function showResult(el, data) {
  if (!el) return;
  if (typeof data === 'string') {
    el.textContent = data;
  } else {
    el.textContent = JSON.stringify(data, null, 2);
  }
}

function examplePlaceholder(text) {
  return `<p class="example-placeholder">${escapeHtml(text)}</p>`;
}

function setExample(id, html) {
  const container = document.getElementById(id);
  if (!container) return;
  if (!html) {
    container.innerHTML = examplePlaceholder('Sin vista previa disponible.');
    return;
  }
  container.innerHTML = html;
}

function formatLocalDateTime(value) {
  if (!value) return '—';
  try {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString();
  } catch (error) {
    return value;
  }
}

function renderHealthExample(data) {
  if (!data || typeof data !== 'object') {
    setExample('health-example', examplePlaceholder('Sin datos de estado.'));
    return;
  }
  const status = String(data.status || data.state || data.message || 'desconocido').toLowerCase();
  let indicatorClass = 'warn';
  if (status.includes('ok') || status.includes('up')) indicatorClass = 'ok';
  if (status.includes('fail') || status.includes('down') || status.includes('error')) indicatorClass = 'error';
  const timestamp = data.timestamp || data.time || data.checked_at;
  const extras = Object.entries(data)
    .filter(([key]) => !['status', 'state', 'message', 'timestamp', 'time', 'checked_at'].includes(key))
    .slice(0, 4)
    .map(([key, value]) => `<span class="chip">${escapeHtml(`${key}: ${value}`)}</span>`)
    .join('');

  setExample('health-example', `
    <div class="example-title">Estado actualizado</div>
    <div class="health-glance">
      <span class="health-indicator ${indicatorClass}" aria-hidden="true"></span>
      <div class="health-meta">
        <strong>${escapeHtml(data.status || data.state || 'Sin estado')}</strong>
        <span>Última respuesta: ${escapeHtml(formatLocalDateTime(timestamp))}</span>
      </div>
    </div>
    ${extras ? `<div class="chip-list">${extras}</div>` : ''}
  `);
}

function renderStationsExample(data) {
  const items = Array.isArray(data?.items)
    ? data.items
    : Array.isArray(data)
      ? data
      : data && typeof data === 'object'
        ? [data]
        : [];

  if (!items.length) {
    setExample('stations-example', examplePlaceholder('No se encontraron estaciones con los filtros actuales.'));
    return;
  }

  const total = typeof data?.total === 'number' ? data.total : items.length;
  const cards = items.slice(0, 4).map((station) => {
    const id = station?.id ?? station?.station_id ?? '—';
    const name = station?.name || station?.station_name || `Estación ${id}`;
    const locationParts = ['district', 'province', 'department', 'city', 'region']
      .map((key) => station?.[key])
      .filter(Boolean);
    const location = locationParts.join(' • ');
    const lat = station?.latitude ?? station?.lat ?? station?.latitud ?? station?.location?.lat ?? station?.location?.latitude;
    const lon = station?.longitude ?? station?.lon ?? station?.longitud ?? station?.location?.lon ?? station?.location?.longitude;
    let coords = '';
    if (Number.isFinite(Number(lat)) && Number.isFinite(Number(lon))) {
      coords = `${Number(lat).toFixed(4)}, ${Number(lon).toFixed(4)}`;
    }
    const type = station?.station_type || station?.type;
    return `
      <article class="station-card">
        <h3>${escapeHtml(name)}</h3>
        <div class="station-meta">
          <span>ID: ${escapeHtml(String(id))}${type ? ` • ${escapeHtml(String(type))}` : ''}</span>
          ${location ? `<span>${escapeHtml(location)}</span>` : ''}
          ${coords ? `<span class="station-coords">${escapeHtml(coords)}</span>` : ''}
        </div>
      </article>
    `;
  }).join('');

  setExample('stations-example', `
    <div class="example-title">Vista rápida (${Math.min(items.length, 4)} mostradas de ${total})</div>
    <div class="station-grid">${cards}</div>
  `);
}

function renderMeasurementsExample(data) {
  const items = Array.isArray(data?.items)
    ? data.items
    : Array.isArray(data)
      ? data
      : data && typeof data === 'object'
        ? [data]
        : [];

  if (!items.length) {
    setExample('measurements-example', examplePlaceholder('Sin mediciones para graficar. Ajusta el rango de fechas.'));
    return;
  }

  const timelineItems = items
    .filter((item) => item && typeof item === 'object')
    .slice(0, 5)
    .map((item) => {
      const station = item.station_name || item.station || `Estación ${item.station_id ?? '—'}`;
      const ts = item.ts || item.timestamp || item.time || item.date;
      const pollutantBadges = POLLUTANTS.map((pollutant) => {
        const raw = item[pollutant.key];
        if (raw === undefined || raw === null) return null;
        const value = Number(raw);
        const label = Number.isFinite(value) ? value.toFixed(1) : raw;
        return `<span class="timeline-value">${escapeHtml(pollutant.label)}: ${escapeHtml(String(label))}</span>`;
      }).filter(Boolean);

      if (!pollutantBadges.length) {
        pollutantBadges.push('<span class="timeline-value">Sin valores reportados</span>');
      }

      return `
        <li class="timeline-item">
          <p class="timeline-title">${escapeHtml(station)}</p>
          <p class="timeline-meta">${escapeHtml(formatLocalDateTime(ts))}</p>
          <div class="timeline-values">${pollutantBadges.join('')}</div>
        </li>
      `;
    }).join('');

  setExample('measurements-example', `
    <div class="example-title">Últimas mediciones (${Math.min(items.length, 5)} mostradas)</div>
    <ol class="timeline">${timelineItems}</ol>
  `);
}

function renderAggregatesExample(data) {
  const items = Array.isArray(data?.items)
    ? data.items
    : Array.isArray(data)
      ? data
      : data && typeof data === 'object'
        ? [data]
        : [];

  if (!items.length) {
    setExample('aggregates-example', examplePlaceholder('Sin datos agregados para mostrar.')); 
    return;
  }

  const sample = items.slice(0, 4);
  const columns = Array.from(new Set(sample.flatMap((item) => Object.keys(item || {}))))
    .filter((key) => !['station_id', 'station_name'].includes(key) || sample.some((row) => row[key] !== undefined))
    .slice(0, 6);

  const header = columns.map((col) => `<th>${escapeHtml(col)}</th>`).join('');
  const rows = sample.map((row) => {
    const cells = columns.map((col) => {
      const value = row?.[col];
      if (col === 'ts' || col === 'timestamp' || col === 'date') {
        return `<td>${escapeHtml(formatLocalDateTime(value))}</td>`;
      }
      if (Number.isFinite(Number(value)) && value !== null && value !== '') {
        return `<td>${escapeHtml(Number(value).toFixed(2))}</td>`;
      }
      return `<td>${escapeHtml(value ?? '—')}</td>`;
    }).join('');
    return `<tr>${cells}</tr>`;
  }).join('');

  setExample('aggregates-example', `
    <div class="example-title">Muestra de agregados (${sample.length} filas)</div>
    <div class="example-scroll">
      <table class="aggregates-table">
        <thead><tr>${header}</tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `);
}

function renderAlertsExample(data) {
  const items = Array.isArray(data?.items)
    ? data.items
    : Array.isArray(data)
      ? data
      : data && typeof data === 'object'
        ? [data]
        : [];

  if (!items.length) {
    setExample('alerts-example', examplePlaceholder('No hay reglas o eventos para mostrar.'));
    return;
  }

  const list = items.slice(0, 5).map((item) => {
    const name = item.name || item.rule_name || `Regla ${item.id ?? item.rule_id ?? '—'}`;
    const pollutant = item.pollutant || item.metric;
    const threshold = item.threshold ?? item.value ?? item.level;
    const ts = item.triggered_at || item.created_at || item.updated_at || item.timestamp;
    return `
      <div class="alert-item">
        <strong>${escapeHtml(name)}</strong>
        ${pollutant ? `<span>Contaminante: ${escapeHtml(String(pollutant))}</span>` : ''}
        ${threshold !== undefined ? `<span>Umbral: ${escapeHtml(String(threshold))}</span>` : ''}
        ${ts ? `<small>${escapeHtml(formatLocalDateTime(ts))}</small>` : ''}
      </div>
    `;
  }).join('');

  setExample('alerts-example', `
    <div class="example-title">Ejemplos (${Math.min(items.length, 5)} mostrados)</div>
    <div class="alerts-list">${list}</div>
  `);
}

function buildUrl(path, params = {}) {
  const url = new URL(path, state.baseUrl.endsWith('/') ? state.baseUrl : state.baseUrl + '/');
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    if (Array.isArray(value)) {
      const items = value.filter((item) => item !== undefined && item !== null && item !== '');
      if (!items.length) return;
      url.searchParams.delete(key);
      items.forEach((item) => {
        url.searchParams.append(key, item);
      });
      return;
    }
    if (value === '') return;
    url.searchParams.set(key, value);
  });
  return url;
}

function formatDuration(ms) {
  if (!Number.isFinite(ms)) return '—';
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

function truncatePreview(text, limit = 1200) {
  if (!text) return 'Sin contenido';
  if (text.length <= limit) return text;
  return `${text.slice(0, limit)}…`;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>'"]/g, (char) => {
    switch (char) {
      case '&':
        return '&amp;';
      case '<':
        return '&lt;';
      case '>':
        return '&gt;';
      case '"':
        return '&quot;';
      case '\'':
        return '&#39;';
      default:
        return char;
    }
  });
}

function pushHistory(entry) {
  state.history.unshift(entry);
  if (state.history.length > 20) {
    state.history.length = 20;
  }
  renderHistory();
}

function renderHistory() {
  if (!historyEl) return;
  if (!state.history.length) {
    historyEl.innerHTML = '<li class="history-item"><div class="history-top"><span class="history-method">—</span><span class="history-url">Sin solicitudes registradas todavía.</span></div></li>';
    if (historyPreviewEl) {
      historyPreviewEl.textContent = 'Aquí aparecerán las respuestas del historial.';
    }
    return;
  }

  historyEl.innerHTML = state.history.map((entry, index) => {
    const url = new URL(entry.url);
    const path = `${url.pathname}${url.search}`;
    return `
      <li class="history-item ${entry.error ? 'error' : ''}">
        <div class="history-top">
          <span class="history-method">${escapeHtml(entry.method)}</span>
          <span>${escapeHtml(path)}</span>
          <span class="history-status ${entry.error ? 'error' : ''}">${escapeHtml(entry.statusText || entry.status)}</span>
        </div>
        <div class="history-url">${escapeHtml(url.origin)}</div>
        <div class="history-url">${escapeHtml(new Date(entry.timestamp).toLocaleTimeString())} • ${escapeHtml(formatDuration(entry.duration))}</div>
        <div class="history-actions">
          <button type="button" data-history-action="preview" data-history-index="${index}">Ver respuesta</button>
          <button type="button" data-history-action="copy" data-history-index="${index}">Copiar cURL</button>
        </div>
      </li>
    `;
  }).join('');
}

function shellEscape(value) {
  return `'${String(value).replace(/'/g, "'\\''")}'`;
}

function generateCurl(entry) {
  const parts = ['curl'];
  parts.push('-X', entry.method);
  parts.push(shellEscape(entry.url));
  Object.entries(entry.headers || {}).forEach(([key, value]) => {
    parts.push('-H', shellEscape(`${key}: ${value}`));
  });
  if (entry.body) {
    parts.push('--data', shellEscape(entry.body));
  }
  return parts.join(' ');
}

async function apiRequest(path, { method = 'GET', params, body, resultEl, expectJson = true, raw = false, logHistory = true } = {}) {
  if (resultEl) {
    showLoading(resultEl);
  }
  const headers = {};
  const startedAt = performance.now();
  const url = buildUrl(path, params);
  if (expectJson) headers['Accept'] = 'application/json';
  if (state.apiKey) headers['X-API-Key'] = state.apiKey;
  let payload;
  if (body !== undefined && body !== null) {
    headers['Content-Type'] = 'application/json';
    payload = JSON.stringify(body);
  }
  let responseText = '';
  try {
    const res = await fetch(url.toString(), {
      method,
      headers,
      body: payload
    });
    responseText = await res.text();
    const duration = performance.now() - startedAt;
    if (!res.ok) {
      const errorMessage = `${res.status} ${res.statusText}\n${responseText}`.trim();
      if (logHistory) {
        pushHistory({
          method,
          url: url.toString(),
          status: res.status,
          statusText: `${res.status} ${res.statusText}`,
          duration,
          preview: truncatePreview(responseText),
          headers,
          body: payload,
          error: true,
          timestamp: new Date().toISOString()
        });
      }
      const err = new Error(errorMessage);
      err.__historyLogged = true;
      throw err;
    }
    if (raw) {
      if (logHistory) {
        pushHistory({
          method,
          url: url.toString(),
          status: res.status,
          statusText: `${res.status} ${res.statusText}`,
          duration,
          preview: truncatePreview(responseText),
          headers,
          body: payload,
          error: false,
          timestamp: new Date().toISOString()
        });
      }
      return { res, text: responseText };
    }
    if (!responseText) {
      if (logHistory) {
        pushHistory({
          method,
          url: url.toString(),
          status: res.status,
          statusText: `${res.status} ${res.statusText}`,
          duration,
          preview: 'Sin contenido',
          headers,
          body: payload,
          error: false,
          timestamp: new Date().toISOString()
        });
      }
      return null;
    }
    if (!expectJson) {
      if (logHistory) {
        pushHistory({
          method,
          url: url.toString(),
          status: res.status,
          statusText: `${res.status} ${res.statusText}`,
          duration,
          preview: truncatePreview(responseText),
          headers,
          body: payload,
          error: false,
          timestamp: new Date().toISOString()
        });
      }
      return responseText;
    }
    try {
      const json = JSON.parse(responseText);
      if (logHistory) {
        pushHistory({
          method,
          url: url.toString(),
          status: res.status,
          statusText: `${res.status} ${res.statusText}`,
          duration,
          preview: truncatePreview(JSON.stringify(json, null, 2)),
          headers,
          body: payload,
          error: false,
          timestamp: new Date().toISOString()
        });
      }
      return json;
    } catch (err) {
      console.warn('Respuesta no JSON', err);
      if (logHistory) {
        pushHistory({
          method,
          url: url.toString(),
          status: res.status,
          statusText: `${res.status} ${res.statusText}`,
          duration,
          preview: truncatePreview(responseText),
          headers,
          body: payload,
          error: false,
          timestamp: new Date().toISOString()
        });
      }
      return responseText;
    }
  } catch (error) {
    if (resultEl) {
      showResult(resultEl, error.message || String(error));
    }
    if (!error.__historyLogged && logHistory) {
      pushHistory({
        method,
        url: url.toString(),
        status: 'error',
        statusText: 'Error de red',
        duration: performance.now() - startedAt,
        preview: truncatePreview(error.message || String(error)),
        headers,
        body: payload,
        error: true,
        timestamp: new Date().toISOString()
      });
    }
    throw error;
  }
}

async function testConnection() {
  if (!state.baseUrl) return;
  setStatus(`Verificando API en ${state.baseUrl}…`);
  try {
    const health = await apiRequest('v1/health', { logHistory: false });
    if (health) {
      let suffix = '';
      if (typeof health === 'object' && health !== null && 'status' in health) {
        suffix = ` • ${health.status}`;
      }
      setStatus(`Conectado a ${state.baseUrl}${suffix}`, 'success');
    } else {
      setStatus(`La API respondió sin contenido desde ${state.baseUrl}`, 'warn');
    }
  } catch (error) {
    setStatus(`No se pudo contactar la API (${error.message || error}).`, 'error');
  }
}

function initConfigForm() {
  const baseInput = $('#base-url');
  const apiInput = $('#api-key');
  baseInput.value = state.baseUrl || 'http://localhost:5000';
  apiInput.value = state.apiKey;

  $('#config-form').addEventListener('submit', (ev) => {
    ev.preventDefault();
    state.baseUrl = normalizeBaseUrl(baseInput.value);
    state.apiKey = apiInput.value.trim();
    localStorage.setItem('pc2-base-url', state.baseUrl);
    localStorage.setItem('pc2-api-key', state.apiKey);
    setStatus(`Configuración actualizada: ${state.baseUrl || '—'}`);
    testConnection();
  });
}

function attachCopyOnClick() {
  $$('.result').forEach((el) => {
    el.addEventListener('click', () => {
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(el);
      selection.removeAllRanges();
      selection.addRange(range);
      navigator.clipboard?.writeText(el.textContent || '').then(() => {
        setStatus('Resultado copiado al portapapeles.');
        setTimeout(() => setStatus(''), 2000);
      }).catch(() => {
        setStatus('Selecciona y copia manualmente.', 'warn');
      });
    });
  });
}

function parseNumberValue(value) {
  if (value === '' || value === null || value === undefined) return undefined;
  const num = Number(value);
  return Number.isFinite(num) ? num : undefined;
}

function toISO(value) {
  if (!value) return undefined;
  try {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toISOString();
  } catch (err) {
    return value;
  }
}

function parseCsvNumbers(value) {
  if (!value) return [];
  return value
    .split(/[,\s]+/)
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => Number(item))
    .filter((num) => Number.isFinite(num));
}

function parseCsvStrings(value) {
  if (!value) return [];
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

async function fetchAllStationIds() {
  if (Array.isArray(state.stationIds) && state.stationIds.length) {
    return state.stationIds;
  }
  const collected = new Set();
  const limit = 200;
  let offset = 0;
  let keepGoing = true;

  while (keepGoing) {
    const page = await apiRequest('v1/stations', {
      params: { limit, offset },
      logHistory: offset === 0
    });
    const items = page?.items || [];
    if (!items.length) {
      break;
    }
    items.forEach((item) => {
      const raw = item?.id ?? item?.station_id ?? item?.ID;
      const id = Number(raw);
      if (Number.isInteger(id) && id > 0) {
        collected.add(id);
      }
    });
    offset += items.length;
    const total = typeof page?.total === 'number' ? page.total : null;
    if (items.length < limit || (total !== null && offset >= total)) {
      keepGoing = false;
    }
  }

  state.stationIds = Array.from(collected).sort((a, b) => a - b);
  return state.stationIds;
}

function computeHourlyHeatmap(items) {
  if (!Array.isArray(items) || !items.length) {
    return null;
  }
  const accumulator = {};
  const uniqueBuckets = new Set();
  POLLUTANTS.forEach((pollutant) => {
    accumulator[pollutant.key] = {
      sums: Array(24).fill(0),
      counts: Array(24).fill(0)
    };
  });

  items.forEach((item) => {
    if (!item?.ts) return;
    const date = new Date(item.ts);
    if (Number.isNaN(date.getTime())) return;
    const hour = date.getHours();
    uniqueBuckets.add(item.ts.slice(0, 13));
    POLLUTANTS.forEach((pollutant) => {
      const raw = item[pollutant.key];
      if (raw === null || raw === undefined) return;
      const value = Number(raw);
      if (!Number.isFinite(value)) return;
      accumulator[pollutant.key].sums[hour] += value;
      accumulator[pollutant.key].counts[hour] += 1;
    });
  });

  let min = Infinity;
  let max = -Infinity;
  const rows = POLLUTANTS.map((pollutant) => {
    const { sums, counts } = accumulator[pollutant.key];
    const values = sums.map((sum, hour) => {
      const count = counts[hour];
      if (!count) return null;
      const avg = sum / count;
      if (avg < min) min = avg;
      if (avg > max) max = avg;
      return avg;
    });
    return { key: pollutant.key, label: pollutant.label, values };
  });

  if (min === Infinity || max === -Infinity) {
    return null;
  }

  return {
    hours: Array.from({ length: 24 }, (_, h) => h),
    rows,
    scale: { min, max },
    totals: {
      items: items.length,
      hourBuckets: uniqueBuckets.size
    }
  };
}

function heatmapColor(value, min, max) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return 'rgba(148, 163, 184, 0.18)';
  }
  if (max <= min) {
    return 'hsl(190, 70%, 70%)';
  }
  const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
  const hue = 190 - ratio * 150; // de azul verdoso a naranja
  const lightness = 82 - ratio * 40;
  return `hsl(${hue}, 70%, ${lightness}%)`;
}

function heatmapTextColor(value, min, max) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return 'var(--muted)';
  }
  if (max <= min) {
    return '#0f172a';
  }
  const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
  return ratio > 0.6 ? '#f8fafc' : '#0f172a';
}

function renderHeatmap(matrix, container) {
  if (!container) return;
  container.innerHTML = '';
  const { hours, rows, scale } = matrix;

  const table = document.createElement('table');
  table.className = 'heatmap-table';

  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  const labelHeader = document.createElement('th');
  labelHeader.textContent = 'Contaminante';
  headRow.appendChild(labelHeader);
  hours.forEach((hour) => {
    const th = document.createElement('th');
    th.textContent = hour.toString().padStart(2, '0');
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    const th = document.createElement('th');
    th.className = 'heatmap-label';
    th.scope = 'row';
    th.textContent = row.label;
    tr.appendChild(th);

    row.values.forEach((value, index) => {
      const td = document.createElement('td');
      td.className = 'heatmap-cell';
      if (value === null) {
        td.textContent = '—';
        td.style.background = 'rgba(148, 163, 184, 0.18)';
        td.style.color = 'var(--muted)';
      } else {
        td.textContent = Math.round(value).toString();
        const bg = heatmapColor(value, scale.min, scale.max);
        td.style.background = bg;
        td.style.color = heatmapTextColor(value, scale.min, scale.max);
        td.title = `${row.label} • ${hours[index]}h → ${value.toFixed(2)}`;
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.appendChild(table);

  const legend = document.createElement('div');
  legend.className = 'heatmap-legend';
  const minSwatch = document.createElement('span');
  minSwatch.innerHTML = `<i style="background:${heatmapColor(scale.min, scale.min, scale.max)}"></i>Min ${scale.min.toFixed(2)}`;
  const maxSwatch = document.createElement('span');
  maxSwatch.innerHTML = `<i style="background:${heatmapColor(scale.max, scale.min, scale.max)}"></i>Max ${scale.max.toFixed(2)}`;
  legend.append(minSwatch, maxSwatch);
  container.appendChild(legend);
}

async function loadHourlyHeatmap() {
  const container = document.getElementById('heatmap-container');
  const messageEl = document.getElementById('heatmap-message');
  if (!container || !messageEl) return;

  container.setAttribute('aria-busy', 'true');
  container.innerHTML = '<p class="heatmap-placeholder">Consultando datos…</p>';
  messageEl.textContent = 'Obteniendo estaciones y agregados horarios…';

  try {
    const stationIds = await fetchAllStationIds();
    if (!stationIds.length) {
      messageEl.textContent = 'No se encontraron estaciones registradas.';
      container.innerHTML = '<p class="heatmap-placeholder">Sin estaciones disponibles.</p>';
      return;
    }

    const params = { station_id: stationIds };
    const start = toISO(document.getElementById('heatmap-start')?.value);
    const end = toISO(document.getElementById('heatmap-end')?.value);
    const tz = document.getElementById('heatmap-tz')?.value?.trim();
    if (start) params.start = start;
    if (end) params.end = end;
    if (tz) params.tz = tz;

    const data = await apiRequest('v1/aggregates/hourly', {
      params,
      logHistory: true
    });

    const items = data?.items || [];
    if (!items.length) {
      messageEl.textContent = 'La API no devolvió datos para el rango indicado.';
      container.innerHTML = '<p class="heatmap-placeholder">Sin datos para mostrar.</p>';
      return;
    }

    const matrix = computeHourlyHeatmap(items);
    if (!matrix) {
      messageEl.textContent = 'No se pudo construir el mapa de calor con la respuesta recibida.';
      container.innerHTML = '<p class="heatmap-placeholder">Sin datos válidos.</p>';
      return;
    }

    renderHeatmap(matrix, container);
    const hours = matrix.totals.hourBuckets;
    const hourText = hours === 1 ? '1 hora agregada' : `${hours} horas agregadas`;
    const stationText = stationIds.length === 1 ? '1 estación' : `${stationIds.length} estaciones`;
    messageEl.textContent = `Promedios calculados con ${hourText} provenientes de ${stationText}.`;
  } catch (error) {
    messageEl.textContent = error.message || String(error);
    container.innerHTML = '<p class="heatmap-placeholder">Ocurrió un error al consultar la API.</p>';
  } finally {
    container.setAttribute('aria-busy', 'false');
  }
}

function gatherMeasurementParams() {
  const tz = $('#measurement-tz')?.value.trim();
  return {
    start: toISO($('#measurement-start')?.value),
    end: toISO($('#measurement-end')?.value),
    fields: $('#measurement-fields')?.value.trim(),
    limit: parseNumberValue($('#measurement-limit')?.value),
    offset: parseNumberValue($('#measurement-offset')?.value),
    order: $('#measurement-order')?.value || 'asc',
    tz: tz ? tz : undefined
  };
}

function registerActions() {
  const actions = {
    health: () => apiRequest('v1/health', { resultEl: $('#health-result') }).then((data) => {
      showResult($('#health-result'), data);
      renderHealthExample(data);
    }),
    stations: () => apiRequest('v1/stations', { resultEl: $('#stations-result') }).then((data) => {
      showResult($('#stations-result'), data);
      renderStationsExample(data);
    }),
    'station-detail': () => {
      const id = parseNumberValue($('#station-id').value);
      if (!id) {
        setStatus('Ingresa un ID de estación válido.', 'warn');
        return;
      }
      apiRequest(`v1/stations/${id}`, { resultEl: $('#stations-result') })
        .then((data) => {
          showResult($('#stations-result'), data);
          renderStationsExample(data);
        });
    },
    'station-latest': () => {
      const id = parseNumberValue($('#station-id').value);
      if (!id) {
        setStatus('Ingresa un ID de estación válido.', 'warn');
        return;
      }
      apiRequest(`v1/stations/${id}/latest`, { resultEl: $('#stations-result') })
        .then((data) => {
          showResult($('#stations-result'), data);
          renderMeasurementsExample(data);
        });
    },
    'station-measurements': () => {
      const id = parseNumberValue($('#station-id').value);
      if (!id) {
        setStatus('Ingresa un ID de estación válido.', 'warn');
        return;
      }
      const params = {
        limit: parseNumberValue($('#station-limit').value),
        offset: parseNumberValue($('#station-offset').value),
        start: toISO($('#station-start')?.value),
        end: toISO($('#station-end')?.value),
        fields: $('#station-fields')?.value.trim(),
        order: $('#station-order')?.value || 'desc'
      };
      const tz = $('#station-tz')?.value.trim();
      if (tz) params.tz = tz;
      apiRequest(`v1/stations/${id}/measurements`, { params, resultEl: $('#stations-result') })
        .then((data) => {
          showResult($('#stations-result'), data);
          renderMeasurementsExample(data);
        });
    },
    measurements: () => apiRequest('v1/measurements', { resultEl: $('#measurements-result') })
      .then((data) => {
        showResult($('#measurements-result'), data);
        renderMeasurementsExample(data);
      }),
    'measurements-latest': () => apiRequest('v1/measurements/latest', { resultEl: $('#measurements-result') })
      .then((data) => {
        showResult($('#measurements-result'), data);
        renderMeasurementsExample(data);
      }),
    'measurements-custom': () => {
      const params = gatherMeasurementParams();
      apiRequest('v1/measurements', { params, resultEl: $('#measurements-result') })
        .then((data) => {
          showResult($('#measurements-result'), data);
          renderMeasurementsExample(data);
        });
    },
    'measurements-range': () => {
      const params = gatherMeasurementParams();
      if (!params.start && !params.end) {
        setStatus('Selecciona al menos fecha de inicio o fin para aplicar el rango.', 'warn');
        return;
      }
      apiRequest('v1/measurements', { params, resultEl: $('#measurements-result') })
        .then((data) => {
          showResult($('#measurements-result'), data);
          renderMeasurementsExample(data);
        });
    },
    'measurements-by-ids': () => {
      const ids = parseCsvNumbers($('#measurement-station-ids')?.value);
      if (!ids.length) {
        setStatus('Ingresa al menos un ID de estación válido.', 'warn');
        return;
      }
      const params = { ...gatherMeasurementParams(), station_id: ids };
      apiRequest('v1/measurements', { params, resultEl: $('#measurements-result') })
        .then((data) => {
          showResult($('#measurements-result'), data);
          renderMeasurementsExample(data);
        });
    },
    'measurements-by-names': () => {
      const names = parseCsvStrings($('#measurement-station-names')?.value);
      if (!names.length) {
        setStatus('Ingresa al menos un nombre de estación.', 'warn');
        return;
      }
      const params = { ...gatherMeasurementParams(), station_name: names.join(',') };
      apiRequest('v1/measurements', { params, resultEl: $('#measurements-result') })
        .then((data) => {
          showResult($('#measurements-result'), data);
          renderMeasurementsExample(data);
        });
    },
    'heatmap-load': () => {
      loadHourlyHeatmap();
    },
    'alert-rules': () => apiRequest('v1/alerts/rules', { resultEl: $('#alerts-result') })
      .then((data) => {
        showResult($('#alerts-result'), data);
        renderAlertsExample(data);
      }),
    'alert-events': () => apiRequest('v1/alerts/events', { resultEl: $('#alerts-result') })
      .then((data) => {
        showResult($('#alerts-result'), data);
        renderAlertsExample(data);
      }),
    'export-csv': async () => {
      const query = $('#export-query').value.trim();
      const status = $('#export-status');
      status.textContent = 'Preparando descarga…';
      try {
        const url = buildUrl('v1/export/csv');
        if (query) {
          query.split('&').forEach((kv) => {
            const [k, v = ''] = kv.split('=');
            if (k) url.searchParams.set(k, v);
          });
        }
        const headers = {};
        if (state.apiKey) headers['X-API-Key'] = state.apiKey;
        const res = await fetch(url.toString(), { headers });
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(`${res.status} ${res.statusText}: ${txt}`);
        }
        const blob = await res.blob();
        const filename = res.headers.get('Content-Disposition')?.split('filename=')[1]?.replace(/"/g, '') || 'export.csv';
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(link.href);
        status.textContent = `Descarga completada (${filename}).`;
      } catch (error) {
        status.textContent = error.message || String(error);
      }
    }
  };

  document.body.addEventListener('click', (event) => {
    const target = event.target.closest('[data-action]');
    if (!target) return;
    const action = target.dataset.action;
    if (actions[action]) {
      actions[action]();
    }
  });
}

function initHistoryControls() {
  renderHistory();

  document.body.addEventListener('click', (event) => {
    const trigger = event.target.closest('[data-history-action]');
    if (!trigger) return;
    const index = Number(trigger.dataset.historyIndex);
    const entry = state.history[index];
    if (!entry) return;

    if (trigger.dataset.historyAction === 'preview') {
      if (historyPreviewEl) {
        historyPreviewEl.textContent = entry.preview || 'Sin contenido';
      }
      setStatus(`Mostrando respuesta de ${entry.method} ${entry.url}`);
    }

    if (trigger.dataset.historyAction === 'copy') {
      const command = generateCurl(entry);
      navigator.clipboard?.writeText(command).then(() => {
        setStatus('Comando cURL copiado al portapapeles.');
      }).catch(() => {
        setStatus('No se pudo copiar automáticamente. Copia manualmente.', 'warn');
      });
    }
  });

  const clearBtn = $('#clear-history');
  clearBtn?.addEventListener('click', () => {
    state.history = [];
    renderHistory();
    if (historyPreviewEl) {
      historyPreviewEl.textContent = 'Aquí aparecerán las respuestas del historial.';
    }
    setStatus('Historial limpio.');
  });
}

function registerForms() {
  $('#aggregates-form').addEventListener('submit', (ev) => {
    ev.preventDefault();
    const params = {
      station_id: parseNumberValue($('#aggregate-station').value),
      start: toISO($('#aggregate-start').value),
      end: toISO($('#aggregate-end').value),
      tz: $('#aggregate-tz').value.trim(),
      fields: $('#aggregate-fields').value.trim(),
      limit: parseNumberValue($('#aggregate-limit').value),
      offset: parseNumberValue($('#aggregate-offset').value)
    };
    const type = $('#aggregate-type').value;
    apiRequest(`v1/aggregates/${type}`, { params, resultEl: $('#aggregates-result') })
      .then((data) => {
        showResult($('#aggregates-result'), data);
        renderAggregatesExample(data);
      });
  });

  $('#create-rule-form').addEventListener('submit', (ev) => {
    ev.preventDefault();
    const payload = {
      name: $('#rule-name').value.trim(),
      station_id: parseNumberValue($('#rule-station').value),
      pollutant: $('#rule-pollutant').value,
      operator: $('#rule-operator').value,
      threshold: Number($('#rule-threshold').value),
      window: $('#rule-window').value.trim() || undefined,
      enabled: $('#rule-enabled').value === 'true'
    };
    if (!payload.station_id) delete payload.station_id;
    if (!payload.window) delete payload.window;

    const evaluate = $('#rule-evaluate').checked;
    const params = evaluate ? { evaluate_now: 'true' } : undefined;

    apiRequest('v1/alerts/rules', {
      method: 'POST',
      body: payload,
      params,
      resultEl: $('#alerts-result')
    }).then((data) => {
      showResult($('#alerts-result'), data);
      renderAlertsExample(data);
      $('#create-rule-form').reset();
    }).catch(() => {});
  });

  $('#evaluate-rules-form').addEventListener('submit', (ev) => {
    ev.preventDefault();
    const id = parseNumberValue($('#evaluate-rule-id').value);
    const body = Number.isInteger(id) && id > 0 ? { rule_id: id } : {};
    apiRequest('v1/alerts/evaluate', {
      method: 'POST',
      body,
      resultEl: $('#alerts-result')
    }).then((data) => {
      showResult($('#alerts-result'), data);
      if (data && typeof data === 'object' && 'events_created' in data) {
        const count = Number(data.events_created) || 0;
        const suffix = count === 1 ? 'evento nuevo.' : `${count} eventos nuevos.`;
        setStatus(`Evaluación completada: ${suffix}`, count ? 'success' : 'info');
      }
      $('#evaluate-rules-form').reset();
    }).catch(() => {});
  });

  $('#update-rule-form').addEventListener('submit', (ev) => {
    ev.preventDefault();
    const id = parseNumberValue($('#update-rule-id').value);
    if (!id) {
      setStatus('ID de regla inválido.', 'warn');
      return;
    }
    const payloadText = $('#update-payload').value.trim();
    if (!payloadText) {
      setStatus('Ingresa un payload JSON válido.', 'warn');
      return;
    }
    let payload;
    try {
      payload = JSON.parse(payloadText);
    } catch (error) {
      setStatus('JSON inválido en el payload.', 'warn');
      return;
    }
    apiRequest(`v1/alerts/rules/${id}`, {
      method: 'PUT',
      body: payload,
      resultEl: $('#alerts-result')
    }).then((data) => {
      showResult($('#alerts-result'), data);
      renderAlertsExample(data);
      $('#update-rule-form').reset();
    }).catch(() => {});
  });

  $('#delete-rule-form').addEventListener('submit', (ev) => {
    ev.preventDefault();
    const id = parseNumberValue($('#delete-rule-id').value);
    if (!id) {
      setStatus('ID de regla inválido.', 'warn');
      return;
    }
    if (!confirm(`¿Eliminar regla ${id}?`)) return;
    apiRequest(`v1/alerts/rules/${id}`, {
      method: 'DELETE',
      resultEl: $('#alerts-result')
    }).then((data) => {
      showResult($('#alerts-result'), data);
      renderAlertsExample(data);
      $('#delete-rule-form').reset();
    }).catch(() => {});
  });
}

window.addEventListener('DOMContentLoaded', () => {
  initConfigForm();
  attachCopyOnClick();
  registerActions();
  registerForms();
  initHistoryControls();
  testConnection();
});
