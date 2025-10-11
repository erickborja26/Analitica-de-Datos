const state = {
  baseUrl: localStorage.getItem('pc2-base-url') || 'http://localhost:5000',
  apiKey: localStorage.getItem('pc2-api-key') || '',
  history: []
};

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

function buildAuthHeaders({ acceptJson = false } = {}) {
  const headers = {};
  if (acceptJson) headers['Accept'] = 'application/json';
  if (state.apiKey) headers['X-API-Key'] = state.apiKey;
  return headers;
}

function buildUrl(path, params = {}) {
  const url = new URL(path, state.baseUrl.endsWith('/') ? state.baseUrl : state.baseUrl + '/');
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, value);
    }
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

function describeNetworkError(error, url) {
  const origin = (url && url.origin) || state.baseUrl;
  const message = (error && error.message) || 'Error de red desconocido.';
  const hints = [
    `No se pudo contactar ${origin}.`,
    'Verifica que el servidor Flask esté corriendo y accesible desde tu navegador.',
    'Revisa que la URL base sea correcta y que no exista un bloqueador de red o VPN impidiéndolo.'
  ];

  const lowerMessage = message.toLowerCase();
  if (lowerMessage.includes('failed to fetch') || lowerMessage.includes('networkerror')) {
    hints.push('Esto suele ocurrir cuando el backend está apagado, la URL es incorrecta o el navegador bloquea la petición por CORS.');
  }
  const pageProtocol = typeof window !== 'undefined' && window.location ? window.location.protocol : '';
  if (pageProtocol === 'https:' && origin.startsWith('http:')) {
    hints.push('Estás viendo la interfaz en HTTPS e intentando llamar un backend HTTP. El navegador podría bloquear contenido mixto; abre la app en HTTP o habilita HTTPS en la API.');
  }

  hints.push(`Mensaje original: ${message}`);
  return hints.join(' ');
}

async function apiRequest(path, { method = 'GET', params, body, resultEl, expectJson = true, raw = false } = {}) {
  if (resultEl) {
    showLoading(resultEl);
  }
  const headers = buildAuthHeaders({ acceptJson: expectJson });
  const startedAt = performance.now();
  const url = buildUrl(path, params);
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
      const err = new Error(errorMessage);
      err.__historyLogged = true;
      throw err;
    }
    if (raw) {
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
      return { res, text: responseText };
    }
    if (!responseText) {
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
      return null;
    }
    if (!expectJson) {
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
      return responseText;
    }
    try {
      const json = JSON.parse(responseText);
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
      return json;
    } catch (err) {
      console.warn('Respuesta no JSON', err);
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
      return responseText;
    }
  } catch (error) {
    let friendlyMessage = error.message || String(error);
    if (/failed to fetch|networkerror/i.test(friendlyMessage) || error.name === 'TypeError') {
      friendlyMessage = describeNetworkError(error, url);
      error.message = friendlyMessage;
    }

    if (resultEl) {
      showResult(resultEl, friendlyMessage);
    }
    if (!error.__historyLogged) {
      pushHistory({
        method,
        url: url.toString(),
        status: 'error',
        statusText: 'Error de red',
        duration: performance.now() - startedAt,
        preview: truncatePreview(friendlyMessage),
        headers,
        body: payload,
        error: true,
        timestamp: new Date().toISOString()
      });
    }
    setStatus(friendlyMessage, 'error');
    throw error;
  }
}

async function testConnection({ quiet = false } = {}) {
  const url = buildUrl('v1/health');
  const headers = buildAuthHeaders({ acceptJson: true });
  const startedAt = performance.now();
  if (!quiet) {
    setStatus(`Verificando conexión con ${url.origin}…`);
  }
  try {
    const res = await fetch(url.toString(), { headers });
    const duration = performance.now() - startedAt;
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`${res.status} ${res.statusText}: ${txt}`);
    }
    let details = '';
    const contentType = res.headers.get('Content-Type') || '';
    if (contentType.includes('application/json')) {
      try {
        const json = await res.json();
        details = json?.status || json?.message || JSON.stringify(json);
      } catch (err) {
        details = await res.text();
      }
    } else {
      details = await res.text();
    }
    const summary = `Conexión exitosa con ${url.origin} (${formatDuration(duration)}). ${details || ''}`.trim();
    setStatus(summary);
    return summary;
  } catch (error) {
    const friendlyMessage = describeNetworkError(error, url);
    setStatus(friendlyMessage, 'error');
    throw error;
  }
}

function initConfigForm() {
  const baseInput = $('#base-url');
  const apiInput = $('#api-key');
  baseInput.value = state.baseUrl;
  apiInput.value = state.apiKey;

  $('#config-form').addEventListener('submit', (ev) => {
    ev.preventDefault();
    state.baseUrl = baseInput.value.replace(/\/$/, '');
    state.apiKey = apiInput.value.trim();
    localStorage.setItem('pc2-base-url', state.baseUrl);
    localStorage.setItem('pc2-api-key', state.apiKey);
    setStatus(`Configuración actualizada: ${state.baseUrl}`);
    testConnection().catch(() => {});
  });

  const testBtn = $('#test-connection');
  if (testBtn) {
    testBtn.addEventListener('click', (ev) => {
      ev.preventDefault();
      state.baseUrl = baseInput.value.replace(/\/$/, '');
      state.apiKey = apiInput.value.trim();
      testConnection().catch(() => {});
    });
  }
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

function registerActions() {
  const actions = {
    health: () => apiRequest('v1/health', { resultEl: $('#health-result') }).then((data) => showResult($('#health-result'), data)),
    stations: () => apiRequest('v1/stations', { resultEl: $('#stations-result') }).then((data) => showResult($('#stations-result'), data)),
    'station-detail': () => {
      const id = parseNumberValue($('#station-id').value);
      if (!id) {
        setStatus('Ingresa un ID de estación válido.', 'warn');
        return;
      }
      apiRequest(`v1/stations/${id}`, { resultEl: $('#stations-result') })
        .then((data) => showResult($('#stations-result'), data));
    },
    'station-latest': () => {
      const id = parseNumberValue($('#station-id').value);
      if (!id) {
        setStatus('Ingresa un ID de estación válido.', 'warn');
        return;
      }
      apiRequest(`v1/stations/${id}/latest`, { resultEl: $('#stations-result') })
        .then((data) => showResult($('#stations-result'), data));
    },
    'station-measurements': () => {
      const id = parseNumberValue($('#station-id').value);
      if (!id) {
        setStatus('Ingresa un ID de estación válido.', 'warn');
        return;
      }
      const params = {
        limit: parseNumberValue($('#station-limit').value),
        offset: parseNumberValue($('#station-offset').value)
      };
      apiRequest(`v1/stations/${id}/measurements`, { params, resultEl: $('#stations-result') })
        .then((data) => showResult($('#stations-result'), data));
    },
    measurements: () => apiRequest('v1/measurements', { resultEl: $('#measurements-result') })
      .then((data) => showResult($('#measurements-result'), data)),
    'measurements-latest': () => apiRequest('v1/measurements/latest', { resultEl: $('#measurements-result') })
      .then((data) => showResult($('#measurements-result'), data)),
    'measurements-custom': () => {
      const params = {
        fields: $('#measurement-fields').value.trim(),
        limit: parseNumberValue($('#measurement-limit').value),
        offset: parseNumberValue($('#measurement-offset').value)
      };
      apiRequest('v1/measurements', { params, resultEl: $('#measurements-result') })
        .then((data) => showResult($('#measurements-result'), data));
    },
    'alert-rules': () => apiRequest('v1/alerts/rules', { resultEl: $('#alerts-result') })
      .then((data) => showResult($('#alerts-result'), data)),
    'alert-events': () => apiRequest('v1/alerts/events', { resultEl: $('#alerts-result') })
      .then((data) => showResult($('#alerts-result'), data)),
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
      .then((data) => showResult($('#aggregates-result'), data));
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
      $('#create-rule-form').reset();
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
  setStatus('Listo para enviar solicitudes.');
  testConnection({ quiet: true }).catch(() => {});
});
