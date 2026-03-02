// ─── CONSTANTS ────────────────────────────────────────────────────────────────

const API_BASE = '';

const NODE_COLORS = {
  pharma:    '#4FC3F7',
  physician: '#81C995',
  drug:      '#FFD54F',
  condition: '#F48FB1',
  device:    '#FFAB40',
};

const NODE_DIM = {
  pharma:    'rgba(79,195,247,0.05)',
  physician: 'rgba(129,201,149,0.05)',
  drug:      'rgba(255,213,79,0.05)',
  condition: 'rgba(244,143,177,0.05)',
  device:    'rgba(255,171,64,0.05)',
};

const EDGE_COLORS = {
  PAID:           '#FF7043',
  PEER_OF:        '#CE93D8',
  MANUFACTURES:   'rgba(100,160,220,0.22)',
  INDICATED_FOR:  'rgba(100,160,220,0.22)',
  SPECIALIZES_IN: 'rgba(100,160,220,0.22)',
  RECEIVED_FOR:   'rgba(100,160,220,0.22)',
};

const US_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
  'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
  'VA','WA','WV','WI','WY','DC',
];

const BADGE_STYLES = {
  pharma:    'background:rgba(79,195,247,0.15); color:#4FC3F7;',
  physician: 'background:rgba(129,201,149,0.15); color:#81C995;',
  drug:      'background:rgba(255,213,79,0.15);  color:#FFD54F;',
  condition: 'background:rgba(244,143,177,0.15); color:#F48FB1;',
  device:    'background:rgba(255,171,64,0.15); color:#FFAB40;',
};

const PANEL_BORDER = {
  pharma:    '#4FC3F7',
  physician: '#81C995',
  drug:      '#FFD54F',
  condition: '#F48FB1',
  device:    '#FFAB40',
};

// ─── APP STATE ────────────────────────────────────────────────────────────────

let G = null;
let currentData   = null;
let selectedNode  = null;
let highlightNodes = new Set();
let highlightLinks = new Set();
let allPharmaNodes = [];


// ─── GRAPH INIT ───────────────────────────────────────────────────────────────

function initGraph() {
  G = ForceGraph3D()(document.getElementById('graph'))
    .backgroundColor('#08090d')
    .nodeId('id')
    .nodeLabel(() => '')  // suppress default tooltip; we use sprites instead
    .nodeColor(node => {
      if (!highlightNodes.size) return NODE_COLORS[node.type] || '#ffffff';
      return highlightNodes.has(node.id)
        ? NODE_COLORS[node.type]
        : NODE_DIM[node.type];
    })
    .nodeVal(node => {
      if (node.type === 'pharma')    return Math.log((node.props.total_paid    || 500) / 50  + 2) * 8 + 8;
      if (node.type === 'physician') return Math.sqrt((node.props.total_received || 50)  / 1000) + 2;
      if (node.type === 'drug')      return 3;
      if (node.type === 'condition') return 5;
      if (node.type === 'device')    return 3;
      return 2;
    })
    .nodeOpacity(0.92)
    .nodeThreeObjectExtend(true)
    .nodeThreeObject(node => {
      if (typeof SpriteText === 'undefined') return null;
      const showLabel = node.type === 'pharma' ||
        (highlightNodes.size && highlightNodes.has(node.id));
      if (!showLabel) return null;
      const sprite = new SpriteText(node.label);
      sprite.color = node.type === 'pharma' ? '#ffffff' : (NODE_COLORS[node.type] || '#cccccc');
      sprite.textHeight = node.type === 'pharma' ? 5 : 4;
      sprite.fontWeight = node.type === 'pharma' ? 'bold' : 'normal';
      sprite.backgroundColor = 'rgba(8,9,13,0.55)';
      sprite.padding = 1.5;
      sprite.borderRadius = 2;
      return sprite;
    })
    .linkSource('source')
    .linkTarget('target')
    .linkColor(link => {
      if (!highlightLinks.size) return EDGE_COLORS[link.type] || 'rgba(100,160,220,0.18)';
      return highlightLinks.has(link)
        ? EDGE_COLORS[link.type]
        : 'rgba(255,255,255,0.02)';
    })
    .linkWidth(link => {
      const isPaid = link.type === 'PAID';
      const isPeer = link.type === 'PEER_OF';
      const base = isPaid
        ? Math.log((link.weight / 1000) + 1) * 0.3 + 0.2
        : isPeer ? 0.25
        : 0.15;
      return (highlightLinks.size && highlightLinks.has(link)) ? base * 4 : base;
    })
    .linkOpacity(0.8)
    .linkDirectionalParticles(link => (highlightLinks.has(link) && link.type === 'PAID') ? 2 : 0)
    .linkDirectionalParticleWidth(1.5)
    .linkDirectionalParticleSpeed(0.005)
    .d3AlphaDecay(0.015)
    .d3VelocityDecay(0.25)
    .onNodeClick(node  => handleNodeClick(node))
    .onNodeHover(node  => handleNodeHover(node))
    .onBackgroundClick(() => {
      selectedNode = null;
      resetHighlight();
      closeNodePanel();
    });

  G.d3Force('charge').strength(-40);
  G.d3Force('link').distance(30);
  G.controls().zoomSpeed = 5;

  window.addEventListener('resize', () => {
    G.width(window.innerWidth);
    G.height(window.innerHeight);
  });
}

// ─── GRAPH LOADING ────────────────────────────────────────────────────────────

async function loadGraph(state, year) {
  showLoading(state, year);
  selectedNode = null;
  resetHighlight();
  closeNodePanel();

  try {
    const res = await fetch(`${API_BASE}/api/graph/${state}/${year}`);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.detail || (Array.isArray(data.detail) ? data.detail.map(d => d.msg || d).join(', ') : null) || `HTTP ${res.status}`);
    }
    currentData = data;

    const graphNodes = JSON.parse(JSON.stringify(data.nodes));
    const graphLinks = JSON.parse(JSON.stringify(data.edges));
    G.graphData({ nodes: graphNodes, links: graphLinks });
    updateStatsBar(data);
    updateSidebar(data.nodes);
    hideLoading();
  } catch (err) {
    console.error(err);
    document.getElementById('loading-text').textContent =
      err.message || 'Could not load data. Is the API running on port 8000?';
  }
}

// ─── LOADING ──────────────────────────────────────────────────────────────────

function showLoading(state, year) {
  document.getElementById('loading-text').textContent =
    `Fetching clinical network for ${state} ${year}...`;
  document.getElementById('loading').style.display = 'flex';
}

function hideLoading() {
  document.getElementById('loading').style.display = 'none';
}

// ─── STATS BAR ────────────────────────────────────────────────────────────────

function updateStatsBar(data) {
  const totalPaid = (data.edges || [])
    .filter(e => e.type === 'PAID')
    .reduce((sum, e) => sum + Number(e.weight || 0), 0);

  document.getElementById('stat-nodes').textContent    = data.meta.node_count;
  document.getElementById('stat-edges').textContent    = data.meta.edge_count;
  document.getElementById('stat-payments').textContent = formatMoney(totalPaid);
  document.getElementById('stat-label').textContent    = `${data.meta.state} · ${data.meta.year}`;
  document.getElementById('stats-bar').style.display   = 'flex';
}

// ─── RANKED SIDEBAR ───────────────────────────────────────────────────────────

function updateSidebar(nodes) {
  allPharmaNodes = nodes
    .filter(n => n.type === 'pharma')
    .sort((a, b) => (b.props.total_paid || 0) - (a.props.total_paid || 0))
    .slice(0, 15);
  renderSidebar(allPharmaNodes);
}

function renderSidebar(list) {
  const container = document.getElementById('pharma-list');
  container.innerHTML = '';

  list.forEach((node, i) => {
    const btn = document.createElement('button');
    btn.className = 'pharma-row';
    btn.innerHTML = `
      <div style="display:flex; align-items:flex-start; gap:8px;">
        <span class="pharma-row-rank">${i + 1}</span>
        <div style="flex:1; min-width:0;">
          <div class="pharma-row-name">${node.label}</div>
          <div class="pharma-row-meta">
            <span class="pharma-row-paid">${formatMoney(node.props.total_paid || 0)}</span>
            <span class="pharma-row-count">${node.props.num_physicians || 0} physicians</span>
          </div>
        </div>
      </div>`;
    btn.onclick = () => flyToNode(node);
    container.appendChild(btn);
  });
}

function filterSidebar(query) {
  const q = query.toLowerCase();
  renderSidebar(allPharmaNodes.filter(n => n.label.toLowerCase().includes(q)));
}

function flyToNode(node) {
  if (!G) return;
  selectedNode = node;
  applyHighlight(node.id);
  showNodePanel(node);
  if (node.x !== undefined && node.y !== undefined && node.z !== undefined) {
    const dist = 220;
    const r = 1 + dist / Math.hypot(node.x || 1, node.y || 1, node.z || 1);
    G.cameraPosition(
      { x: node.x * r, y: node.y * r, z: node.z * r },
      node,
      1000
    );
  } else {
    const graph = G.graphData();
    if (graph && graph.nodes) {
      const graphNode = graph.nodes.find(n => n && n.id === node.id);
      if (graphNode && graphNode.x !== undefined) {
        const dist = 220;
        const r = 1 + dist / Math.hypot(graphNode.x || 1, graphNode.y || 1, graphNode.z || 1);
        G.cameraPosition(
          { x: graphNode.x * r, y: graphNode.y * r, z: graphNode.z * r },
          graphNode,
          1000
        );
      }
    }
  }
}

// ─── NODE INFO PANEL ──────────────────────────────────────────────────────────

function showNodePanel(node) {
  const badge = document.getElementById('panel-badge');
  badge.textContent = node.type;
  badge.style.cssText = `
    font-size:9px; font-weight:700; letter-spacing:0.15em; text-transform:uppercase;
    padding:2px 8px; border-radius:3px; display:inline-block; margin-bottom:8px;
    ${BADGE_STYLES[node.type] || ''}`;

  document.getElementById('panel-title').textContent     = node.label;
  document.getElementById('node-panel').style.borderLeftColor = PANEL_BORDER[node.type] || '#1A2D45';

  const propsEl = document.getElementById('panel-props');
  propsEl.innerHTML = '';

  const entries = Object.entries(node.props || {});
  const filtered = entries.filter(([, val]) => val !== '' && val !== null && val !== undefined);
  if (!filtered.length) {
    propsEl.innerHTML = '<p class="prop-val" style="color:#7B8FA6;">No additional data.</p>';
  } else {
    filtered.forEach(([key, val]) => {
      let display = Array.isArray(val) ? val.join(', ') : val;
      if (typeof val === 'number' && (key === 'total_paid' || key === 'total_received')) {
        display = formatMoney(val);
      }
      const row = document.createElement('div');
      row.innerHTML = `
        <div class="prop-key">${key.replace(/_/g, ' ')}</div>
        <div class="prop-val">${display}</div>`;
      propsEl.appendChild(row);
    });
  }

  const detail = document.getElementById('pharma-detail');
  const physicianDetail = document.getElementById('physician-detail');
  if (node.type === 'pharma') {
    detail.style.display = 'block';
    physicianDetail.style.display = 'none';
    showPharmaDetail(node);
  } else if (node.type === 'physician') {
    detail.style.display = 'none';
    physicianDetail.style.display = 'block';
    showPhysicianDetail(node);
  } else {
    detail.style.display = 'none';
    physicianDetail.style.display = 'none';
  }

  document.getElementById('node-panel').style.transform = 'translateX(0)';
}

function showPharmaDetail(pharmaNode) {
  const links = currentData ? currentData.edges : [];
  const nodes = currentData ? currentData.nodes : [];
  const nodeMap = Object.fromEntries(nodes.map(n => [n.id, n]));
  let pharmaId = pharmaNode && (pharmaNode.id != null && pharmaNode.id !== '' ? String(pharmaNode.id) : null);
  if (!pharmaId && pharmaNode && pharmaNode.label) {
    const byLabel = nodes.find(n => n.type === 'pharma' && n.label === pharmaNode.label);
    if (byLabel) pharmaId = String(byLabel.id);
  }
  if (!pharmaId) return;

  // Physicians paid by this pharma (source/target are strings from API)
  const paidEdges = links
    .filter(l => l.type === 'PAID' && String(l.source || '') === pharmaId)
    .sort((a, b) => Number(b.weight || 0) - Number(a.weight || 0));

  const physicianEl = document.getElementById('detail-physicians');
  physicianEl.innerHTML = '';
  if (!paidEdges.length) {
    physicianEl.innerHTML = '<div class="detail-item-meta">None found</div>';
  } else {
    paidEdges.slice(0, 20).forEach(edge => {
      const targetId = String(edge.target || '');
      const doc = nodeMap[targetId];
      if (!doc) return;
      const row = document.createElement('div');
      row.className = 'detail-item';
      row.innerHTML = `
        <span class="detail-item-name">${doc.label}</span>
        <span class="detail-item-meta">${formatMoney(edge.weight)}</span>`;
      row.onclick = () => { flyToNode(doc); applyHighlight(doc.id); };
      row.style.cursor = 'pointer';
      physicianEl.appendChild(row);
    });
    if (paidEdges.length > 20) {
      const more = document.createElement('div');
      more.className = 'detail-item-meta';
      more.style.paddingTop = '6px';
      more.textContent = `+ ${paidEdges.length - 20} more`;
      physicianEl.appendChild(more);
    }
  }

  // Drugs manufactured by this pharma
  const drugEdges = links
    .filter(l => l.type === 'MANUFACTURES' && String(l.source || '') === pharmaId);

  const drugsEl = document.getElementById('detail-drugs');
  drugsEl.innerHTML = '';
  if (!drugEdges.length) {
    drugsEl.innerHTML = '<div class="detail-item-meta">None found in OpenFDA</div>';
  } else {
    drugEdges.forEach(edge => {
      const drug = nodeMap[String(edge.target || '')];
      if (!drug || drug.type !== 'drug') return;
      const row = document.createElement('div');
      row.className = 'detail-item';
      row.innerHTML = `
        <span class="detail-item-name">${drug.label}</span>
        <span class="detail-item-meta">${drug.props.generic_name || ''}</span>`;
      drugsEl.appendChild(row);
    });
  }

  // Devices/products (from CMS payment product field — not in OpenFDA)
  const deviceNodes = drugEdges
    .map(e => nodeMap[String(e.target || '')])
    .filter(n => n && n.type === 'device');
  const devicesEl = document.getElementById('detail-devices');
  devicesEl.innerHTML = '';
  if (!deviceNodes.length) {
    devicesEl.innerHTML = '<div class="detail-item-meta">None in this dataset</div>';
  } else {
    deviceNodes.forEach(device => {
      const row = document.createElement('div');
      row.className = 'detail-item';
      row.innerHTML = `
        <span class="detail-item-name">${device.label}</span>
        <span class="detail-item-meta">${formatMoney(device.props.total_payments || 0)} in payments</span>`;
      devicesEl.appendChild(row);
    });
  }

  // Conditions — via drug → condition edges for drugs made by this pharma
  const drugIds = new Set(drugEdges.map(e => String(e.target || '')));
  const conditionIds = new Set(
    links
      .filter(l => l.type === 'INDICATED_FOR' && drugIds.has(String(l.source || '')))
      .map(l => String(l.target || ''))
  );

  const conditionsEl = document.getElementById('detail-conditions');
  conditionsEl.innerHTML = '';
  if (!conditionIds.size) {
    conditionsEl.innerHTML = '<div class="detail-item-meta">None parsed from drug labels</div>';
  } else {
    conditionIds.forEach(condId => {
      const cond = nodeMap[condId];
      if (!cond) return;
      const row = document.createElement('div');
      row.className = 'detail-item';
      row.innerHTML = `
        <span class="detail-item-name">${cond.label}</span>
        <span class="detail-item-meta">${cond.props.icd10_code || ''}</span>`;
      conditionsEl.appendChild(row);
    });
  }
}

function showPhysicianDetail(physicianNode) {
  const links = currentData ? currentData.edges : [];
  const nodes = currentData ? currentData.nodes : [];
  const nodeMap = Object.fromEntries(nodes.map(n => [n.id, n]));
  const physicianId = String(physicianNode.id || '');

  // Pharma companies that paid this physician
  const paidEdges = links
    .filter(l => l.type === 'PAID' && String(l.target || '') === physicianId)
    .sort((a, b) => Number(b.weight || 0) - Number(a.weight || 0));

  const payersEl = document.getElementById('detail-payers');
  payersEl.innerHTML = '';
  if (!paidEdges.length) {
    payersEl.innerHTML = '<div class="detail-item-meta">No payments recorded</div>';
  } else {
    paidEdges.forEach(edge => {
      const pharma = nodeMap[String(edge.source || '')];
      if (!pharma) return;
      const row = document.createElement('div');
      row.className = 'detail-item';
      row.style.cursor = 'pointer';
      row.innerHTML = `
        <span class="detail-item-name">${pharma.label}</span>
        <span class="detail-item-paid">${formatMoney(edge.weight)}</span>`;
      row.onclick = () => flyToNode(pharma);
      payersEl.appendChild(row);
    });
  }

  // Drugs/devices this physician received payments for
  const receivedEdges = links
    .filter(l => l.type === 'RECEIVED_FOR' && String(l.source || '') === physicianId);

  const productsEl = document.getElementById('detail-products');
  productsEl.innerHTML = '';
  if (!receivedEdges.length) {
    productsEl.innerHTML = '<div class="detail-item-meta">No specific products recorded</div>';
  } else {
    receivedEdges.forEach(edge => {
      const product = nodeMap[String(edge.target || '')];
      if (!product) return;
      const row = document.createElement('div');
      row.className = 'detail-item';
      row.innerHTML = `
        <span class="detail-item-name">${product.label}</span>
        <span class="detail-item-meta" style="color:${product.type === 'device' ? '#FFAB40' : '#FFD54F'};">${product.type}</span>`;
      productsEl.appendChild(row);
    });
  }
}

function closeNodePanel() {
  document.getElementById('node-panel').style.transform = 'translateX(100%)';
}

function resetCamera() {
  if (!G) return;
  selectedNode = null;
  resetHighlight();
  closeNodePanel();
  G.zoomToFit(600, 80);
}

// ─── HIGHLIGHT ────────────────────────────────────────────────────────────────

function getNodeId(val) {
  return typeof val === 'object' && val !== null ? val.id : val;
}

function applyHighlight(nodeId) {
  const links = G ? G.graphData().links : [];
  highlightNodes = new Set([nodeId]);
  highlightLinks = new Set();

  links.forEach(link => {
    const s = getNodeId(link.source);
    const t = getNodeId(link.target);
    if (s === nodeId || t === nodeId) {
      highlightNodes.add(s);
      highlightNodes.add(t);
      highlightLinks.add(link);
    }
  });
  refreshGraph();
}

function resetHighlight() {
  highlightNodes = new Set();
  highlightLinks = new Set();
  refreshGraph();
}

function refreshGraph() {
  if (!G) return;
  G.nodeColor(G.nodeColor());
  G.nodeThreeObject(G.nodeThreeObject());
  G.linkColor(G.linkColor());
  G.linkWidth(G.linkWidth());
}

// ─── EVENT HANDLERS ───────────────────────────────────────────────────────────

function handleNodeHover(node) {
  if (node) {
    applyHighlight(node.id);
  } else if (selectedNode) {
    applyHighlight(selectedNode.id);
  } else {
    resetHighlight();
  }
}

function handleNodeClick(node) {
  selectedNode = node;
  showNodePanel(node);
  applyHighlight(node.id);
}

// ─── LEGEND ───────────────────────────────────────────────────────────────────

function toggleLegend() {
  const body = document.getElementById('legend-body');
  const btn  = document.getElementById('legend-toggle');
  const open = body.style.display !== 'none';
  body.style.display = open ? 'none' : 'block';
  btn.textContent    = open ? '+' : '−';
}

// ─── SELECTORS ────────────────────────────────────────────────────────────────

// Only years with confirmed CMS dataset IDs
const VALID_YEARS = [2024, 2023, 2022, 2021, 2020, 2019, 2018];
const DEFAULT_YEAR = 2024;

function initStateSelect() {
  const sel = document.getElementById('state-select');
  US_STATES.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s;
    opt.textContent = s;
    if (s === 'GA') opt.selected = true;
    sel.appendChild(opt);
  });
}

function initYearSelect() {
  const sel = document.getElementById('year-select');
  VALID_YEARS.forEach(y => {
    const opt = document.createElement('option');
    opt.value = String(y);
    opt.textContent = String(y);
    if (y === DEFAULT_YEAR) opt.selected = true;
    sel.appendChild(opt);
  });
}

function getSelectedState() {
  return document.getElementById('state-select').value;
}

function getSelectedYear() {
  return document.getElementById('year-select').value;
}

function onSelectorChange() {
  loadGraph(getSelectedState(), getSelectedYear());
}

document.getElementById('state-select').addEventListener('change', onSelectorChange);
document.getElementById('year-select').addEventListener('change', onSelectorChange);

// ─── UTILITIES ────────────────────────────────────────────────────────────────

function formatMoney(val) {
  if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(1)}M`;
  if (val >= 1_000)     return `$${(val / 1_000).toFixed(1)}K`;
  return `$${Math.round(val)}`;
}

// ─── BOOT ─────────────────────────────────────────────────────────────────────

initStateSelect();
initYearSelect();
initGraph();
loadGraph(getSelectedState(), getSelectedYear());
