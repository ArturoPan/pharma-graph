// ─── CONSTANTS ────────────────────────────────────────────────────────────────

const API_BASE = '';

const NODE_COLORS = {
  pharma:    '#00E5FF',
  physician: '#00FF9D',
  drug:      '#FFD600',
  condition: '#FF4D8B',
};

const NODE_DIM = {
  pharma:    'rgba(0,229,255,0.06)',
  physician: 'rgba(0,255,157,0.06)',
  drug:      'rgba(255,214,0,0.06)',
  condition: 'rgba(255,77,139,0.06)',
};

const EDGE_COLORS = {
  PAID:           '#FF6B35',
  PEER_OF:        '#7B61FF',
  MANUFACTURES:   '#1A2D45',
  INDICATED_FOR:  '#1A2D45',
  SPECIALIZES_IN: '#1A2D45',
  RECEIVED_FOR:   '#1A2D45',
};

const US_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
  'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
  'VA','WA','WV','WI','WY','DC',
];

const BADGE_STYLES = {
  pharma:    'background:rgba(0,229,255,0.12); color:#00E5FF;',
  physician: 'background:rgba(0,255,157,0.12); color:#00FF9D;',
  drug:      'background:rgba(255,214,0,0.12);  color:#FFD600;',
  condition: 'background:rgba(255,77,139,0.12); color:#FF4D8B;',
};

const PANEL_BORDER = {
  pharma:    '#00E5FF',
  physician: '#00FF9D',
  drug:      '#FFD600',
  condition: '#FF4D8B',
};

// ─── APP STATE ────────────────────────────────────────────────────────────────

let G = null;
let currentData   = null;
let selectedNode  = null;
let highlightNodes = new Set();
let highlightLinks = new Set();
let allPharmaNodes = [];

// Follow the Money state
let ftmMode    = false;
let ftmStep    = 0;
let ftmPharmaId = null;

// ─── GRAPH INIT ───────────────────────────────────────────────────────────────

function initGraph() {
  G = ForceGraph3D()(document.getElementById('graph'))
    .backgroundColor('#050A0F')
    .nodeId('id')
    .nodeLabel(node => node.label)
    .nodeColor(node => {
      if (!highlightNodes.size) return NODE_COLORS[node.type] || '#ffffff';
      return highlightNodes.has(node.id)
        ? NODE_COLORS[node.type]
        : NODE_DIM[node.type];
    })
    .nodeVal(node => {
      if (node.type === 'pharma')    return Math.log((node.props.total_paid    || 500) / 50  + 2) * 6 + 6;
      if (node.type === 'physician') return Math.sqrt((node.props.total_received || 50)  / 2000) + 1;
      if (node.type === 'drug')      return 3;
      if (node.type === 'condition') return 5;
      return 2;
    })
    .nodeOpacity(0.9)
    .linkSource('source')
    .linkTarget('target')
    .linkColor(link => {
      if (!highlightLinks.size) return EDGE_COLORS[link.type] || '#1A2D45';
      return highlightLinks.has(link)
        ? EDGE_COLORS[link.type]
        : 'rgba(255,255,255,0.015)';
    })
    .linkWidth(link => {
      const base = link.type === 'PAID'
        ? Math.log((link.weight / 1000) + 1) + 0.5
        : 0.5;
      return (highlightLinks.size && highlightLinks.has(link)) ? base * 2.5 : base;
    })
    .linkDirectionalParticles(link => link.type === 'PAID' ? 4 : 0)
    .linkDirectionalParticleSpeed(0.005)
    .linkDirectionalParticleWidth(link => Math.log((link.weight / 5000) + 1) + 1)
    .linkDirectionalParticleColor(() => '#FF6B35')
    .d3AlphaDecay(0.015)
    .d3VelocityDecay(0.25)
    .onNodeClick(node  => handleNodeClick(node))
    .onNodeHover(node  => handleNodeHover(node))
    .onBackgroundClick(() => {
      if (ftmMode) return;
      selectedNode = null;
      resetHighlight();
      closeNodePanel();
    });

  // Tighter force physics — less repulsion, shorter links, nodes cluster closer
  G.d3Force('charge').strength(-25);
  G.d3Force('link').distance(25);

  // Faster scroll-wheel zoom so users can pull back from a node quickly
  G.controls().zoomSpeed = 3;

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
  if (ftmMode) resetFollowMoney();

  try {
    const res = await fetch(`${API_BASE}/api/graph/${state}/${year}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    currentData = data;

    G.graphData({ nodes: data.nodes, links: data.edges });
    updateStatsBar(data);
    updateSidebar(data.nodes);
    hideLoading();
  } catch (err) {
    console.error(err);
    document.getElementById('loading-text').textContent =
      'Could not load data. Is the API running on port 8000?';
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
  const totalPaid = data.edges
    .filter(e => e.type === 'PAID')
    .reduce((sum, e) => sum + e.weight, 0);

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
  if (!G || node.x === undefined) return;
  const dist = 220;
  const r = 1 + dist / Math.hypot(node.x || 1, node.y || 1, node.z || 1);
  G.cameraPosition(
    { x: node.x * r, y: node.y * r, z: node.z * r },
    node,
    1000
  );
  showNodePanel(node);
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
  if (!entries.length) {
    propsEl.innerHTML = '<p class="prop-val" style="color:#7B8FA6;">No additional data.</p>';
    return;
  }

  entries.forEach(([key, val]) => {
    let display = Array.isArray(val) ? val.join(', ') : val;
    if (typeof val === 'number' && (key === 'total_paid' || key === 'total_received')) {
      display = formatMoney(val);
    }
    if (display === '' || display === null || display === undefined) display = '—';

    const row = document.createElement('div');
    row.innerHTML = `
      <div class="prop-key">${key.replace(/_/g, ' ')}</div>
      <div class="prop-val">${display}</div>`;
    propsEl.appendChild(row);
  });

  // Show pharma detail lists if this is a pharma node
  const detail = document.getElementById('pharma-detail');
  if (node.type === 'pharma') {
    showPharmaDetail(node);
    detail.style.display = 'block';
  } else {
    detail.style.display = 'none';
  }

  document.getElementById('node-panel').style.transform = 'translateX(0)';
}

function showPharmaDetail(pharmaNode) {
  const links = currentData ? currentData.edges : [];
  const nodes = currentData ? currentData.nodes : [];
  const nodeMap = Object.fromEntries(nodes.map(n => [n.id, n]));

  // Physicians paid by this pharma — sorted by amount desc
  const paidEdges = links
    .filter(l => l.type === 'PAID' && getNodeId(l.source) === pharmaNode.id)
    .sort((a, b) => b.weight - a.weight);

  const physicianEl = document.getElementById('detail-physicians');
  physicianEl.innerHTML = '';
  if (!paidEdges.length) {
    physicianEl.innerHTML = '<div class="detail-item-meta">None found</div>';
  } else {
    paidEdges.slice(0, 20).forEach(edge => {
      const doc = nodeMap[getNodeId(edge.target)];
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
    .filter(l => l.type === 'MANUFACTURES' && getNodeId(l.source) === pharmaNode.id);

  const drugsEl = document.getElementById('detail-drugs');
  drugsEl.innerHTML = '';
  if (!drugEdges.length) {
    drugsEl.innerHTML = '<div class="detail-item-meta">None found in OpenFDA</div>';
  } else {
    drugEdges.forEach(edge => {
      const drug = nodeMap[getNodeId(edge.target)];
      if (!drug) return;
      const row = document.createElement('div');
      row.className = 'detail-item';
      row.innerHTML = `
        <span class="detail-item-name">${drug.label}</span>
        <span class="detail-item-meta">${drug.props.generic_name || ''}</span>`;
      drugsEl.appendChild(row);
    });
  }

  // Conditions — via drug → condition edges for drugs made by this pharma
  const drugIds = new Set(drugEdges.map(e => getNodeId(e.target)));
  const conditionIds = new Set(
    links
      .filter(l => l.type === 'INDICATED_FOR' && drugIds.has(getNodeId(l.source)))
      .map(l => getNodeId(l.target))
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

function closeNodePanel() {
  document.getElementById('node-panel').style.transform = 'translateX(100%)';
}

// ─── HIGHLIGHT ────────────────────────────────────────────────────────────────

function getNodeId(val) {
  return typeof val === 'object' && val !== null ? val.id : val;
}

function applyHighlight(nodeId) {
  const links = currentData ? currentData.edges : [];
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
  G.linkColor(G.linkColor());
  G.linkWidth(G.linkWidth());
}

// ─── EVENT HANDLERS ───────────────────────────────────────────────────────────

function handleNodeHover(node) {
  if (ftmMode) return;
  if (node) {
    applyHighlight(node.id);
  } else if (selectedNode) {
    applyHighlight(selectedNode.id);
  } else {
    resetHighlight();
  }
}

function handleNodeClick(node) {
  if (ftmMode) {
    handleFtmClick(node);
    return;
  }
  selectedNode = node;
  showNodePanel(node);
  applyHighlight(node.id);
}

// ─── FOLLOW THE MONEY ─────────────────────────────────────────────────────────

function toggleFollowMoney() {
  ftmMode ? resetFollowMoney() : startFollowMoney();
}

function startFollowMoney() {
  ftmMode     = true;
  ftmStep     = 1;
  ftmPharmaId = null;
  selectedNode = null;
  resetHighlight();
  closeNodePanel();

  const btn = document.getElementById('ftm-btn');
  btn.style.background = 'rgba(255,107,53,0.12)';
  btn.classList.add('ftm-glow');
  showFtmBar('Click a pharma company (cyan) to start.');
}

function handleFtmClick(node) {
  if (ftmStep === 1) {
    if (node.type !== 'pharma') {
      showFtmBar('Select a pharma company (cyan node) first.');
      return;
    }
    ftmPharmaId = node.id;
    ftmStep = 2;
    highlightNodes = new Set([node.id]);
    highlightLinks = new Set();
    refreshGraph();
    showFtmBar(`"${node.label}" selected — now click a drug (yellow node).`);

  } else if (ftmStep === 2) {
    if (node.type !== 'drug') {
      showFtmBar('Select a drug (yellow node).');
      return;
    }
    runFollowMoney(ftmPharmaId, node.id);
  }
}

function runFollowMoney(pharmaId, drugId) {
  const links = currentData ? currentData.edges : [];

  const paidByPharma = new Set(
    links
      .filter(l => l.type === 'PAID' && getNodeId(l.source) === pharmaId)
      .map(l => getNodeId(l.target))
  );

  const receivedForDrug = new Set(
    links
      .filter(l => l.type === 'RECEIVED_FOR' && getNodeId(l.target) === drugId)
      .map(l => getNodeId(l.source))
  );

  const intersection  = new Set([...paidByPharma].filter(id => receivedForDrug.has(id)));
  const targetSet     = intersection.size > 0 ? intersection : paidByPharma;

  highlightNodes = new Set([pharmaId, drugId, ...targetSet]);
  highlightLinks = new Set(
    links.filter(l => {
      const s = getNodeId(l.source);
      const t = getNodeId(l.target);
      return highlightNodes.has(s) && highlightNodes.has(t);
    })
  );
  refreshGraph();

  const n = targetSet.size;
  const msg = intersection.size > 0
    ? `${n} physician${n !== 1 ? 's' : ''} paid by this company for this drug.`
    : `${n} physician${n !== 1 ? 's' : ''} paid by this company (no direct drug match).`;
  showFtmBar(msg);
}

function resetFollowMoney() {
  ftmMode     = false;
  ftmStep     = 0;
  ftmPharmaId = null;

  const btn = document.getElementById('ftm-btn');
  btn.style.background = 'transparent';
  btn.classList.remove('ftm-glow');
  document.getElementById('ftm-bar').style.display = 'none';
  resetHighlight();
}

function showFtmBar(text) {
  document.getElementById('ftm-instruction').textContent = text;
  document.getElementById('ftm-bar').style.display = 'block';
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

function onSelectorChange() {
  loadGraph(
    document.getElementById('state-select').value,
    document.getElementById('year-select').value
  );
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
initGraph();
loadGraph('GA', '2023');
