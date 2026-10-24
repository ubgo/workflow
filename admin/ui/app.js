// Workflow admin UI. Plain script, no framework and no third-party code.
// Every value from the server is put on the page with textContent (the
// el helper), never as HTML, because run errors and labels are written
// by applications and may hold anything. Every call goes to the admin
// JSON API this page is served beside; writes send Content-Type:
// application/json, which the API requires (a cross-site form cannot).
'use strict';

(function () {
  // API is the handler's mount point: this page is at <mount>/ui/.
  const API = location.pathname.replace(/ui\/[^/]*$/, '');

  // STATUSES is workflow.Status, in lifecycle order, for the status filter.
  const STATUSES = ['scheduled', 'queued', 'running', 'waiting', 'retrying', 'succeeded', 'failed', 'cancelled', 'discarded'];
  const TERMINAL = new Set(['succeeded', 'failed', 'cancelled', 'discarded']);
  const PAGE_SIZES = [25, 50, 100, 200];
  const DEFAULT_SIZE = 50;
  const VIEWS = ['queues', 'runs', 'failures', 'discarded', 'schedules', 'charts', 'system'];
  const THEME_KEY = 'ubgo-workflow-admin-theme';
  // CONFIRM_MS is how long a destructive button waits for its second click.
  const CONFIRM_MS = 3000;

  const main = document.getElementById('main');
  let caps = null; // operation name -> supported; null until loaded (then buttons show and the API refuses)
  let capNames = null; // the client's capability names (Caps.String split on "|"); null when unknown
  let hasOutput = false; // an output source is wired (capabilities "output"); the run page then reads it
  let hasMetrics = false; // a metrics source is wired (capabilities "metrics"); the charts view then reads it
  const counts = new Map(); // filter key -> {total, truncated, cursors}

  // --- small helpers ---

  // el builds an element. attrs: class, title, href, type, value, on<event>
  // handlers, dataset via data-*; children are nodes or strings (as text).
  function el(tag, attrs, ...children) {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v === undefined || v === null || v === false) continue;
      if (k.startsWith('on')) n.addEventListener(k.slice(2), v);
      else if (k === 'class') n.className = v;
      else if (k === 'value') n.value = v;
      else n.setAttribute(k, v === true ? '' : String(v));
    }
    for (const c of children.flat()) {
      if (c === undefined || c === null || c === false) continue;
      n.append(c instanceof Node ? c : document.createTextNode(String(c)));
    }
    return n;
  }

  async function api(method, path, body) {
    const init = { method, credentials: 'same-origin', headers: { Accept: 'application/json' } };
    if (method !== 'GET') {
      init.headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(body === undefined ? {} : body);
    }
    const res = await fetch(API + path, init);
    let data = null;
    try { data = await res.json(); } catch (_) { /* empty or non-JSON body */ }
    if (!res.ok) {
      const msg = data && data.error ? data.error.message : res.status + ' ' + res.statusText;
      const err = new Error(msg);
      err.status = res.status;
      throw err;
    }
    return data;
  }

  let toastTimer = 0;
  function toast(msg, bad) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.className = 'toast show' + (bad ? ' bad' : '');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { t.className = 'toast'; }, bad ? 6000 : 2500);
  }

  const supported = (op) => caps === null || caps[op] === true;
  const hasCap = (name) => capNames === null || capNames.has(name);

  function pad2(n) { return String(n).padStart(2, '0'); }
  function fmtTime(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) + ' ' + pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
  }
  function fmtDur(secs) {
    if (secs === undefined || secs === null) return '';
    secs = Math.max(0, Math.round(secs));
    if (secs < 60) return secs + 's';
    if (secs < 3600) return Math.floor(secs / 60) + 'm ' + pad2(secs % 60) + 's';
    if (secs < 86400) return Math.floor(secs / 3600) + 'h ' + pad2(Math.floor(secs / 60) % 60) + 'm';
    return Math.floor(secs / 86400) + 'd ' + Math.floor(secs / 3600) % 24 + 'h';
  }
  function timeCell(iso) { return el('td', { class: 'nowrap num', title: iso || undefined }, fmtTime(iso)); }
  function badge(status) { return el('span', { class: 'badge s-' + status }, status); }
  function labelChips(labels) {
    return Object.keys(labels || {}).sort().map((k) => el('span', { class: 'label mono' }, k + '=' + labels[k]));
  }
  function runID(ref) { return ref.run_id || ref.scheduled_id || ''; }
  function runHref(ref) {
    return ref.run_id ? '#/run/' + encodeURIComponent(ref.run_id) : '#/run/' + encodeURIComponent(ref.scheduled_id) + '?scheduled=1';
  }
  function runPath(ref) {
    return ref.run_id ? 'runs/' + encodeURIComponent(ref.run_id) : 'runs/' + encodeURIComponent(ref.scheduled_id) + '?scheduled=1';
  }
  // scheduleHref links to one schedule's page.
  function scheduleHref(id) { return '#/schedule/' + encodeURIComponent(id); }
  // LABEL_SCHEDULE is workflow.LabelSchedule: every run a schedule fires
  // (tick or trigger) carries its schedule ID under it.
  const LABEL_SCHEDULE = 'ubgo.workflow/schedule';
  // LABEL_TRIGGER is workflow.LabelTrigger, set on a run a trigger started.
  const LABEL_TRIGGER = 'ubgo.workflow/trigger';
  // secsBetween is b minus a in seconds, or undefined when either is unset.
  function secsBetween(a, b) {
    if (!a || !b) return undefined;
    const d = (new Date(b) - new Date(a)) / 1000;
    return isNaN(d) ? undefined : d;
  }
  // canForceComplete and canForceFail mirror workflow.CanForceComplete and
  // workflow.CanForceFail, so the buttons show only where the driver accepts them.
  const canForceComplete = (st) => !TERMINAL.has(st) || st === 'failed';
  const canForceFail = (st) => !TERMINAL.has(st);

  // withSuffix puts a path suffix before a ref path's query string.
  function withSuffix(path, suffix) {
    const i = path.indexOf('?');
    return i < 0 ? path + suffix : path.slice(0, i) + suffix + path.slice(i);
  }

  // button runs an async action, disabled while it runs; reload after.
  // danger buttons need a second click within CONFIRM_MS.
  function button(text, action, opts) {
    opts = opts || {};
    let armed = false;
    let timer = 0;
    const b = el('button', { type: 'button', class: 'btn' + (opts.danger ? ' danger' : '') + (opts.primary ? ' primary' : ''), title: opts.title });
    b.textContent = text;
    b.addEventListener('click', async (ev) => {
      ev.stopPropagation();
      if (opts.danger && !armed) {
        armed = true;
        b.textContent = 'Confirm ' + text.toLowerCase() + '?';
        timer = setTimeout(() => { armed = false; b.textContent = text; }, CONFIRM_MS);
        return;
      }
      clearTimeout(timer);
      armed = false;
      b.disabled = true;
      try {
        const msg = await action();
        toast(msg || text + ': done');
        if (!opts.noReload) await render();
        else b.disabled = false; // the view stays: the button can be used again
      } catch (e) {
        toast(text + ': ' + e.message, true);
        b.disabled = false;
        b.textContent = text;
      }
    });
    return b;
  }

  // --- routing: #/<view>[/<id>][?query] ---

  function parseHash() {
    const raw = location.hash.replace(/^#\/?/, '');
    const qi = raw.indexOf('?');
    const path = qi < 0 ? raw : raw.slice(0, qi);
    const params = new URLSearchParams(qi < 0 ? '' : raw.slice(qi + 1));
    const [view, ...rest] = path.split('/');
    return { view: view || 'queues', id: rest.length ? decodeURIComponent(rest.join('/')) : '', params };
  }
  function go(view, params) {
    const qs = params && [...params.keys()].length ? '?' + params.toString() : '';
    location.hash = '#/' + view + qs;
  }

  // --- views ---

  async function render() {
    const r = parseHash();
    for (const a of document.querySelectorAll('#nav a')) {
      const v = a.dataset.view;
      a.classList.toggle('on', v === r.view || (r.view === 'run' && v === 'runs') || (r.view === 'schedule' && v === 'schedules'));
    }
    let node;
    try {
      switch (r.view) {
        case 'queues': node = await viewQueues(); break;
        case 'runs': case 'failures': case 'discarded': node = await viewRuns(r.view, r.params); break;
        case 'run': node = await viewRun(r.id, r.params); break;
        case 'schedules': node = await viewSchedules(r.params); break;
        case 'schedule': node = await viewSchedule(r.id); break;
        case 'charts': node = await viewCharts(r.params); break;
        case 'system': node = await viewSystem(); break;
        default: node = el('p', { class: 'empty' }, 'No such page.');
      }
    } catch (e) {
      node = el('p', { class: 'empty' }, e.status === 403 ? 'Not allowed: ' + e.message : 'Could not load: ' + e.message);
    }
    main.replaceChildren(node);
    // shown names the view on screen, so a script driving the page can
    // wait for a render to finish rather than for any table.
    main.dataset.shown = location.hash;
  }

  async function viewQueues() {
    const canPause = supported('pause');
    const [q, p] = await Promise.all([
      api('GET', 'queues'),
      canPause ? api('GET', 'pauses').catch(() => ({ pauses: [] })) : Promise.resolve({ pauses: [] }),
    ]);
    const pausedQueues = new Set(p.pauses.filter((x) => x.queue).map((x) => x.queue));
    const workers = q.queues.some((x) => x.workers !== undefined);
    const wfInput = el('input', { type: 'text', placeholder: 'workflow name', 'aria-label': 'Workflow to pause', class: 'mono w-md' });
    const tools = el('div', { class: 'toolbar' },
      canPause && hasCap('PauseWorkflow') && el('label', null, 'Pause workflow', wfInput),
      canPause && hasCap('PauseWorkflow') && button('Pause', async () => {
        const wf = wfInput.value.trim();
        if (!wf) throw new Error('enter a workflow name');
        await api('POST', 'pause', { workflow: wf });
        return 'Paused workflow ' + wf;
      }),
      el('span', { class: 'grow' }),
      el('span', { class: 'muted small' }, 'at ' + fmtTime(q.at) + (q.truncated ? ' · depth is a lower bound (scan limit reached)' : '')),
      canPause && hasCap('PauseWorkflow') && button('Pause all', async () => { await api('POST', 'pause', { all: true }); return 'Paused everything'; }, { danger: true }),
    );

    const pauseRows = p.pauses.map((x) => {
      const target = x.all ? { all: true } : x.workflow ? { workflow: x.workflow } : { queue: x.queue };
      const what = x.all ? 'everything' : x.workflow ? 'workflow ' + x.workflow : 'queue ' + x.queue;
      return el('tr', null,
        el('td', null, el('span', { class: 'badge s-paused' }, 'paused'), ' ', el('span', { class: 'mono' }, what)),
        el('td', { class: 'acts' }, button('Resume', async () => { await api('POST', 'resume', target); return 'Resumed ' + what; })));
    });
    const pausesBlock = canPause && p.pauses.length
      ? el('div', { class: 'section' }, el('h2', null, 'Active pauses'),
        el('div', { class: 'tablewrap' }, el('table', null, el('tbody', null, pauseRows))))
      : null;

    const rows = q.queues.map((x) => el('tr', null,
      el('td', { class: 'mono' }, el('a', { href: '#/runs?queue=' + encodeURIComponent(x.queue) + '&status=queued' }, x.queue),
        pausedQueues.has(x.queue) ? [' ', el('span', { class: 'badge s-paused' }, 'paused')] : null),
      el('td', { class: 'num' }, x.depth),
      el('td', { class: 'num' }, x.depth ? fmtDur(x.oldest_wait_seconds) : ''),
      timeCell(x.depth ? x.oldest_enqueued_at : ''),
      workers && el('td', { class: 'num' }, x.workers === undefined ? '' : x.workers),
      workers && el('td', { class: 'num' }, x.running === undefined ? '' : x.running),
      el('td', { class: 'acts' }, canPause && hasCap('PauseQueue')
        ? (pausedQueues.has(x.queue)
          ? button('Resume', async () => { await api('POST', 'resume', { queue: x.queue }); return 'Resumed queue ' + x.queue; })
          : button('Pause', async () => { await api('POST', 'pause', { queue: x.queue }); return 'Paused queue ' + x.queue; }))
        : null)));
    const table = q.queues.length
      ? el('div', { class: 'tablewrap' }, el('table', null,
        el('thead', null, el('tr', null, el('th', null, 'Queue'), el('th', { class: 'num' }, 'Depth'), el('th', { class: 'num' }, 'Oldest wait'),
          el('th', { class: 'num' }, 'Oldest enqueued'), workers && el('th', { class: 'num' }, 'Workers'), workers && el('th', { class: 'num' }, 'Running'), el('th', null, ''))),
        el('tbody', null, rows)))
      : el('p', { class: 'empty' }, 'No queued runs.');
    return el('div', null, tools, pausesBlock, el('h2', null, 'Queues'), table);
  }

  // filterKey identifies a list query without its page, for the count cache.
  function filterKey(kind, params) {
    const p = new URLSearchParams(params);
    p.delete('page');
    p.sort();
    return kind + '?' + p.toString();
  }

  // apiQuery turns the page's hash params into the API's query string.
  function apiQuery(kind, params, size) {
    const q = new URLSearchParams();
    if (kind === 'runs') for (const s of params.getAll('status')) q.append('status', s);
    for (const k of ['workflow', 'queue', 'tenant']) if (params.get(k)) q.set(k, params.get(k));
    for (const l of (params.get('label') || '').split(/\s+/)) if (l) q.append('label', l);
    q.set('limit', String(size));
    return q;
  }

  async function viewRuns(kind, params) {
    const size = PAGE_SIZES.includes(Number(params.get('size'))) ? Number(params.get('size')) : DEFAULT_SIZE;
    let page = Math.max(1, parseInt(params.get('page') || '1', 10) || 1);
    const key = filterKey(kind, params);
    const q = apiQuery(kind, params, size);
    let cnt = counts.get(key);
    let data;
    if (!cnt || page > cnt.cursors.length) {
      // First visit to this filter (or a stale page number): count and
      // take the cursors, then open the page asked for.
      const cq = new URLSearchParams(q);
      cq.set('count', '1');
      const first = await api('GET', kind + '?' + cq.toString());
      cnt = { total: first.total || 0, truncated: !!first.truncated, cursors: first.cursors || [''] };
      counts.set(key, cnt);
      page = Math.min(page, cnt.cursors.length);
      data = page === 1 ? first : null;
    }
    if (!data) {
      const pq = new URLSearchParams(q);
      if (cnt.cursors[page - 1]) pq.set('cursor', cnt.cursors[page - 1]);
      data = await api('GET', kind + '?' + pq.toString());
    }

    const setParam = (k, v) => {
      const p = new URLSearchParams(params);
      if (v === '' || v === null || v === undefined) p.delete(k); else p.set(k, v);
      p.delete('page');
      go(kind, p);
    };
    // width is a class (w-sm, w-md, w-lg): the page's CSP refuses inline styles.
    const input = (name, placeholder, width) => el('input', {
      type: 'text', name, placeholder, value: params.get(name) || '', class: 'mono filter ' + width,
      'aria-label': placeholder,
      onkeydown: (e) => { if (e.key === 'Enter') setParam(name, e.target.value.trim()); },
      onchange: (e) => setParam(name, e.target.value.trim()),
    });

    const chosen = new Set(params.getAll('status'));
    const chips = kind === 'runs' ? el('span', { class: 'chips', role: 'group', 'aria-label': 'Status' }, STATUSES.map((s) => el('button', {
      type: 'button', class: 'btn' + (chosen.has(s) ? ' on' : ''), 'aria-pressed': chosen.has(s) ? 'true' : 'false', 'data-status': s,
      onclick: () => {
        const p = new URLSearchParams(params);
        const next = new Set(chosen);
        if (next.has(s)) next.delete(s); else next.add(s);
        p.delete('status');
        for (const x of STATUSES) if (next.has(x)) p.append('status', x);
        p.delete('page');
        go(kind, p);
      },
    }, s))) : null;

    const tools = el('div', { class: 'toolbar' },
      chips,
      input('workflow', 'workflow', 'w-md'),
      input('queue', 'queue', 'w-sm'),
      hasCap('Tenant') ? input('tenant', 'tenant', 'w-sm') : null,
      input('label', 'label key=value or key=pre* …', 'w-lg'),
      el('button', { type: 'button', class: 'btn', title: 'Clear filters (Esc)', onclick: () => go(kind, new URLSearchParams()) }, 'Clear'),
    );

    const bulk = bulkNode(kind, params, data.runs.length > 0, () => counts.delete(key));

    const canRetry = supported('retry');
    const canDelete = supported('delete');
    const canCancel = supported('cancel');
    const rows = data.runs.map((r) => {
      const href = runHref(r.ref);
      const path = runPath(r.ref);
      return el('tr', { class: 'click', onclick: () => { location.hash = href; } },
        el('td', { class: 'mono nowrap' }, el('a', { href }, runID(r.ref))),
        el('td', { class: 'mono' }, r.workflow),
        el('td', { class: 'mono' }, r.queue),
        el('td', null, badge(r.status)),
        el('td', { class: 'num' }, r.attempts),
        timeCell(r.enqueued_at),
        timeCell(r.finished_at || (r.status === 'retrying' ? r.next_attempt_at : '')),
        el('td', { class: 'err', title: r.error || undefined }, r.failed_step ? r.failed_step + ': ' : '', r.error || ''),
        el('td', null, labelChips(r.labels)),
        el('td', { class: 'acts' },
          canRetry && r.status === 'failed' ? button('Retry', async () => { const o = await api('POST', withSuffix(path, '/retry')); counts.delete(key); return 'Retried as ' + runID(o.ref); }) : null,
          canCancel && !TERMINAL.has(r.status) ? button('Cancel', async () => { await api('POST', withSuffix(path, '/cancel')); counts.delete(key); return 'Cancelled ' + runID(r.ref); }, { danger: true }) : null,
          canDelete && TERMINAL.has(r.status) ? button('Delete', async () => { await api('DELETE', path); counts.delete(key); return 'Deleted ' + runID(r.ref); }, { danger: true }) : null));
    });
    const table = data.runs.length
      ? el('div', { class: 'tablewrap' }, el('table', null,
        el('thead', null, el('tr', null, el('th', null, 'Run'), el('th', null, 'Workflow'), el('th', null, 'Queue'), el('th', null, 'Status'),
          el('th', { class: 'num' }, 'Att.'), el('th', { class: 'num' }, 'Enqueued'), el('th', { class: 'num' }, 'Finished / next'), el('th', null, 'Error'), el('th', null, 'Labels'), el('th', null, ''))),
        el('tbody', null, rows)))
      : el('p', { class: 'empty' }, 'No runs match.');

    const pager = pagerNode({
      page, pages: cnt.cursors.length, total: cnt.total, truncated: cnt.truncated, size, shown: data.runs.length,
      onPage: (n) => { const p = new URLSearchParams(params); if (n === 1) p.delete('page'); else p.set('page', String(n)); go(kind, p); },
      onSize: (n) => setParam('size', n === DEFAULT_SIZE ? '' : String(n)),
    });
    return el('div', null, tools, bulk, table, pager);
  }

  // filterBody turns the page's filters into a bulk call's JSON body
  // (admin's filterJSON): label=k=v pairs, a value ending in * as a prefix.
  function filterBody(params, statuses) {
    const b = {};
    for (const k of ['workflow', 'queue', 'tenant']) if (params.get(k)) b[k] = params.get(k);
    for (const l of (params.get('label') || '').split(/\s+/)) {
      if (!l) continue;
      const i = l.indexOf('=');
      if (i < 1) continue;
      const k = l.slice(0, i);
      const v = l.slice(i + 1);
      if (v.endsWith('*')) (b.label_prefixes = b.label_prefixes || {})[k] = v.slice(0, -1);
      else (b.labels = b.labels || {})[k] = v;
    }
    if (statuses && statuses.length) b.statuses = statuses;
    return b;
  }

  // bulkNode is the "all matching" actions of a run list: each first asks
  // the server for a dry run, shows how many runs it would touch, and acts
  // only on a second click. The statuses sent follow what each action
  // accepts: retry only failed runs, delete only finished ones, cancel only
  // unfinished ones (the server refuses anything else).
  function bulkNode(kind, params, any, onDone) {
    const chosen = kind === 'runs' ? params.getAll('status') : [kind === 'failures' ? 'failed' : 'discarded'];
    const acts = [];
    const retryable = kind === 'failures' || (kind === 'runs' && chosen.length === 1 && chosen[0] === 'failed');
    if (supported('retry_all') && retryable) acts.push({ label: 'Retry all matching', verb: 'retry', past: 'Retried', path: 'runs/retry', statuses: [] });
    const finished = chosen.length > 0 && chosen.every((x) => TERMINAL.has(x));
    if (supported('delete_all') && finished) acts.push({ label: 'Delete all matching', verb: 'delete', past: 'Deleted', path: 'runs/delete', statuses: chosen, danger: true });
    const open = kind === 'runs' && chosen.length > 0 && chosen.every((x) => !TERMINAL.has(x));
    if (supported('cancel_all') && open) acts.push({ label: 'Cancel all matching', verb: 'cancel', past: 'Cancelled', path: 'runs/cancel', statuses: chosen, danger: true });
    if (!acts.length || !any) return null;
    const bar = el('div', { class: 'bulkbar', hidden: true, role: 'region', 'aria-label': 'Bulk action preview' });
    const start = async (a) => {
      const body = filterBody(params, a.statuses);
      const prev = await api('POST', a.path, Object.assign({ dry_run: true }, body));
      const n = prev.matched;
      const sample = prev.sample.slice(0, 5).map(runID).join(', ') + (n > 5 ? ', …' : '');
      bar.replaceChildren(
        el('strong', null, a.label.replace(' all matching', '') + ' ' + n + ' run' + (n === 1 ? '' : 's') + (prev.truncated ? '+ (first batch; repeat for the rest)' : '') + '?'),
        el('span', { class: 'sample mono small' }, sample),
        el('span', { class: 'grow' }),
        n ? button('Confirm ' + a.verb, async () => {
          const res = await api('POST', a.path, body);
          onDone();
          if (res.cancelled !== undefined) return 'Cancelled ' + res.cancelled + ' runs';
          return a.past + ' ' + res.done + (res.skipped ? ', skipped ' + res.skipped + ' that changed meanwhile' : '') + (res.truncated ? '; more match, run it again' : '');
        }, { primary: !a.danger, danger: false }) : null,
        el('button', { type: 'button', class: 'btn', onclick: () => { bar.hidden = true; } }, 'Close'));
      bar.hidden = false;
      return 'Preview: ' + n + ' matching';
    };
    return el('div', null,
      el('div', { class: 'toolbar' }, el('span', { class: 'muted small' }, 'All runs matching these filters:'),
        acts.map((a) => button(a.label + '…', () => start(a), { noReload: true, title: 'Shows how many runs first; nothing changes until you confirm' }))),
      bar);
  }

  // pageList is the numbered buttons: first, last, and two either side of
  // the current page, with gaps (0) where pages are skipped.
  function pageList(page, pages) {
    const out = [];
    for (let i = 1; i <= pages; i++) {
      if (i === 1 || i === pages || Math.abs(i - page) <= 2) out.push(i);
      else if (out[out.length - 1] !== 0) out.push(0);
    }
    return out;
  }

  function pagerNode(o) {
    const from = o.shown ? (o.page - 1) * o.size + 1 : 0;
    const to = (o.page - 1) * o.size + o.shown;
    const nav = (label, n, title) => el('button', {
      type: 'button', class: 'btn', title, 'data-page': n, disabled: n < 1 || n > o.pages || n === o.page,
      onclick: () => o.onPage(n),
    }, label);
    return el('div', { class: 'pager', role: 'navigation', 'aria-label': 'Pages' },
      el('span', { class: 'count muted' }, from + '–' + to + ' of ' + o.total + (o.truncated ? '+' : '')),
      nav('«', 1, 'First page'),
      nav('‹', o.page - 1, 'Previous page (←)'),
      pageList(o.page, o.pages).map((n) => n === 0
        ? el('span', { class: 'gap' }, '…')
        : el('button', { type: 'button', class: 'btn' + (n === o.page ? ' on' : ''), 'data-page': n, 'aria-current': n === o.page ? 'page' : undefined, onclick: () => o.onPage(n) }, n)),
      nav('›', o.page + 1, 'Next page (→)'),
      nav('»', o.pages, 'Last page'),
      el('span', { class: 'grow' }),
      el('label', { class: 'muted' }, 'Per page ', el('select', {
        'aria-label': 'Page size', name: 'size',
        onchange: (e) => o.onSize(Number(e.target.value)),
      }, PAGE_SIZES.map((n) => el('option', { value: String(n), selected: n === o.size }, n)))),
    );
  }

  // payloadText shows a base64 payload as text when it is UTF-8, else as
  // base64.
  function payloadText(b64) {
    const bin = atob(b64);
    const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
    try {
      const s = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
      try { return JSON.stringify(JSON.parse(s), null, 2); } catch (_) { return s; }
    } catch (_) {
      return 'base64: ' + b64;
    }
  }

  async function viewRun(id, params) {
    const ref = params.get('scheduled') === '1' ? { scheduled_id: id } : { run_id: id };
    const path = runPath(ref);
    const r = await api('GET', path);
    const kv = (k, v) => [el('dt', null, k), el('dd', null, v)];
    const canForce = supported('force');
    const reason = el('input', { type: 'text', name: 'reason', placeholder: 'reason (optional)', 'aria-label': 'Reason to force-fail', class: 'w-lg' });
    const actions = el('div', { class: 'toolbar' },
      el('a', { href: '#/runs', class: 'btn linkbtn' }, '← Runs'),
      el('span', { class: 'mono' }, runID(r.ref)), badge(r.status),
      el('span', { class: 'grow' }),
      supported('retry') && r.status === 'failed' ? button('Retry', async () => { const o = await api('POST', withSuffix(path, '/retry')); counts.clear(); return 'Retried as ' + runID(o.ref); }, { primary: true }) : null,
      supported('cancel') && !TERMINAL.has(r.status) ? button('Cancel', async () => { await api('POST', withSuffix(path, '/cancel')); counts.clear(); return 'Cancelled'; }, { danger: true }) : null,
      supported('delete') && TERMINAL.has(r.status) ? button('Delete', async () => { await api('DELETE', path); counts.clear(); location.hash = '#/runs'; return 'Deleted'; }, { danger: true, noReload: true }) : null,
      canForce && canForceComplete(r.status) ? button('Force complete', async () => { await api('POST', withSuffix(path, '/complete')); counts.clear(); return 'Marked succeeded'; }, { danger: true, title: 'End the run as succeeded without running its remaining steps' }) : null,
      canForce && canForceFail(r.status) ? reason : null,
      supported('reschedule') && r.status === 'scheduled' ? rescheduleNodes(path) : null,
      supported('cancel_step') && !TERMINAL.has(r.status) && r.status !== 'scheduled' && r.status !== 'queued' ? cancelStepNodes(path, r.failed_step) : null,
      canForce && canForceFail(r.status) ? button('Force fail', async () => { await api('POST', withSuffix(path, '/fail'), { reason: reason.value.trim() }); counts.clear(); return 'Marked failed'; }, { danger: true, title: 'End the run as failed now, with no retry' }) : null,
    );
    const wait = secsBetween(r.enqueued_at, r.started_at);
    const took = secsBetween(r.started_at, r.finished_at);
    const grid = el('dl', { class: 'kv' },
      kv('Run ID', el('span', { class: 'mono' }, r.ref.run_id || '—')),
      r.ref.scheduled_id ? kv('Scheduled ID', el('span', { class: 'mono' }, r.ref.scheduled_id)) : null,
      kv('Workflow', el('a', { href: '#/runs?workflow=' + encodeURIComponent(r.workflow), class: 'mono' }, r.workflow)),
      kv('Queue', el('a', { href: '#/runs?queue=' + encodeURIComponent(r.queue), class: 'mono' }, r.queue)),
      r.tenant ? kv('Tenant', el('a', { href: '#/runs?tenant=' + encodeURIComponent(r.tenant), class: 'mono' }, r.tenant)) : null,
      r.run_at ? kv('Due', fmtTime(r.run_at)) : null,
      r.progress ? kv('Progress', progressNode(r.progress)) : null,
      r.attempted_by && r.attempted_by.length ? kv('Attempted by', el('span', { class: 'mono' }, r.attempted_by.join(', '))) : null,
      kv('Status', badge(r.status)),
      kv('Priority', r.priority),
      kv('Attempts', r.attempts),
      r.next_attempt ? kv('Next attempt', '#' + r.next_attempt + ' at ' + fmtTime(r.next_attempt_at)) : null,
      kv('Enqueued', fmtTime(r.enqueued_at)),
      kv('Started', fmtTime(r.started_at) || '—'),
      kv('Finished', fmtTime(r.finished_at) || '—'),
      kv('Queue wait', wait === undefined ? '—' : fmtDur(wait)),
      kv('Duration', took === undefined ? (r.started_at ? 'running' : '—') : fmtDur(took)),
      r.labels && r.labels[LABEL_SCHEDULE] ? kv('Schedule', el('a', { href: scheduleHref(r.labels[LABEL_SCHEDULE]), class: 'mono' }, r.labels[LABEL_SCHEDULE])) : null,
      r.failed_step ? kv('Failed step', el('span', { class: 'mono' }, r.failed_step)) : null,
      kv('Labels', r.labels && Object.keys(r.labels).length ? labelChips(r.labels) : '—'),
    );
    const output = hasOutput && r.ref.run_id ? await api('GET', withSuffix(path, '/output')).catch((e) => ({ error: e.message })) : null;
    return el('div', null, actions, grid,
      r.error ? [el('h2', null, r.status === 'retrying' ? 'Error of the failed attempt' : 'Error'), el('pre', { class: 'box' }, r.error)] : null,
      attemptsNode(r.errors),
      resultNode(r),
      outputNode(output),
      r.payload ? [el('h2', null, 'Payload'), el('pre', { class: 'box' }, payloadText(r.payload))]
        : el('p', { class: 'muted small pad' }, 'Payload hidden (the handler was built without WithPayloads).'));
  }

  // progressNode is the latest progress a step reported.
  function progressNode(p) {
    return [el('progress', { max: '100', value: String(Math.max(0, Math.min(100, p.percent))) }), ' ',
      el('span', { class: 'num' }, Math.round(p.percent) + '%'), p.message ? ' ' + p.message : '',
      el('span', { class: 'muted small' }, (p.step ? ' · ' + p.step : '') + (p.at ? ' · ' + fmtTime(p.at) : ''))];
  }

  // resultNode is a succeeded run's result: shown when the handler sends
  // payloads (WithPayloads), else a note that it is hidden.
  function resultNode(r) {
    if (r.output) return el('div', { class: 'section', id: 'result' }, el('h2', null, 'Result'), el('pre', { class: 'box' }, payloadText(r.output)));
    if (r.has_output) return el('p', { class: 'muted small pad' }, 'Result hidden (the handler was built without WithPayloads).');
    return null;
  }

  // rescheduleNodes moves a delayed run: a local date and time, sent as UTC.
  function rescheduleNodes(path) {
    const at = el('input', { type: 'datetime-local', 'aria-label': 'New due time', name: 'at' });
    return [at, button('Reschedule', async () => {
      if (!at.value) throw new Error('pick a time');
      const d = new Date(at.value);
      if (isNaN(d)) throw new Error('not a time');
      await api('POST', withSuffix(path, '/reschedule'), { at: d.toISOString() });
      counts.clear();
      return 'Rescheduled to ' + fmtTime(d.toISOString());
    }, { title: 'Run it at this time instead (local time)' })];
  }

  // cancelStepNodes withdraws one step; the run decides what follows.
  function cancelStepNodes(path, hint) {
    const step = el('input', { type: 'text', name: 'step', placeholder: 'step name', value: hint || '', 'aria-label': 'Step to cancel', class: 'mono w-sm' });
    return [step, button('Cancel step', async () => {
      const name = step.value.trim();
      if (!name) throw new Error('enter a step name');
      await api('POST', withSuffix(path, '/steps/' + encodeURIComponent(name) + '/cancel'));
      counts.clear();
      return 'Cancelled step ' + name;
    }, { danger: true, title: 'Withdraw this step: a running attempt is told to stop; a waiting one fails' })];
  }

  // attemptsNode is the per-attempt error history, oldest first, with a
  // stack under each error the handler was allowed to send (WithStacks).
  function attemptsNode(errors) {
    if (!errors || !errors.length) return null;
    const rows = errors.map((e) => el('tr', null,
      el('td', { class: 'num' }, e.attempt),
      el('td', { class: 'mono' }, e.step, e.node ? el('span', { class: 'muted small' }, ' on ' + e.node) : null),
      timeCell(e.at),
      el('td', { class: 'errcell' }, e.error, e.stack ? el('details', null, el('summary', { class: 'muted small' }, 'stack'), el('pre', { class: 'box' }, e.stack)) : null)));
    return el('div', { class: 'section', id: 'attempts' }, el('h2', null, 'Failed attempts (' + errors.length + ')'),
      el('div', { class: 'tablewrap' }, el('table', null,
        el('thead', null, el('tr', null, el('th', { class: 'num' }, 'Attempt'), el('th', null, 'Step'), el('th', { class: 'num' }, 'At'), el('th', null, 'Error'))),
        el('tbody', null, rows))));
  }

  // outputNode is the captured output of each attempt (WithOutputs).
  function outputNode(out) {
    if (!out) return null;
    if (out.error) return el('p', { class: 'muted small pad' }, 'Output: ' + out.error);
    if (!out.attempts.length) return el('div', { class: 'section', id: 'output' }, el('h2', null, 'Output'), el('p', { class: 'muted small pad' }, 'No output captured yet: an attempt\'s output is stored when it ends.'));
    return el('div', { class: 'section', id: 'output' }, el('h2', null, 'Output'),
      out.attempts.map((o, i) => el('details', { class: 'out', open: i === out.attempts.length - 1 },
        el('summary', null, el('span', { class: 'mono' }, o.step + ' #' + o.attempt), ' ',
          o.error ? el('span', { class: 'badge s-failed' }, 'error') : el('span', { class: 'badge s-succeeded' }, 'ok'), ' ',
          el('span', { class: 'muted small' }, fmtTime(o.started) + (secsBetween(o.started, o.finished) !== undefined ? ' · ' + fmtDur(secsBetween(o.started, o.finished)) : '')),
          o.truncated ? el('span', { class: 'muted small' }, ' · truncated') : null),
        el('pre', { class: 'box' }, o.text))));
  }

  // lastRunNode is the run a schedule's last fire started, with that run's
  // status ("gone" once the driver forgot it): a dash until it fires, or
  // on a driver without schedule history.
  function lastRunNode(last) {
    if (!last || !last.run || !(last.run.run_id || last.run.scheduled_id)) return '—';
    return [el('a', { href: runHref(last.run), class: 'mono' }, runID(last.run)), ' ', last.status ? badge(last.status) : el('span', { class: 'muted small' }, 'gone')];
  }

  // triggerButton runs a schedule now; the toast names the new run, or
  // says why the driver refused (an unfinished previous run under the
  // skip overlap policy is a 409).
  function triggerButton(s, opts) {
    return button('Run now', async () => {
      const o = await api('POST', 'schedules/' + encodeURIComponent(s.id) + '/trigger');
      counts.clear();
      return 'Triggered ' + (s.name || s.id) + ' as ' + runID(o.ref);
    }, Object.assign({ title: 'Enqueue a run now with the schedule\'s payload and labels' + (s.overlap === 'skip' ? '; refused while its previous run is unfinished' : '') }, opts || {}));
  }

  async function viewSchedule(id) {
    const s = await api('GET', 'schedules/' + encodeURIComponent(id));
    const kv = (k, v) => [el('dt', null, k), el('dd', null, v)];
    const win = (ws) => (ws || []).map((w) => w.from + '–' + w.to).join(', ');
    const runsQ = new URLSearchParams();
    runsQ.append('label', LABEL_SCHEDULE + '=' + s.id);
    runsQ.set('limit', '10');
    const recent = supported('runs') ? await api('GET', 'runs?' + runsQ.toString()).catch(() => null) : null;
    const actions = el('div', { class: 'toolbar' },
      el('a', { href: '#/schedules', class: 'btn linkbtn' }, '← Schedules'),
      el('span', { class: 'mono' }, s.name || s.id),
      s.paused ? el('span', { class: 'badge s-paused' }, 'paused') : el('span', { class: 'badge s-succeeded' }, 'active'),
      el('span', { class: 'grow' }),
      supported('schedule_trigger') ? triggerButton(s, { primary: true }) : null,
      supported('schedule_pause') ? (s.paused
        ? button('Resume', async () => { await api('POST', 'schedules/' + encodeURIComponent(s.id) + '/resume'); return 'Resumed ' + (s.name || s.id); })
        : button('Pause', async () => { await api('POST', 'schedules/' + encodeURIComponent(s.id) + '/pause'); return 'Paused ' + (s.name || s.id); })) : null,
    );
    const grid = el('dl', { class: 'kv' },
      kv('ID', el('span', { class: 'mono' }, s.id)),
      s.name ? kv('Name', el('span', { class: 'mono' }, s.name)) : null,
      kv('Workflow', el('a', { href: '#/runs?workflow=' + encodeURIComponent(s.workflow), class: 'mono' }, s.workflow)),
      s.tenant ? kv('Tenant', el('a', { href: '#/runs?tenant=' + encodeURIComponent(s.tenant), class: 'mono' }, s.tenant)) : null,
      kv('Cron', el('span', { class: 'mono' }, s.cron)),
      kv('Zone', s.location || '—'),
      kv('Declared', s.declared ? 'yes (Workflow.Cron)' : 'no'),
      kv('Overlap', s.overlap === 'skip' ? 'skip a fire while the previous run is unfinished' : 'allow'),
      kv('Next fire', s.paused ? 'paused' : (fmtTime(s.next) || '—')),
      kv('Last fire', s.last && s.last.fire ? fmtTime(s.last.fire) : (hasCap('ScheduleHistory') ? 'never' : 'not recorded by this driver')),
      kv('Last run', lastRunNode(s.last)),
      s.between ? kv('Between', win(s.between)) : null,
      s.unless ? kv('Unless', win(s.unless)) : null,
      kv('Labels', s.labels && Object.keys(s.labels).length ? labelChips(s.labels) : '—'),
    );
    let runs = null;
    if (recent) {
      const rows = recent.runs.map((r) => el('tr', { class: 'click', onclick: () => { location.hash = runHref(r.ref); } },
        el('td', { class: 'mono nowrap' }, el('a', { href: runHref(r.ref) }, runID(r.ref))),
        el('td', null, badge(r.status), r.labels && r.labels[LABEL_TRIGGER] ? el('span', { class: 'muted small' }, ' manual') : null),
        el('td', { class: 'num' }, r.attempts),
        timeCell(r.enqueued_at),
        timeCell(r.finished_at),
        el('td', { class: 'err', title: r.error || undefined }, r.error || '')));
      runs = el('div', { class: 'section' }, el('h2', null, 'Recent runs'),
        rows.length ? el('div', { class: 'tablewrap' }, el('table', null,
          el('thead', null, el('tr', null, el('th', null, 'Run'), el('th', null, 'Status'), el('th', { class: 'num' }, 'Att.'), el('th', { class: 'num' }, 'Enqueued'), el('th', { class: 'num' }, 'Finished'), el('th', null, 'Error'))),
          el('tbody', null, rows))) : el('p', { class: 'empty' }, 'No runs kept for this schedule.'),
        el('p', { class: 'pad small' }, el('a', { href: '#/runs?label=' + encodeURIComponent(LABEL_SCHEDULE + '=' + s.id) }, 'All runs of this schedule →')));
    }
    return el('div', null, actions, grid, runs);
  }

  async function viewSchedules(params) {
    const size = PAGE_SIZES.includes(Number(params.get('size'))) ? Number(params.get('size')) : DEFAULT_SIZE;
    const page = Math.max(1, parseInt(params.get('page') || '1', 10) || 1);
    const data = await api('GET', 'schedules?limit=' + size + '&cursor=' + (page - 1) * size);
    const canPause = supported('schedule_pause');
    const canPauseWf = supported('pause');
    const win = (ws) => (ws || []).map((w) => w.from + '–' + w.to).join(', ');
    const canTrigger = supported('schedule_trigger');
    const rows = data.schedules.map((s) => el('tr', null,
      el('td', { class: 'mono' }, el('a', { href: scheduleHref(s.id) }, s.name || s.id), s.declared ? el('span', { class: 'muted small' }, ' (declared)') : null,
        s.overlap === 'skip' ? el('span', { class: 'muted small', title: 'Overlap: skip a fire while the previous run is unfinished' }, ' no-overlap') : null),
      el('td', { class: 'mono' }, el('a', { href: '#/runs?workflow=' + encodeURIComponent(s.workflow) }, s.workflow)),
      el('td', { class: 'mono' }, s.tenant || ''),
      el('td', { class: 'mono nowrap' }, s.cron),
      el('td', { class: 'mono' }, s.location),
      timeCell(s.paused ? '' : s.next),
      timeCell(s.last ? s.last.fire : ''),
      el('td', { class: 'nowrap' }, s.last ? lastRunNode(s.last) : ''),
      el('td', { class: 'small' }, s.between ? 'between ' + win(s.between) : '', s.unless ? ' unless ' + win(s.unless) : ''),
      el('td', null, labelChips(s.labels)),
      el('td', null, s.paused ? el('span', { class: 'badge s-paused' }, 'paused') : el('span', { class: 'badge s-succeeded' }, 'active')),
      el('td', { class: 'acts' },
        canTrigger ? triggerButton(s) : null,
        canPause ? (s.paused
          ? button('Resume', async () => { await api('POST', 'schedules/' + encodeURIComponent(s.id) + '/resume'); return 'Resumed ' + (s.name || s.id); })
          : button('Pause', async () => { await api('POST', 'schedules/' + encodeURIComponent(s.id) + '/pause'); return 'Paused ' + (s.name || s.id); }))
          : null,
        canPauseWf && hasCap('PauseWorkflow') ? button('Pause workflow', async () => { await api('POST', 'pause', { workflow: s.workflow }); return 'Paused workflow ' + s.workflow; }, { title: 'Hold every run of ' + s.workflow }) : null)));
    const table = data.schedules.length
      ? el('div', { class: 'tablewrap' }, el('table', null,
        el('thead', null, el('tr', null, el('th', null, 'Schedule'), el('th', null, 'Workflow'), el('th', null, 'Tenant'), el('th', null, 'Cron'), el('th', null, 'Zone'),
          el('th', { class: 'num', title: 'Next fire time in your browser\'s zone; the schedule runs in its Zone' }, 'Next (local)'),
          el('th', { class: 'num' }, 'Last fire'), el('th', null, 'Last run'), el('th', null, 'Windows'), el('th', null, 'Labels'), el('th', null, 'State'), el('th', null, ''))),
        el('tbody', null, rows)))
      : el('p', { class: 'empty' }, 'No schedules.');
    const pages = Math.max(1, Math.ceil(data.total / size));
    const pager = pagerNode({
      page, pages, total: data.total, truncated: false, size, shown: data.schedules.length,
      onPage: (n) => { const p = new URLSearchParams(params); if (n === 1) p.delete('page'); else p.set('page', String(n)); go('schedules', p); },
      onSize: (n) => { const p = new URLSearchParams(); if (n !== DEFAULT_SIZE) p.set('size', String(n)); go('schedules', p); },
    });
    return el('div', null, el('h2', null, 'Schedules'), table, pager);
  }

  // --- charts ---

  // SERIES is the run ends a chart stacks, bottom to top, with their
  // colour class (tokens in app.css, checked for colour-blind separation).
  const SERIES = ['succeeded', 'failed', 'cancelled', 'discarded'];
  const WINDOWS = [['1h', '1 hour'], ['6h', '6 hours'], ['24h', '24 hours']];
  const SVGNS = 'http://www.w3.org/2000/svg';

  // svg builds an SVG element; attrs are set as attributes (presentation
  // attributes, never a style attribute: the CSP refuses inline styles).
  function svg(tag, attrs, ...children) {
    const n = document.createElementNS(SVGNS, tag);
    for (const [k, v] of Object.entries(attrs || {})) if (v !== undefined && v !== null) n.setAttribute(k, String(v));
    for (const c of children.flat(Infinity)) if (c !== null && c !== undefined) n.append(c instanceof Node ? c : document.createTextNode(String(c)));
    return n;
  }

  // niceMax rounds a maximum up to 1, 2 or 5 times a power of ten.
  function niceMax(v) {
    if (v <= 4) return 4;
    const p = Math.pow(10, Math.floor(Math.log10(v)));
    for (const m of [1, 2, 5, 10]) if (m * p >= v) return m * p;
    return 10 * p;
  }

  function pointTotal(p) { return SERIES.reduce((s, k) => s + p[k], 0); }
  function fmtAt(iso, stepSecs) {
    const d = new Date(iso);
    const hm = pad2(d.getHours()) + ':' + pad2(d.getMinutes());
    return stepSecs >= 3600 ? (d.getMonth() + 1) + '/' + d.getDate() + ' ' + hm : hm;
  }

  // chartNode draws stacked bars of run ends per step with a y grid, a
  // few time labels, a hover readout and a native tooltip per bar.
  function chartNode(m) {
    // The chart is drawn at the page's width rather than stretched, so its
    // text keeps its shape on a phone.
    const W = Math.max(300, main.clientWidth - 32);
    const H = 220;
    const L = 36;
    const B = 18;
    const T = 6;
    const n = m.points.length;
    const max = niceMax(Math.max(0, ...m.points.map(pointTotal)));
    const pw = (W - L) / Math.max(n, 1);
    const gap = pw > 6 ? 2 : 0;
    const y = (v) => T + (H - T - B) * (1 - v / max);
    const readout = el('div', { class: 'readout', 'aria-live': 'polite' }, 'Hover a bar for its counts.');
    const grid = [0, 0.5, 1].map((f) => [
      svg('line', { class: 'grid', x1: L, x2: W, y1: y(max * f), y2: y(max * f) }),
      svg('text', { class: 'axis', x: L - 4, y: y(max * f) + 3, 'text-anchor': 'end' }, Math.round(max * f))]);
    const every = Math.max(1, Math.ceil(n / Math.max(2, Math.floor(W / 120))));
    const bars = m.points.map((p, i) => {
      const x = L + i * pw + gap / 2;
      const w = Math.max(pw - gap, 1);
      let acc = 0;
      const segs = [];
      for (const k of SERIES) {
        if (!p[k]) continue;
        const y0 = y(acc);
        acc += p[k];
        const y1 = y(acc);
        // A 1px surface gap between stacked segments keeps them apart.
        segs.push(svg('rect', { class: 's-' + k, x, y: y1, width: w, height: Math.max(y0 - y1 - (acc < pointTotal(p) ? 1 : 0), 1), rx: Math.min(2, w / 2) }));
      }
      const text = fmtTime(p.at) + '  ' + SERIES.map((k) => k + ' ' + p[k]).join(' · ');
      const hit = svg('rect', { class: 'hit', x: L + i * pw, y: T, width: pw, height: H - T - B, 'data-i': i }, svg('title', null, text));
      hit.addEventListener('mouseenter', () => { readout.textContent = text; });
      const label = i % every === 0 ? svg('text', { class: 'axis', x: x + w / 2, y: H - 4, 'text-anchor': 'middle' }, fmtAt(p.at, m.step_seconds)) : null;
      return [segs, label, hit];
    });
    const chart = svg('svg', { class: 'chart', viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'aria-label': 'Run ends per ' + fmtDur(m.step_seconds) + ', stacked by status' }, grid, bars);
    return [readout, chart];
  }

  async function viewCharts(params) {
    if (!hasMetrics) return el('p', { class: 'empty' }, 'No metrics source is wired: build the handler\'s Admin with admin.WithMetrics (admin.NewMemoryMetrics is the in-process default).');
    const win = WINDOWS.some((w) => w[0] === params.get('window')) ? params.get('window') : '1h';
    const q = new URLSearchParams({ window: win });
    if (params.get('workflow')) q.set('workflow', params.get('workflow'));
    const m = await api('GET', 'metrics?' + q.toString());
    const set = (k, v) => { const p = new URLSearchParams(params); if (v) p.set(k, v); else p.delete(k); go('charts', p); };
    const tools = el('div', { class: 'toolbar' },
      el('span', { class: 'chips', role: 'group', 'aria-label': 'Window' }, WINDOWS.map(([w, label]) => el('button', {
        type: 'button', class: 'btn' + (w === win ? ' on' : ''), 'aria-pressed': w === win ? 'true' : 'false', onclick: () => set('window', w === '1h' ? '' : w),
      }, label))),
      el('input', { type: 'text', class: 'mono filter w-md', placeholder: 'workflow', 'aria-label': 'workflow', value: params.get('workflow') || '',
        onkeydown: (e) => { if (e.key === 'Enter') set('workflow', e.target.value.trim()); }, onchange: (e) => set('workflow', e.target.value.trim()) }),
      el('span', { class: 'grow' }),
      el('span', { class: 'muted small' }, 'per ' + fmtDur(m.step_seconds) + (m.since ? ' · data since ' + fmtTime(m.since) : '')));
    const totals = {};
    for (const k of SERIES) totals[k] = m.points.reduce((s, p) => s + p[k], 0);
    const all = SERIES.reduce((s, k) => s + totals[k], 0);
    const failRate = all ? Math.round((totals.failed / all) * 1000) / 10 : 0;
    const tiles = el('div', { class: 'tiles' },
      el('div', { class: 'tile' }, el('div', { class: 'v' }, all), el('div', { class: 'k' }, 'runs ended')),
      SERIES.map((k) => el('div', { class: 'tile' }, el('div', { class: 'v' }, totals[k]), el('div', { class: 'k' }, k))),
      el('div', { class: 'tile' }, el('div', { class: 'v' }, failRate + '%'), el('div', { class: 'k' }, 'failed share')));
    const legend = el('div', { class: 'legend', 'aria-hidden': 'true' }, SERIES.map((k) => el('span', { class: 'key' }, el('span', { class: 'sw c-' + k }), k)));
    const rows = m.points.filter((p) => pointTotal(p) > 0).map((p) => el('tr', null, timeCell(p.at), SERIES.map((k) => el('td', { class: 'num' }, p[k]))));
    const table = el('details', { class: 'out' }, el('summary', { class: 'muted small' }, 'Table (' + rows.length + ' non-empty steps)'),
      rows.length ? el('div', { class: 'tablewrap' }, el('table', null,
        el('thead', null, el('tr', null, el('th', { class: 'num' }, 'At'), SERIES.map((k) => el('th', { class: 'num' }, k)))),
        el('tbody', null, rows))) : el('p', { class: 'empty' }, 'No run ended in this window.'));
    return el('div', null, tools, tiles, el('h2', null, 'Run ends'), legend, el('div', { class: 'chartwrap' }, chartNode(m)), table);
  }

  // --- system: health and nodes ---

  async function viewSystem() {
    const [h, n] = await Promise.all([
      supported('health') ? api('GET', 'health').catch((e) => ({ error: e.message })) : Promise.resolve(null),
      supported('nodes') ? api('GET', 'nodes').catch((e) => ({ error: e.message })) : Promise.resolve(null),
    ]);
    const ok = (b) => el('span', { class: 'badge ' + (b ? 's-succeeded' : 's-failed') }, b ? 'yes' : 'no');
    let health = null;
    if (h && h.error) health = el('p', { class: 'muted small pad' }, 'Health: ' + h.error);
    else if (h) {
      health = el('div', { class: 'section' }, el('h2', null, 'Health'),
        el('dl', { class: 'kv' }, el('dt', null, 'Live'), el('dd', null, ok(h.live)), el('dt', null, 'Ready'), el('dd', null, ok(h.ready))),
        h.checks.length ? el('div', { class: 'tablewrap' }, el('table', null,
          el('thead', null, el('tr', null, el('th', null, 'Check'), el('th', null, 'OK'), el('th', null, 'Detail'))),
          el('tbody', null, h.checks.map((c) => el('tr', null, el('td', { class: 'mono' }, c.name), el('td', null, ok(c.ok)), el('td', null, c.detail || '')))))) : null);
    }
    let nodes;
    if (!n) nodes = el('p', { class: 'muted small pad' }, 'This driver does not list nodes.');
    else if (n.error) nodes = el('p', { class: 'muted small pad' }, 'Nodes: ' + n.error);
    else {
      nodes = el('div', { class: 'section' }, el('h2', null, 'Nodes (' + n.nodes.length + ')'),
        n.nodes.length ? el('div', { class: 'tablewrap' }, el('table', null,
          el('thead', null, el('tr', null, el('th', null, 'Node'), el('th', null, 'Mode'), el('th', null, 'Queues'), el('th', { class: 'num' }, 'Workers'), el('th', { class: 'num' }, 'Running'), el('th', { class: 'num' }, 'Started'), el('th', { class: 'num' }, 'Last seen'))),
          el('tbody', null, n.nodes.map((x) => el('tr', null,
            el('td', { class: 'mono' }, x.id), el('td', null, x.mode), el('td', { class: 'mono' }, x.queues.length ? x.queues.join(', ') : 'all'),
            el('td', { class: 'num' }, x.workers), el('td', { class: 'num' }, x.running), timeCell(x.started_at), timeCell(x.last_seen)))))) : el('p', { class: 'empty' }, 'No node is online.'));
    }
    return el('div', null, health, nodes);
  }

  // --- theme, keys, start ---

  function applyTheme(t) {
    if (t === 'light' || t === 'dark') document.documentElement.dataset.theme = t;
    else delete document.documentElement.dataset.theme;
  }
  function storedTheme() { try { return localStorage.getItem(THEME_KEY); } catch (_) { return null; } }
  function toggleTheme() {
    const dark = document.documentElement.dataset.theme
      ? document.documentElement.dataset.theme === 'dark'
      : matchMedia('(prefers-color-scheme: dark)').matches;
    const next = dark ? 'light' : 'dark';
    applyTheme(next);
    try { localStorage.setItem(THEME_KEY, next); } catch (_) { /* storage blocked: theme lasts this visit */ }
  }

  function pageStep(delta) {
    const b = document.querySelector('.pager button[title^="' + (delta < 0 ? 'Previous' : 'Next') + '"]');
    if (b && !b.disabled) b.click();
  }

  document.addEventListener('keydown', (e) => {
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    const typing = e.target instanceof HTMLInputElement || e.target instanceof HTMLSelectElement;
    if (e.key === 'Escape') {
      if (typing) { e.target.blur(); return; }
      const r = parseHash();
      if (['runs', 'failures', 'discarded'].includes(r.view) && [...r.params.keys()].length) go(r.view, new URLSearchParams());
      return;
    }
    if (typing) return;
    if (e.key === '/') {
      const f = document.querySelector('input.filter, .toolbar input');
      if (f) { e.preventDefault(); f.focus(); }
    } else if (e.key === 'ArrowLeft') pageStep(-1);
    else if (e.key === 'ArrowRight') pageStep(1);
    else if (e.key === 'r') { counts.clear(); render(); }
    else if (e.key === 't') toggleTheme();
    else if (e.key >= '1' && e.key <= String(VIEWS.length)) location.hash = '#/' + VIEWS[Number(e.key) - 1];
  });

  document.getElementById('refresh').addEventListener('click', () => { counts.clear(); render(); });
  document.getElementById('theme').addEventListener('click', toggleTheme);
  window.addEventListener('hashchange', render);
  applyTheme(storedTheme());

  (async function start() {
    try {
      const c = await api('GET', 'capabilities');
      caps = {};
      for (const o of c.operations) caps[o.operation] = o.supported;
      hasOutput = c.output === true;
      hasMetrics = c.metrics === true;
      capNames = new Set(c.capabilities.split('|'));
      const off = c.operations.filter((o) => !o.supported).map((o) => o.operation);
      document.getElementById('caps').textContent = off.length ? 'not supported here: ' + off.join(', ') : '';
      for (const a of document.querySelectorAll('#nav a')) {
        const op = { queues: 'queues', runs: 'runs', failures: 'failures', discarded: 'discarded', schedules: 'schedules', system: 'health' }[a.dataset.view];
        if (caps[op] === false || (a.dataset.view === 'charts' && !hasMetrics)) a.hidden = true;
      }
    } catch (_) {
      caps = null; // not allowed to read capabilities: show everything and let the API refuse
    }
    if (!location.hash) location.hash = '#/queues';
    else render();
  })();
})();
