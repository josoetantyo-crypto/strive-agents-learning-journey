// API Agents — buat, baca, list, hapus agent (data di Upstash Redis)
import crypto from 'node:crypto';
import { redis, K, parse, hashObj, validId, cors, isAdmin, ensureMigrated, PENDING_MSG } from './_db.js';

export default async function handler(req, res) {
  cors(res, 'GET,POST,DELETE,OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();

  try {
    const r = redis();

    if (req.method === 'POST') {
      const { name, wa } = req.body || {};
      const cleanName = String(name || '').trim().slice(0, 60);
      let cleanWa = String(wa || '').replace(/[^0-9]/g, '');
      if (cleanWa.startsWith('0')) cleanWa = '62' + cleanWa.slice(1);
      if (!cleanName) return res.status(400).json({ error: 'Nama agent wajib diisi' });
      if (cleanWa.length < 9) return res.status(400).json({ error: 'No WA tidak valid' });

      const now = new Date().toISOString();
      const id = crypto.randomUUID().replace(/-/g, '').slice(0, 10);
      const agent = { id, name: cleanName, wa: cleanWa, createdAt: now };
      if (!(await r.set(K.agent(id), JSON.stringify(agent), { nx: true }))) {
        return res.status(409).json({ error: 'Coba generate lagi' });
      }
      const p = r.pipeline();
      p.set(K.agentData(id), JSON.stringify({ p1: [], state: null, updatedAt: now }));
      p.sadd(K.agents, id);
      p.hset(K.p1stats, { [id]: JSON.stringify({ t: 0, m: 0, u: now }) });
      await p.exec();
      return res.status(200).json({ agent: { ...agent, updatedAt: now, p1: [] } });
    }

    if (req.method === 'GET') {
      const { id } = req.query;
      if (id) {
        const aid = String(id);
        if (!validId(aid)) return res.status(404).json({ error: 'Agent tidak ditemukan' });
        let [metaS, dataS] = await r.mget(K.agent(aid), K.agentData(aid));
        if (!metaS) {
          // Belum ada di database baru — mungkin agent lama yang datanya belum selesai dipindahkan
          const mig = await ensureMigrated(r);
          if (!mig.done) return res.status(503).json({ error: PENDING_MSG, pending: true });
          [metaS, dataS] = await r.mget(K.agent(aid), K.agentData(aid));
          if (!metaS) return res.status(404).json({ error: 'Agent tidak ditemukan' });
        }
        const meta = parse(metaS);
        const data = parse(dataS) || {};
        return res.status(200).json({
          agent: {
            ...meta,
            updatedAt: data.updatedAt || meta.createdAt,
            p1: Array.isArray(data.p1) ? data.p1 : [],
            state: data.state || null,
          },
        });
      }

      // List semua agent + ringkasan progress P1 — khusus admin
      if (!isAdmin(req)) return res.status(401).json({ error: 'Password admin salah' });
      const mig = await ensureMigrated(r, { force: req.query.migrate === '1' });
      const ids = await r.smembers(K.agents);
      let agents = [];
      if (ids.length) {
        const p = r.pipeline();
        p.mget(...ids.map(K.agent));
        p.hgetall(K.p1stats);
        const [metas, statsRaw] = await p.exec();
        const stats = hashObj(statsRaw);
        agents = metas
          .map((s) => parse(s))
          .filter(Boolean)
          .map((a) => {
            const st = parse(stats[a.id]) || {};
            return {
              id: a.id,
              name: a.name,
              wa: a.wa,
              createdAt: a.createdAt,
              updatedAt: st.u || a.createdAt,
              p1Total: st.t || 0,
              p1Met: st.m || 0,
            };
          });
      }
      agents.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
      return res.status(200).json({ agents, migrated: mig.done });
    }

    if (req.method === 'DELETE') {
      if (!isAdmin(req)) return res.status(401).json({ error: 'Password admin salah' });
      const aid = String(req.query.id || '');
      if (!validId(aid)) return res.status(400).json({ error: 'id wajib' });
      const p = r.pipeline();
      p.del(K.agent(aid), K.agentData(aid));
      p.srem(K.agents, aid);
      p.hdel(K.p1stats, aid);
      await p.exec();
      return res.status(200).json({ ok: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    return res.status(500).json({ error: e.message || 'Server error' });
  }
}
