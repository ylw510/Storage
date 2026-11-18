export const DATA_FLOW_STEPS = [
  {
    id: 'ingest',
    kind: 'data',
    title: '对象进入 Filer 写路径',
    detail: 'S3/客户端把对象交给 Filer 侧写流程；按 MaxMB 切成 chunk（示意 20MiB → 多段）。',
    packet: '20MiB object',
    from: 'edge',
    to: 'filer',
    filer: '切 chunk',
    master: '空闲',
    volume: '空闲',
    hi: { filer: true },
  },
  {
    id: 'ask',
    kind: 'ctrl',
    title: 'Filer → Master：申请写位点',
    detail: '控制面 AssignVolume。Filer 带 path/size/collection，问 Master「写到哪」。',
    packet: 'AssignVolume',
    from: 'filer',
    to: 'master',
    filer: '等待 fid',
    master: '选 volume',
    volume: '空闲',
    hi: { filer: true, master: true },
  },
  {
    id: 'fid',
    kind: 'ctrl',
    title: 'Master → Filer：返回 fid + URL',
    detail: 'PickForWrite 生成 fid，返回 volume URL / replicas / Auth。仍无对象字节。',
    packet: '{fid, URL}',
    from: 'master',
    to: 'filer',
    filer: '拿到写地址',
    master: '已记账',
    volume: '待写入',
    hi: { filer: true, master: true },
  },
  {
    id: 'write',
    kind: 'data',
    title: '数据面：字节直写 Volume',
    detail: '真实 payload 不经 Master。对 Volume POST /{fid}，Needle 追加进 .dat。',
    packet: 'chunk → Needle',
    from: 'filer',
    to: 'volume',
    filer: '记临时 Chunk',
    master: '不碰数据',
    volume: 'Needle 落盘',
    hi: { filer: true, volume: true },
  },
  {
    id: 'loop',
    kind: 'data',
    title: '下一 chunk：再 Assign → 再写',
    detail: '每个 chunk 重复控制面+数据面；可能落到不同 volume。',
    packet: 'chunk1…n',
    from: 'filer',
    to: 'volume',
    filer: 'Chunks[] 变长',
    master: '多次 Assign',
    volume: '多个 Needle',
    hi: { filer: true, master: true, volume: true },
  },
  {
    id: 'meta',
    kind: 'meta',
    title: '元数据：CreateEntry 只存索引',
    detail: 'Filer 持久化 Entry{path, Chunks[]}。读时靠 Chunks 找回 Needle。',
    packet: 'Entry+Chunks',
    from: 'filer',
    to: 'filer',
    filer: '索引已提交',
    master: '拓扑照旧',
    volume: '数据已在',
    hi: { filer: true },
  },
];

const KIND = {
  data: { fill: '#1f6f8b', name: '数据面' },
  ctrl: { fill: '#6b2d5c', name: '控制面' },
  meta: { fill: '#b45f06', name: '元数据' },
};

const btn = {
  border: '1px solid #c9d0da',
  background: '#fff',
  borderRadius: 8,
  padding: '6px 12px',
  cursor: 'pointer',
  fontSize: 13,
};

const box = (on, stroke, fill) => ({
  flex: '1 1 120px',
  minWidth: 110,
  borderRadius: 12,
  padding: '12px 10px',
  border: '2px solid ' + (on ? stroke : '#cbd5e1'),
  background: on ? fill : '#fff',
  color: on ? stroke : '#64748b',
  transition: 'all .2s ease',
});

export const DataPlaneFlow = () => {
  const [i, setI] = useState(0);
  const [playing, setPlaying] = useState(true);
  const step = DATA_FLOW_STEPS[i];
  const kind = KIND[step.kind];

  useEffect(() => {
    if (!playing) return undefined;
    const t = setInterval(() => {
      setI((prev) => (prev >= DATA_FLOW_STEPS.length - 1 ? 0 : prev + 1));
    }, 1800);
    return () => clearInterval(t);
  }, [playing]);

  return (
    <div
      className="not-prose"
      style={{
        border: '1px solid #d8dde6',
        borderRadius: 12,
        padding: 16,
        background: 'linear-gradient(165deg, #f8fafc 0%, #eef2f7 55%, #e8f0ec 100%)',
        maxWidth: 720,
      }}
    >
      <div style={{ display: 'flex', gap: 12, marginBottom: 8, fontSize: 12, color: '#4b5563' }}>
        <span>● 数据面</span>
        <span style={{ color: '#6b2d5c' }}>● 控制面</span>
        <span style={{ color: '#b45f06' }}>● 元数据</span>
      </div>

      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
        <div style={box(!!step.hi.filer, '#b45f06', '#fff4e8')}>
          <div style={{ fontWeight: 700, marginBottom: 4 }}>Filer</div>
          <div style={{ fontSize: 12 }}>{step.filer}</div>
        </div>
        <div style={box(!!step.hi.master, '#6b2d5c', '#f5e9f1')}>
          <div style={{ fontWeight: 700, marginBottom: 4 }}>Master</div>
          <div style={{ fontSize: 12 }}>{step.master}</div>
        </div>
        <div style={box(!!step.hi.volume, '#2f5d3a', '#e7f2ea')}>
          <div style={{ fontWeight: 700, marginBottom: 4 }}>Volume</div>
          <div style={{ fontSize: 12 }}>{step.volume}</div>
        </div>
      </div>

      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          marginBottom: 12,
          padding: '10px 12px',
          borderRadius: 10,
          background: '#fff',
          border: '1px solid #e2e8f0',
        }}
      >
        <strong style={{ color: kind.fill }}>{step.from}</strong>
        <div
          style={{
            flex: 1,
            height: 8,
            borderRadius: 99,
            background: kind.fill + '33',
            position: 'relative',
            overflow: 'hidden',
          }}
        >
          <div
            key={step.id}
            style={{
              position: 'absolute',
              top: -2,
              width: 12,
              height: 12,
              borderRadius: '50%',
              background: kind.fill,
              animation: 'mintPutDot 1.2s linear infinite',
            }}
          />
        </div>
        <strong style={{ color: kind.fill }}>{step.to}</strong>
        <span
          style={{
            marginLeft: 8,
            fontSize: 12,
            fontFamily: 'ui-monospace, monospace',
            color: kind.fill,
            fontWeight: 700,
          }}
        >
          {step.packet}
        </span>
      </div>

      <div
        style={{
          background: '#fff',
          borderRadius: 10,
          padding: '12px 14px',
          border: '1px solid #e2e8f0',
          marginBottom: 12,
        }}
      >
        <div style={{ fontSize: 12, color: '#6b7280', marginBottom: 4 }}>
          步骤 {i + 1}/{DATA_FLOW_STEPS.length} · <strong style={{ color: kind.fill }}>{kind.name}</strong>
        </div>
        <div style={{ fontSize: 15, fontWeight: 700, marginBottom: 6 }}>{step.title}</div>
        <div style={{ fontSize: 14, lineHeight: 1.55, color: '#374151' }}>{step.detail}</div>
      </div>

      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
        <button
          type="button"
          style={btn}
          onClick={() => {
            setPlaying(false);
            setI(0);
          }}
        >
          重置
        </button>
        <button
          type="button"
          style={btn}
          onClick={() => {
            setPlaying(false);
            setI((v) => Math.max(0, v - 1));
          }}
        >
          上一步
        </button>
        <button
          type="button"
          style={{
            border: '1px solid #1f6f8b',
            background: '#1f6f8b',
            color: '#fff',
            borderRadius: 8,
            padding: '6px 12px',
            cursor: 'pointer',
            fontSize: 13,
          }}
          onClick={() => setPlaying((p) => !p)}
        >
          {playing ? '暂停' : '自动播放'}
        </button>
        <button
          type="button"
          style={btn}
          onClick={() => {
            setPlaying(false);
            setI((v) => Math.min(DATA_FLOW_STEPS.length - 1, v + 1));
          }}
        >
          下一步
        </button>
      </div>
    </div>
  );
};
