export const PUT_STEPS = [
  {
    id: 'put',
    from: 'Client',
    to: 'S3',
    label: 'PUT /bucket/key',
    where: 'S3 Gateway',
    detail: '解析 S3 语义，进入 putToFiler；按 MaxMB 切 chunk。',
  },
  {
    id: 'assign-req',
    from: 'S3',
    to: 'Filer',
    label: 'AssignVolume(path, size…)',
    where: 'Filer',
    detail: '解析 filer.conf（collection / replication / TTL），再向 Master 要写位点。',
  },
  {
    id: 'assign',
    from: 'Filer',
    to: 'Master',
    label: 'gRPC Assign',
    where: 'Master',
    detail: 'PickForWrite 选可写 volume；生成 fid = volumeId + needleId + cookie。',
  },
  {
    id: 'assign-resp',
    from: 'Master',
    to: 'Filer',
    label: '{ fid, URL, replicas, Auth }',
    where: 'Master → Filer',
    detail: '把写地址交回；Filer/S3 据此直连 Volume。',
  },
  {
    id: 'needle',
    from: 'S3',
    to: 'Volume',
    label: 'POST /{fid}（可含副本）',
    where: 'Volume',
    detail: '追加写入 Needle；真实字节落在 volume .dat。每个 chunk 重复 Assign→写 Needle。',
  },
  {
    id: 'entry',
    from: 'S3',
    to: 'Filer',
    label: 'CreateEntry(path, Chunks[])',
    where: 'Filer',
    detail: '只存索引 Entry{ FileId, Offset, Size… }，不存对象本体。',
  },
];

const COLORS = {
  Client: '#3d5a4c',
  S3: '#1f6f8b',
  Filer: '#b45f06',
  Master: '#6b2d5c',
  Volume: '#2f5d3a',
};

const NODES = ['Client', 'S3', 'Filer', 'Master', 'Volume'];

const btn = {
  border: '1px solid #c9d0da',
  background: '#fff',
  borderRadius: 8,
  padding: '6px 12px',
  cursor: 'pointer',
  fontSize: 13,
};

export const PutFlowPlayer = () => {
  const [i, setI] = useState(0);
  const [playing, setPlaying] = useState(true);
  const step = PUT_STEPS[i];

  useEffect(() => {
    if (!playing) return undefined;
    const t = setInterval(() => {
      setI((prev) => (prev >= PUT_STEPS.length - 1 ? 0 : prev + 1));
    }, 1600);
    return () => clearInterval(t);
  }, [playing]);

  return (
    <div
      className="not-prose"
      style={{
        border: '1px solid #d8dde6',
        borderRadius: 12,
        padding: 16,
        background: 'linear-gradient(165deg, #f7f9fc 0%, #eef3f0 100%)',
        maxWidth: 720,
      }}
    >
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 14 }}>
        {NODES.map((n) => {
          const on = n === step.from || n === step.to;
          return (
            <div
              key={n}
              style={{
                flex: '1 1 100px',
                minWidth: 88,
                textAlign: 'center',
                padding: '10px 8px',
                borderRadius: 10,
                border: '2px solid ' + (on ? COLORS[n] : '#c9d0da'),
                background: on ? COLORS[n] + '22' : '#fff',
                color: on ? COLORS[n] : '#5a6573',
                fontWeight: on ? 700 : 500,
              }}
            >
              {n}
            </div>
          );
        })}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12, minHeight: 36 }}>
        <span style={{ fontWeight: 700, color: COLORS[step.from] }}>{step.from}</span>
        <div
          style={{
            flex: 1,
            height: 8,
            borderRadius: 99,
            background: 'linear-gradient(90deg, ' + COLORS[step.from] + ', ' + COLORS[step.to] + ')',
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
              background: '#fff',
              border: '2px solid ' + COLORS[step.to],
              animation: 'mintPutDot 1.2s linear infinite',
            }}
          />
        </div>
        <span style={{ fontWeight: 700, color: COLORS[step.to] }}>{step.to}</span>
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
          步骤 {i + 1}/{PUT_STEPS.length} · 发生在 <strong style={{ color: '#111' }}>{step.where}</strong>
        </div>
        <div style={{ fontFamily: 'ui-monospace, monospace', fontSize: 14, marginBottom: 6 }}>{step.label}</div>
        <div style={{ fontSize: 14, lineHeight: 1.5, color: '#374151' }}>{step.detail}</div>
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
            setI((v) => Math.min(PUT_STEPS.length - 1, v + 1));
          }}
        >
          下一步
        </button>
      </div>
    </div>
  );
};
