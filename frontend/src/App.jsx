import React, { useState, useEffect } from 'react';
import ReactFlow, { Background, Controls, MiniMap, useReactFlow, ReactFlowProvider } from 'reactflow';
import 'reactflow/dist/style.css';
import axios from 'axios';
import dagre from 'dagre'; 

const getLayoutedElements = (nodes, edges, direction = 'LR') => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: direction, ranksep: 350, nodesep: 150 });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: 350, height: 120 });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  nodes.forEach((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    node.targetPosition = 'left';
    node.sourcePosition = 'right';
    node.position = {
      x: nodeWithPosition.x - 350 / 2,
      y: nodeWithPosition.y - 120 / 2,
    };
    return node;
  });

  return { layoutedNodes: nodes, layoutedEdges: edges };
};

function FlowViz() {
  const [step, setStep] = useState('UPLOAD'); 
  const [dagMetadata, setDagMetadata] = useState({});
  const [selectedDags, setSelectedDags] = useState([]);
  const [focusedDagInfo, setFocusedDagInfo] = useState(null); 
  
  const [listSearchInput, setListSearchInput] = useState("");
  
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchInput, setSearchInput] = useState(""); 
  const [searchTerm, setSearchTerm] = useState(""); 
  const [matchingIds, setMatchingIds] = useState([]);
  const [currentIndex, setCurrentIndex] = useState(-1);

  // 💡 [추가] 서버 경로 초기값 세팅 및 탭 상태 (기본값 설정 완료)
  const [serverLocalPath, setServerLocalPath] = useState('D:\\세일링스톤\\32. FLOWVIZ\\dags');
  const [uploadMethod, setUploadMethod] = useState('SERVER'); 

  const { setCenter, getNodes, fitView } = useReactFlow();

  useEffect(() => {
    const handleGlobalKeyDown = (e) => {
      if (e.key === 'Escape') {
        setSearchInput("");
        setSearchTerm("");
        setMatchingIds([]);
        setCurrentIndex(-1);
        setListSearchInput("");
      }
    };
    window.addEventListener('keydown', handleGlobalKeyDown);
    return () => window.removeEventListener('keydown', handleGlobalKeyDown);
  }, []);

  const handleFileUpload = async (event) => {
    const files = event.target.files;
    if (!files || files.length === 0) return;

    setLoading(true);
    const formData = new FormData();
    Array.from(files).forEach((file) => formData.append("files", file));

    try {
      const response = await axios.post("http://localhost:8000/api/upload", formData, {
        headers: { "Content-Type": "multipart/form-data" },
      });
      setDagMetadata(response.data.metadata);
      setStep('SELECT'); 
    } catch (error) {
      console.error("업로드 실패:", error);
      alert("DAG 분석 중 오류가 발생했습니다.");
    } finally {
      setLoading(false);
    }
  };

  // 💡 [추가] 서버 로컬 경로에서 파일 읽어오기
  const handleServerLoad = async () => {
    if(!serverLocalPath.trim()) {
      alert("서버 내 폴더 경로를 입력해주세요.");
      return;
    }
    setLoading(true);
    try {
      const response = await axios.post("http://localhost:8000/api/load-local", {
        directory: serverLocalPath
      });
      setDagMetadata(response.data.metadata);
      setStep('SELECT'); 
    } catch (error) {
      console.error("불러오기 실패:", error);
      const errMsg = error.response?.data?.detail || "경로를 읽는 중 오류가 발생했습니다.";
      alert(errMsg);
    } finally {
      setLoading(false);
    }
  };

  const toggleDagSelection = (dagId) => {
    setSelectedDags(prev => 
      prev.includes(dagId) ? prev.filter(id => id !== dagId) : [...prev, dagId]
    );
  };

  const selectAllRelatedDags = (dagId) => {
    const info = dagMetadata[dagId];
    const related = [dagId, ...info.upstream, ...info.downstream];
    setSelectedDags(prev => Array.from(new Set([...prev, ...related])));
  };

  const selectUpstreamDags = (dagId) => {
    const info = dagMetadata[dagId];
    const related = [dagId, ...info.upstream];
    setSelectedDags(prev => Array.from(new Set([...prev, ...related])));
  };

  const selectDownstreamDags = (dagId) => {
    const info = dagMetadata[dagId];
    const related = [dagId, ...info.downstream];
    setSelectedDags(prev => Array.from(new Set([...prev, ...related])));
  };

  const deselectAllDags = () => {
    setSelectedDags([]);
  };

  const generateVisualization = async () => {
    if (selectedDags.length === 0) {
      alert("시각화할 DAG를 최소 1개 이상 선택해주세요.");
      return;
    }
    setLoading(true);
    try {
      const response = await axios.post("http://localhost:8000/api/visualize", {
        selected_dags: selectedDags
      });

      const { layoutedNodes, layoutedEdges } = getLayoutedElements(
        response.data.nodes,
        response.data.edges
      );

      setNodes(layoutedNodes);
      setEdges(layoutedEdges);
      setStep('VISUALIZE'); 
      
      setTimeout(() => fitView({ duration: 800, padding: 0.2 }), 100);
    } catch (error) {
      console.error("시각화 생성 실패:", error);
      alert("관계도 생성 중 오류가 발생했습니다.");
    } finally {
      setLoading(false);
    }
  };

  const executeSearch = () => {
    const term = searchInput.trim().toLowerCase();
    if (!term) {
      setSearchTerm(""); setMatchingIds([]); setCurrentIndex(-1); return;
    }
    const currentNodes = getNodes();
    const matchedNodes = currentNodes.filter(node => node.id.toLowerCase().includes(term));
    const matchedIds = matchedNodes.map(n => n.id);

    if (matchedIds.length > 0) {
      let nextIndex = 0;
      if (searchTerm === term && matchingIds.length === matchedIds.length) {
        nextIndex = (currentIndex + 1) % matchedIds.length;
      }
      setSearchTerm(term); setMatchingIds(matchedIds); setCurrentIndex(nextIndex);

      const targetNode = matchedNodes[nextIndex];
      if (targetNode) {
        setTimeout(() => setCenter(targetNode.position.x + 150, targetNode.position.y + 50, { zoom: 1.2, duration: 800 }), 50);
      }
    } else {
      setSearchTerm(term); setMatchingIds([]); setCurrentIndex(-1); alert("검색된 대상이 없습니다.");
    }
  };

  useEffect(() => {
    if (step !== 'VISUALIZE') return;
    setNodes((currentNodes) =>
      currentNodes.map((node) => {
        const isMatch = matchingIds.includes(node.id);
        const isFocused = isMatch && matchingIds[currentIndex] === node.id;

        return {
          ...node,
          className: 'nodrag nopan', 
          style: {
            ...node.style,
            backgroundColor: isMatch ? '#64ffda' : (node.id.includes('MAIN') ? '#112240' : '#175a76'),
            color: isMatch ? '#020c1b' : (node.id.includes('MAIN') ? '#64ffda' : '#ffffff'),
            border: isFocused ? '4px solid #ffb86c' : (isMatch ? '3px solid #fff' : '1px solid #0d3848'),
            boxShadow: isFocused ? '0 0 30px rgba(255, 184, 108, 1)' : (isMatch ? '0 0 15px rgba(100, 255, 218, 0.7)' : 'none'),
            zIndex: isMatch ? 1000 : 0, 
            userSelect: 'text', WebkitUserSelect: 'text', cursor: 'text', pointerEvents: 'all', 
          }
        };
      })
    );
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [matchingIds, currentIndex, step]);

  const filteredDagKeys = Object.keys(dagMetadata).filter(dagId => 
    dagId.toLowerCase().includes(listSearchInput.toLowerCase())
  );

  return (
    <div style={{ height: '100vh', width: '100vw', display: 'flex', flexDirection: 'column', backgroundColor: '#020c1b', color: '#ccd6f6', overflow: 'hidden' }}>
      
      <div style={{ padding: '20px', backgroundColor: '#0a192f', borderBottom: '1px solid #233554', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexShrink: 0 }}>
        <h2 style={{ color: '#64ffda', margin: 0, fontFamily: 'sans-serif' }}>FlowViz DAG Parser</h2>
        
        {step === 'SELECT' && (
          <button onClick={generateVisualization} style={{ padding: '10px 20px', backgroundColor: '#64ffda', color: '#020c1b', fontWeight: 'bold', border: 'none', borderRadius: '6px', cursor: 'pointer' }}>
            선택된 범위 시각화 그리기
          </button>
        )}

        {step === 'VISUALIZE' && (
          <div style={{ display: 'flex', gap: '20px', alignItems: 'center' }}>
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
              <input type="text" placeholder="캔버스 노드 검색 (ESC: 취소)" value={searchInput} onChange={(e) => setSearchInput(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && executeSearch()}
                style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #64ffda', backgroundColor: '#112240', color: '#fff', outline: 'none', width: '220px' }} />
              <button onClick={executeSearch} style={{ padding: '8px 16px', borderRadius: '6px', border: 'none', backgroundColor: '#64ffda', color: '#020c1b', fontWeight: 'bold', cursor: 'pointer' }}>검색</button>
              {matchingIds.length > 0 && <span style={{ fontSize: '14px' }}>{currentIndex + 1} / {matchingIds.length}</span>}
            </div>
            <button onClick={() => setStep('SELECT')} style={{ padding: '8px 16px', backgroundColor: '#233554', color: '#64ffda', border: '1px solid #64ffda', borderRadius: '6px', cursor: 'pointer' }}>
              ← 다시 선택하기
            </button>
            <button onClick={() => {setStep('UPLOAD'); setDagMetadata({}); setSelectedDags([]);}} style={{ padding: '8px 16px', backgroundColor: '#ff6b6b', color: '#fff', border: 'none', borderRadius: '6px', cursor: 'pointer' }}>
              처음으로
            </button>
          </div>
        )}
      </div>

      <div style={{ flex: 1, position: 'relative', display: 'flex', overflow: 'hidden' }}>
        {loading && <div style={{ position: 'absolute', top: '50%', left: '50%', color: '#64ffda', zIndex: 100, transform: 'translate(-50%, -50%)', fontWeight: 'bold', fontSize: '18px', backgroundColor: 'rgba(2,12,27,0.8)', padding: '20px', borderRadius: '10px' }}>처리 중...</div>}

        {/* 💡 [수정됨] 시작 화면: 폴더 불러오기 vs 로컬 업로드 */}
        {step === 'UPLOAD' && (
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
            <div style={{ backgroundColor: '#112240', padding: '40px', borderRadius: '12px', width: '550px', boxShadow: '0 10px 30px -10px rgba(2,12,27,0.7)' }}>
              
              <div style={{ display: 'flex', marginBottom: '30px', borderBottom: '2px solid #233554' }}>
                <button onClick={() => setUploadMethod('SERVER')} style={{ flex: 1, padding: '15px', background: 'none', border: 'none', color: uploadMethod === 'SERVER' ? '#64ffda' : '#8892b0', borderBottom: uploadMethod === 'SERVER' ? '2px solid #64ffda' : 'none', fontWeight: 'bold', fontSize: '16px', cursor: 'pointer', marginBottom: '-2px' }}>서버 경로에서 불러오기</button>
                <button onClick={() => setUploadMethod('LOCAL')} style={{ flex: 1, padding: '15px', background: 'none', border: 'none', color: uploadMethod === 'LOCAL' ? '#64ffda' : '#8892b0', borderBottom: uploadMethod === 'LOCAL' ? '2px solid #64ffda' : 'none', fontWeight: 'bold', fontSize: '16px', cursor: 'pointer', marginBottom: '-2px' }}>PC에서 업로드</button>
              </div>

              {uploadMethod === 'SERVER' ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '20px', padding: '20px 0' }}>
                  <p style={{ margin: 0, color: '#8892b0', fontSize: '14px', lineHeight: '1.5' }}>
                    백엔드 서버 내의 파이썬(.py) 파일이 모여있는 폴더(디렉토리)의 절대 경로를 입력하세요.
                  </p>
                  <div>
                    <label style={{ fontSize: '12px', color: '#64ffda', display: 'block', marginBottom: '8px' }}>Target Directory Path</label>
                    <input 
                      type="text" 
                      value={serverLocalPath} 
                      onChange={(e) => setServerLocalPath(e.target.value)} 
                      placeholder="예: D:\세일링스톤\32. FLOWVIZ\dags" 
                      style={{ padding: '12px', borderRadius: '4px', border: '1px solid #233554', backgroundColor: '#0a192f', color: '#fff', outline: 'none', width: '100%', boxSizing: 'border-box', fontSize: '15px' }} 
                    />
                  </div>
                  
                  <button onClick={handleServerLoad} style={{ marginTop: '10px', padding: '14px', backgroundColor: '#64ffda', color: '#020c1b', border: 'none', borderRadius: '6px', fontWeight: 'bold', fontSize: '16px', cursor: 'pointer' }}>
                    지정된 폴더에서 불러오기
                  </button>
                </div>
              ) : (
                <div style={{ textAlign: 'center', padding: '40px 0' }}>
                  <p style={{ marginBottom: '20px', color: '#ccd6f6' }}>개인 PC에 있는 DAG 스크립트 파일들을 선택해주세요.</p>
                  <label style={{ display: 'inline-block', padding: '12px 24px', backgroundColor: '#64ffda', color: '#020c1b', fontWeight: 'bold', borderRadius: '6px', cursor: 'pointer' }}>
                    파일 선택창 열기
                    <input type="file" accept=".py" multiple onChange={handleFileUpload} style={{ display: 'none' }} />
                  </label>
                </div>
              )}
            </div>
          </div>
        )}

        {step === 'SELECT' && (
          <div style={{ display: 'flex', width: '100%', height: '100%', boxSizing: 'border-box', padding: '20px', gap: '20px' }}>
            
            <div style={{ flex: 1, backgroundColor: '#112240', borderRadius: '8px', padding: '20px', display: 'flex', flexDirection: 'column', minHeight: 0 }}>
              <div style={{ borderBottom: '1px solid #233554', paddingBottom: '10px', marginBottom: '10px', flexShrink: 0 }}>
                <h3 style={{ marginTop: 0, marginBottom: '10px' }}>📋 전체 DAG 목록</h3>
                <input 
                  type="text" 
                  placeholder="DAG 목록 검색 (ESC: 취소)" 
                  value={listSearchInput}
                  onChange={(e) => setListSearchInput(e.target.value)}
                  style={{ width: '100%', boxSizing: 'border-box', padding: '10px', borderRadius: '4px', border: '1px solid #233554', backgroundColor: '#0a192f', color: '#fff', outline: 'none' }}
                />
              </div>
              
              <div style={{ flex: 1, overflowY: 'auto', minHeight: 0 }}>
                {filteredDagKeys.length > 0 ? (
                  filteredDagKeys.map(dagId => (
                    <div key={dagId} style={{ display: 'flex', alignItems: 'center', margin: '8px 0', padding: '10px', backgroundColor: focusedDagInfo === dagId ? '#233554' : 'transparent', borderRadius: '6px', cursor: 'pointer' }} onClick={() => setFocusedDagInfo(dagId)}>
                      <input type="checkbox" checked={selectedDags.includes(dagId)} onChange={() => toggleDagSelection(dagId)} style={{ marginRight: '10px', transform: 'scale(1.2)' }} />
                      <span style={{ fontWeight: 'bold', color: dagMetadata[dagId].type === 'MAIN' ? '#64ffda' : '#ccd6f6', wordBreak: 'break-all' }}>{dagId}</span>
                      <span style={{ marginLeft: 'auto', fontSize: '12px', padding: '2px 6px', backgroundColor: '#0a192f', borderRadius: '4px', minWidth: '35px', textAlign: 'center' }}>{dagMetadata[dagId].type}</span>
                    </div>
                  ))
                ) : (
                  <div style={{ color: '#8892b0', textAlign: 'center', marginTop: '20px' }}>검색 결과가 없습니다.</div>
                )}
              </div>
            </div>

            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '20px', minHeight: 0 }}>
              
              <div style={{ flex: 1, backgroundColor: '#112240', borderRadius: '8px', padding: '20px', display: 'flex', flexDirection: 'column', minHeight: 0 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid #233554', paddingBottom: '10px', flexShrink: 0 }}>
                  <h3 style={{ margin: 0, color: '#ffb86c' }}>✅ 시각화할 DAG 목록 ({selectedDags.length}개)</h3>
                  <button onClick={deselectAllDags} style={{ padding: '6px 12px', backgroundColor: '#ff6b6b', color: '#fff', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold', fontSize: '12px' }}>
                    전체 선택 해제
                  </button>
                </div>
                
                <div style={{ flex: 1, overflowY: 'auto', minHeight: 0, marginTop: '10px', paddingRight: '10px' }}>
                  {selectedDags.length > 0 ? (
                    selectedDags.map(dagId => (
                      <div key={`selected_${dagId}`} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 12px', backgroundColor: '#233554', margin: '6px 0', borderRadius: '4px' }}>
                        <span style={{ fontSize: '14px', wordBreak: 'break-all' }}>{dagId}</span>
                        <button onClick={() => toggleDagSelection(dagId)} style={{ background: 'none', border: 'none', color: '#ff6b6b', cursor: 'pointer', fontSize: '16px', fontWeight: 'bold', marginLeft: '10px' }}>✖</button>
                      </div>
                    ))
                  ) : (
                    <div style={{ color: '#8892b0', marginTop: '10px', textAlign: 'center' }}>좌측에서 대상을 선택하세요.</div>
                  )}
                </div>
              </div>

              <div style={{ flex: 1, backgroundColor: '#112240', borderRadius: '8px', padding: '20px', display: 'flex', flexDirection: 'column', minHeight: 0 }}>
                <h3 style={{ borderBottom: '1px solid #233554', paddingBottom: '10px', marginTop: 0, flexShrink: 0 }}>🔍 선행/후행 분석 정보</h3>
                
                <div style={{ flex: 1, overflowY: 'auto', minHeight: 0, paddingRight: '10px' }}>
                  {focusedDagInfo ? (
                    <div>
                      <h4 style={{ color: '#64ffda', wordBreak: 'break-all' }}>{focusedDagInfo}</h4>
                      
                      <div style={{ display: 'flex', gap: '8px', marginBottom: '20px', flexWrap: 'wrap' }}>
                        <button onClick={() => selectAllRelatedDags(focusedDagInfo)} style={{ padding: '6px 10px', backgroundColor: '#64ffda', color: '#020c1b', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold', fontSize: '13px' }}>
                          선/후행 모두
                        </button>
                        <button onClick={() => selectUpstreamDags(focusedDagInfo)} style={{ padding: '6px 10px', backgroundColor: '#ffb86c', color: '#020c1b', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold', fontSize: '13px' }}>
                          ↑ 선행만
                        </button>
                        <button onClick={() => selectDownstreamDags(focusedDagInfo)} style={{ padding: '6px 10px', backgroundColor: '#8be9fd', color: '#020c1b', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold', fontSize: '13px' }}>
                          ↓ 후행만
                        </button>
                        <button onClick={deselectAllDags} style={{ padding: '6px 10px', backgroundColor: '#ff6b6b', color: '#fff', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold', fontSize: '13px' }}>
                          ✖ 전체 해제
                        </button>
                      </div>
                      
                      <div style={{ marginBottom: '20px' }}>
                        <strong style={{ display: 'block', marginBottom: '8px', color: '#ffb86c' }}>↑ 선행 DAG (Upstream)</strong>
                        {dagMetadata[focusedDagInfo].upstream.length > 0 
                          ? dagMetadata[focusedDagInfo].upstream.map(id => <div key={id} style={{ padding: '8px', backgroundColor: '#233554', margin: '4px 0', borderRadius: '4px', fontSize: '14px' }}>{id}</div>)
                          : <div style={{ color: '#8892b0', fontSize: '14px' }}>없음</div>}
                      </div>
                      
                      <div>
                        <strong style={{ display: 'block', marginBottom: '8px', color: '#8be9fd' }}>↓ 후행 DAG (Downstream)</strong>
                        {dagMetadata[focusedDagInfo].downstream.length > 0 
                          ? dagMetadata[focusedDagInfo].downstream.map(id => <div key={id} style={{ padding: '8px', backgroundColor: '#233554', margin: '4px 0', borderRadius: '4px', fontSize: '14px' }}>{id}</div>)
                          : <div style={{ color: '#8892b0', fontSize: '14px' }}>없음</div>}
                      </div>
                    </div>
                  ) : (
                    <div style={{ color: '#8892b0', marginTop: '20px', textAlign: 'center' }}>좌측 목록에서 DAG를 클릭하세요.</div>
                  )}
                </div>
              </div>

            </div>
          </div>
        )}

        {step === 'VISUALIZE' && (
          <ReactFlow nodes={nodes} edges={edges} nodesDraggable={false} panOnDrag={true} selectionOnDrag={false}>
            <Background color="#233554" gap={16} />
            <Controls style={{ fill: '#64ffda' }} />
            <MiniMap nodeStrokeColor="#64ffda" nodeColor="#112240" maskColor="rgba(2, 12, 27, 0.8)" />
          </ReactFlow>
        )}
      </div>
    </div>
  );
}

export default function App() {
  return (
    <ReactFlowProvider>
      <FlowViz />
    </ReactFlowProvider>
  );
}