import os
from fastapi import FastAPI, File, UploadFile, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import uvicorn

from parser import save_and_analyze_dags, generate_flow_data_for_selected

app = FastAPI()

# CORS 설정 (React 연동을 위함)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 💡 [추가] 서버 로컬 폴더 경로를 받기 위한 Pydantic 모델
class LocalPathConfig(BaseModel):
    directory: str

@app.post("/api/upload")
async def upload_files(files: List[UploadFile] = File(...)):
    """
    1단계: 파일을 업로드 받고, 폴더에 저장한 뒤 선/후행 메타데이터만 반환합니다.
    """
    files_data = []
    for file in files:
        content = await file.read()
        files_data.append((file.filename, content.decode('utf-8')))
        
    metadata = save_and_analyze_dags(files_data) 
    return {"metadata": metadata}

# 💡 [새로 추가] 서버 로컬 디렉토리에서 스크립트를 직접 읽어오는 엔드포인트
@app.post("/api/load-local")
async def load_local_directory(config: LocalPathConfig):
    files_data = []
    target_dir = config.directory

    # 1. 디렉토리 존재 여부 확인
    if not os.path.exists(target_dir) or not os.path.isdir(target_dir):
        raise HTTPException(status_code=404, detail="지정한 폴더가 서버에 존재하지 않습니다.")

    try:
        # 2. 지정된 폴더 내의 모든 .py 파일을 찾아 메모리로 읽기
        for filename in os.listdir(target_dir):
            if filename.endswith(".py"):
                filepath = os.path.join(target_dir, filename)
                with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read()
                files_data.append((filename, content))
                
        if not files_data:
            raise HTTPException(status_code=404, detail="해당 폴더에 파이썬(.py) 파일이 없습니다.")
            
        # 3. 기존 파서 로직 실행 (DB/메타데이터 생성)
        metadata = save_and_analyze_dags(files_data)
        return {"metadata": metadata}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 파일 읽기 오류: {str(e)}")


@app.post("/api/visualize")
async def visualize_dags(request: Request):
    """
    2단계: 사용자가 프론트엔드에서 체크(선택)한 DAG 리스트를 받아,
    해당 범위의 시각화 노드/엣지 데이터만 생성하여 반환합니다.
    """
    data = await request.json()
    selected_dags = data.get("selected_dags", [])
    
    graph_data = generate_flow_data_for_selected(selected_dags)
    return graph_data

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)