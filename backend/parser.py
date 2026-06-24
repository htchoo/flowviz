import os
import re

def convert_cron_to_kst(cron_str):
    if not cron_str or cron_str == "None":
        return "⏰ 스케줄 없음"
    parts = cron_str.split()
    if len(parts) == 5 and parts[0].isdigit() and parts[1].isdigit():
        minute = int(parts[0])
        utc_hour = int(parts[1])
        kst_hour = (utc_hour + 9) % 24
        return f"⏰ KST 매일 {kst_hour:02d}:{minute:02d}"
    return f"⏰ {cron_str}"

def save_and_analyze_dags(files_data, save_folder="./uploaded_dags"):
    os.makedirs(save_folder, exist_ok=True)
    
    for filename, content in files_data:
        filepath = os.path.join(save_folder, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)

    dag_details = {}
    
    for filename in os.listdir(save_folder):
        if not filename.endswith(".py"): continue
        filepath = os.path.join(save_folder, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        main_dag_match = re.search(r'dag_id\s*=\s*["\'](.*?)["\']', content)
        main_dag_id = main_dag_match.group(1) if main_dag_match else filename.replace(".py", "")

        if main_dag_id not in dag_details:
            dag_details[main_dag_id] = {"id": main_dag_id, "type": "MAIN", "upstream": set(), "downstream": set()}
        else:
            dag_details[main_dag_id]["type"] = "MAIN"

        dag_list_all_match = re.search(r'dag_list_all\s*=\s*\[(.*?)\]', content, re.DOTALL)
        if dag_list_all_match:
            raw_lists = re.findall(r'([a-zA-Z0-9_]+)', dag_list_all_match.group(1))
            stage_names = [name for name in raw_lists if 'dag_list' in name]

            for stage_name in stage_names:
                list_match = re.search(rf'{stage_name}\s*=\s*\[(.*?)\]', content, re.DOTALL)
                if list_match:
                    dags = re.findall(r'["\'](.*?)["\']', list_match.group(1))
                    for sub_dag in dags:
                        if sub_dag not in dag_details:
                            dag_details[sub_dag] = {"id": sub_dag, "type": "SUB", "upstream": set(), "downstream": set()}
                        
                        dag_details[main_dag_id]["downstream"].add(sub_dag)
                        dag_details[sub_dag]["upstream"].add(main_dag_id)
                        
    for d_id, info in dag_details.items():
        info["upstream"] = list(info["upstream"])
        info["downstream"] = list(info["downstream"])
        
    return dag_details

def generate_flow_data_for_selected(selected_dags, folder_path="./uploaded_dags"):
    parsed_files = []
    
    for filename in os.listdir(folder_path):
        if not filename.endswith(".py"): continue
        filepath = os.path.join(folder_path, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        main_dag_match = re.search(r'dag_id\s*=\s*["\'](.*?)["\']', content)
        main_dag_id = main_dag_match.group(1) if main_dag_match else filename.replace(".py", "")

        schedule_match = re.search(r'schedule_interval\s*=\s*["\'](.*?)["\']', content)
        if not schedule_match:
            schedule_match = re.search(r'schedule_interval\s*=\s*(None)', content)
        schedule_str = schedule_match.group(1) if schedule_match else "None"
        kst_schedule = convert_cron_to_kst(schedule_str)

        dag_list_all_match = re.search(r'dag_list_all\s*=\s*\[(.*?)\]', content, re.DOTALL)
        stage_dags_list = []
        
        if dag_list_all_match:
            raw_lists = re.findall(r'([a-zA-Z0-9_]+)', dag_list_all_match.group(1))
            stage_names = [name for name in raw_lists if 'dag_list' in name]

            for stage_name in stage_names:
                list_match = re.search(rf'{stage_name}\s*=\s*\[(.*?)\]', content, re.DOTALL)
                if list_match:
                    dags = re.findall(r'["\'](.*?)["\']', list_match.group(1))
                    filtered_dags = [d for d in dags if d in selected_dags]
                    if filtered_dags:
                        stage_dags_list.append(filtered_dags)

        if main_dag_id in selected_dags or stage_dags_list:
            parsed_files.append({
                "main_dag_id": main_dag_id,
                "schedule": kst_schedule,
                "stages": stage_dags_list
            })

    # 💡 [핵심 추가] 현재 그려질 전체 그래프에서 부모로부터 '호출당하는(Triggered)' DAG들을 모두 수집
    triggered_in_graph = set()
    for f in parsed_files:
        for stage in f["stages"]:
            triggered_in_graph.update(stage)

    all_nodes = []
    all_edges = []
    file_y_offset = 100
    X_GAP = 600  
    Y_GAP = 220  
    main_dag_map = {} 

    for file_data in parsed_files:
        main_dag_id = file_data["main_dag_id"]
        stages = file_data["stages"]

        # 💡 [핵심 로직] 본인의 후행 작업(stages)이 하나도 없으면서, 부모(Stage 노드)에 의해 이미 호출된 경우 
        # 불필요한 MAIN 껍데기 노드를 그리지 않고 건너뜁니다!
        if not stages and main_dag_id in triggered_in_graph:
            continue

        max_nodes_in_file = max([len(stage) for stage in stages]) if stages else 1
        current_file_center_y = file_y_offset + (max_nodes_in_file * Y_GAP) / 2

        main_node_id = f"main_{main_dag_id}"
        main_dag_map[main_dag_id] = main_node_id
        
        all_nodes.append({
            "id": main_node_id,
            "className": "nodrag nopan",
            "sourcePosition": "right", 
            "targetPosition": "left",
            "data": { "label": f"👑 MAIN/GROUP\n{main_dag_id}\n\n{file_data['schedule']}", "search_key": main_dag_id },
            "position": {"x": 50, "y": current_file_center_y},
            "style": {
                "backgroundColor": "#112240", "color": "#64ffda", "padding": "20px", "borderRadius": "8px", 
                "width": "max-content", "minWidth": 280, "textAlign": "center", "fontWeight": "bold", "whiteSpace": "pre",
                "userSelect": "text", "WebkitUserSelect": "text", "cursor": "text", "pointerEvents": "all" 
            }
        })

        x_offset = 50 + X_GAP
        prev_stage_nodes = [main_node_id]

        for idx, dags in enumerate(stages):
            current_stage_nodes = []
            stage_height = (len(dags) - 1) * Y_GAP
            y_start = current_file_center_y - (stage_height / 2)
            
            for i, dag in enumerate(dags):
                task_node_id = f"task_{main_dag_id}_{dag}"
                current_stage_nodes.append(task_node_id)
                
                all_nodes.append({
                    "id": task_node_id,
                    "className": "nodrag nopan", 
                    "sourcePosition": "right", "targetPosition": "left",
                    "data": { "label": f"🔹 [Stage {idx+1}]\n{dag}", "original_dag_id": dag, "search_key": dag },
                    "position": {"x": x_offset, "y": y_start + (i * Y_GAP)},
                    "style": {
                        "backgroundColor": "#175a76", "color": "#ffffff", "border": "1px solid #0d3848", 
                        "width": "max-content", "minWidth": 300, "borderRadius": "0px", "padding": "15px",
                        "whiteSpace": "pre", "textAlign": "center", "fontWeight": "bold",
                        "userSelect": "text", "WebkitUserSelect": "text", "cursor": "text", "pointerEvents": "all"
                    }
                })

                for prev_node in prev_stage_nodes:
                    all_edges.append({
                        "id": f"e_{prev_node}_{task_node_id}", "source": prev_node, "target": task_node_id,
                        "animated": True, "type": "step", "style": {"stroke": "#64ffda", "strokeWidth": 2}
                    })
                    
            prev_stage_nodes = current_stage_nodes
            x_offset += X_GAP
        
        file_y_offset += (max_nodes_in_file * Y_GAP) + 300 

    for node in all_nodes:
        original_dag_id = node.get("data", {}).get("original_dag_id")
        # 연결 대상(target_main_node)이 생성되지 않고 스킵된 경우, 자연스럽게 Triggers 엣지도 생성되지 않습니다.
        if original_dag_id and original_dag_id in main_dag_map:
            target_main_node_id = main_dag_map[original_dag_id]
            all_edges.append({
                "id": f"e_trigger_{node['id']}_{target_main_node_id}", "source": node['id'], "target": target_main_node_id,
                "animated": True, "type": "step", "label": "Triggers",
                "style": {"stroke": "#ffb86c", "strokeWidth": 3, "strokeDasharray": "5,5"},
                "labelStyle": {"fill": "#ffb86c", "fontWeight": "bold"}
            })

    return {"nodes": all_nodes, "edges": all_edges}