import requests
import datetime
import pandas as pd
import os
from collections import defaultdict
from math import floor

PROMETHEUS = "http://localhost:9090"
START = "2025-06-10T00:00:00Z"
END = "2025-06-13T23:59:59Z"
STEP = "60s"

LINUX_CPU_QUERY = '1 - avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) by (instance)'
WINDOWS_CPU_QUERY = '100 - (avg by (instance) (rate(windows_cpu_time_total{mode="idle"}[1m])) * 100)'
LINUX_LOAD1_QUERY = 'avg(node_load1) by (instance)'
LINUX_MEM_QUERY = '1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)'
WINDOWS_MEM_QUERY = '1 - (windows_os_physical_memory_free_bytes / windows_cs_physical_memory_bytes)'

OUTPUT_DIR = "./cpu_exports"

def get_instance_type_map():
    url = f"{PROMETHEUS}/api/v1/series"
    params = {'match[]': 'up{job=~".*"}'}
    response = requests.get(url, params=params)
    result = response.json()['data']

    instance_type_map = {}
    for item in result:
        instance = item.get('instance')
        job = item.get('job')
        if instance:
            if job == 'node_exporter':
                instance_type_map[instance] = 'Linux'
            elif job == 'win_exporter':
                instance_type_map[instance] = 'Windows'
            else:
                instance_type_map[instance] = 'Unknown'
    return instance_type_map

def query_range(query, start, end, step):
    url = f"{PROMETHEUS}/api/v1/query_range"
    params = {
        'query': query,
        'start': start,
        'end': end,
        'step': step
    }
    response = requests.get(url, params=params)

    data = response.json()

    if data["status"] != "success":
        print("❌ 查詢失敗:", data.get("error", "未知錯誤"))
        return

    return response.json()['data']['result']

def split_by_host_and_day(results, sample_every=10):  # 每 N 筆取一筆
    data_by_day = defaultdict(lambda: defaultdict(list))
    instance_type_map = get_instance_type_map()

    for item in results:
        instance = item['metric']['instance']
        host_type = instance_type_map.get(instance, "Unknown")
        if host_type == "Unknown":
            print(f"⚠️ 無法識別主機類型：{instance}")
            continue

        for idx, point in enumerate(item['values']):
            if idx % sample_every != 0:  # 每 N 筆取一筆
                continue
            ts = datetime.datetime.fromtimestamp(point[0])
            date_str = ts.strftime("%Y-%m-%d")
            value = float(point[1])
            data_by_day[date_str][host_type].append((ts.strftime("%H:%M"), value))  # 簡化時間格式

    return data_by_day

def split_load_by_day(results, sample_every=10):
    data_by_day = defaultdict(list)
    instance_type_map = get_instance_type_map()

    for item in results:
        instance = item['metric']['instance']
        host_type = instance_type_map.get(instance, "Unknown")
        if host_type != "Linux":
            continue

        for idx, point in enumerate(item['values']):
            if idx % sample_every != 0:
                continue
            ts = datetime.datetime.fromtimestamp(point[0])
            date_str = ts.strftime("%Y-%m-%d")
            value = float(point[1])
            data_by_day[date_str].append((ts.strftime("%H:%M"), value))

    return data_by_day

def write_excel_per_day(data_by_day, load1_by_day, linux_mem_by_day, windows_mem_by_day, linux_core_count):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for date, host_data in data_by_day.items():
        output_file = os.path.join(OUTPUT_DIR, f"cpu_{date}.xlsx")
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            book = writer.book
            sheet_names = {}

            # 先寫入 Linux/Windows 資料
            for host_type in ["Linux", "Windows"]:
                if host_type in host_data and host_data[host_type]:
                    if host_type == "Linux":
                        cpu_data = host_data[host_type]
                        load1_data = load1_by_day.get(date, [])
                        mem_data = linux_mem_by_day.get(date, [])
                        max_len = max(len(cpu_data), len(load1_data), len(mem_data))
                        rows = []
                        for i in range(max_len):
                            t = cpu_data[i][0] if i < len(cpu_data) else None
                            cpu = cpu_data[i][1] if i < len(cpu_data) else None
                            load1 = load1_data[i][1] if i < len(load1_data) else None
                            mem = mem_data[i][1] if i < len(mem_data) else None
                            rows.append([t, cpu, load1, mem])
                        df = pd.DataFrame(rows, columns=["Timestamp", "CPU_Usage", "Load1", "Mem_Usage"])
                    else:
                        cpu_data = host_data[host_type]
                        mem_data = windows_mem_by_day.get(date, [])
                        max_len = max(len(cpu_data), len(mem_data))
                        rows = []
                        for i in range(max_len):
                            t = cpu_data[i][0] if i < len(cpu_data) else None
                            cpu = cpu_data[i][1] if i < len(cpu_data) else None
                            mem = mem_data[i][1] if i < len(mem_data) else None
                            rows.append([t, cpu, mem])
                        df = pd.DataFrame(rows, columns=["Timestamp", "CPU_Usage", "Mem_Usage"])
                    df.to_excel(writer, sheet_name=host_type, index=False)
                    sheet_names[host_type] = host_type

            # 產生圖表
            for host_type in ["Linux", "Windows"]:
                if host_type in sheet_names:
                    if host_type == "Linux":
                        # 用剛剛建立的 df
                        # CPU Usage 圖表
                        chart_cpu = book.add_chart({'type': 'bar'})
                        chart_cpu.add_series({
                            'name': 'CPU Usage',
                            'categories': f"='Linux'!$A$2:$A${len(df)+1}",
                            'values': f"='Linux'!$B$2:$B${len(df)+1}",
                        })
                        core_num = next(iter(linux_core_count.values()), "?")
                        chart_cpu.set_title({'name': f'CPU Usage - Linux - {date} ({core_num} cores)'})
                        chart_cpu.set_x_axis({'name': 'Time'})
                        chart_cpu.set_y_axis({'name': 'CPU Usage (%)'})
                        chart_cpu.set_legend({'position': 'bottom'})

                        # Load1 圖表
                        chart_load = book.add_chart({'type': 'bar'})
                        chart_load.add_series({
                            'name': 'Load1',
                            'categories': f"='Linux'!$A$2:$A${len(df)+1}",
                            'values': f"='Linux'!$C$2:$C${len(df)+1}",
                        })
                        chart_load.set_title({'name': f'Load1 - Linux - {date} ({core_num} cores)'})
                        chart_load.set_x_axis({'name': 'Time'})
                        chart_load.set_y_axis({'name': 'Load1'})
                        chart_load.set_legend({'position': 'bottom'})

                        # Mem Usage 圖表
                        chart_mem = book.add_chart({'type': 'bar'})
                        chart_mem.add_series({
                            'name': 'Mem Usage',
                            'categories': f"='Linux'!$A$2:$A${len(df)+1}",
                            'values': f"='Linux'!$D$2:$D${len(df)+1}",
                        })
                        chart_mem.set_title({'name': f'Memory Usage - Linux - {date}'})
                        chart_mem.set_x_axis({'name': 'Time'})
                        chart_mem.set_y_axis({'name': 'Mem Usage'})
                        chart_mem.set_legend({'position': 'bottom'})

                        # 插入到同一個分頁
                        chart_sheet = book.add_worksheet('Chart_Linux')
                        chart_sheet.insert_chart('B3', chart_cpu)
                        chart_sheet.insert_chart('B20', chart_load)
                        chart_sheet.insert_chart('B37', chart_mem)
                    else:
                        chart_cpu = book.add_chart({'type': 'bar'})
                        chart_cpu.add_series({
                            'name': 'CPU Usage',
                            'categories': f"='Windows'!$A$2:$A${len(df)+1}",
                            'values': f"='Windows'!$B$2:$B${len(df)+1}",
                        })
                        chart_cpu.set_title({'name': f'CPU Usage - Windows - {date}'})
                        chart_cpu.set_x_axis({'name': 'Time'})
                        chart_cpu.set_y_axis({'name': 'CPU Usage (%)'})
                        chart_cpu.set_legend({'position': 'bottom'})

                        chart_mem = book.add_chart({'type': 'bar'})
                        chart_mem.add_series({
                            'name': 'Mem Usage',
                            'categories': f"='Windows'!$A$2:$A${len(df)+1}",
                            'values': f"='Windows'!$C$2:$C${len(df)+1}",
                        })
                        chart_mem.set_title({'name': f'Memory Usage - Windows - {date}'})
                        chart_mem.set_x_axis({'name': 'Time'})
                        chart_mem.set_y_axis({'name': 'Mem Usage'})
                        chart_mem.set_legend({'position': 'bottom'})

                        chart_sheet = book.add_worksheet('Chart_Windows')
                        chart_sheet.insert_chart('B3', chart_cpu)
                        chart_sheet.insert_chart('B20', chart_mem)

            # 產生 Linux Load1 圖表
            if "Linux_Load1" in sheet_names:
                row_count = len(load1_by_day[date])
                if row_count > 0:
                    # 取得本日第一台 Linux instance 的 core 數
                    instance = None
                    for item in linux_core_count:
                        instance = item
                        break
                    core_num = linux_core_count.get(instance, "?")
                    chart = book.add_chart({'type': 'bar'})
                    chart.add_series({
                        'name':       'Linux Load1',
                        'categories': f"='Linux_Load1'!$A$2:$A${row_count + 1}",  # Y軸: 時間
                        'values':     f"='Linux_Load1'!$B$2:$B${row_count + 1}",  # X軸: Load1
                    })
                    chart.set_title({'name': f'Load Average (1min) - Linux - {date} ({core_num} cores)'})
                    chart.set_x_axis({'name': 'Load1'})
                    chart.set_y_axis({'name': 'Time'})
                    chart.set_legend({'position': 'bottom'})
                    chart_sheet = book.add_worksheet('Chart_Linux_Load1')
                    chart_sheet.insert_chart('B3', chart)

        print(f"✅ 匯出完成：{output_file}")

def get_linux_core_count():
    query = 'count(count(node_cpu_seconds_total{mode="idle"}) by (cpu, instance)) by (instance)'
    url = f"{PROMETHEUS}/api/v1/query"
    params = {'query': query}
    response = requests.get(url, params=params)
    data = response.json()
    core_count = {}
    if data["status"] == "success":
        for item in data["data"]["result"]:
            instance = item["metric"]["instance"]
            value = int(float(item["value"][1]))
            core_count[instance] = value
    return core_count

if __name__ == '__main__':
    print("🔍 查詢 Prometheus CPU/Memory 使用率資料中...")

    linux_results = query_range(LINUX_CPU_QUERY, START, END, STEP)
    windows_results = query_range(WINDOWS_CPU_QUERY, START, END, STEP)
    linux_load1_results = query_range(LINUX_LOAD1_QUERY, START, END, STEP)
    linux_mem_results = query_range(LINUX_MEM_QUERY, START, END, STEP)
    windows_mem_results = query_range(WINDOWS_MEM_QUERY, START, END, STEP)
    linux_core_count = get_linux_core_count()

    if not linux_results and not windows_results:
        print("🚫 沒查到任何 CPU 資料")
        exit()

    combined_results = (linux_results or []) + (windows_results or [])
    print("📦 分析與分類資料中...")
    daily_data = split_by_host_and_day(combined_results)
    daily_load1 = split_load_by_day(linux_load1_results)
    daily_linux_mem = split_load_by_day(linux_mem_results)
    daily_windows_mem = split_load_by_day(windows_mem_results)

    print("💾 開始寫入 Excel 報表...")
    write_excel_per_day(daily_data, daily_load1, daily_linux_mem, daily_windows_mem, linux_core_count)
