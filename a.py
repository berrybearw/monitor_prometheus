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

def write_excel_per_day(data_by_day, load1_by_day):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for date, host_data in data_by_day.items():
        output_file = os.path.join(OUTPUT_DIR, f"cpu_{date}.xlsx")
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            book = writer.book
            sheet_names = {}

            # 先寫入 CPU 資料
            for host_type in ["Linux", "Windows"]:
                if host_type in host_data and host_data[host_type]:
                    df = pd.DataFrame(host_data[host_type], columns=["Timestamp", "CPU_Usage"])
                    df.to_excel(writer, sheet_name=host_type, index=False)
                    sheet_names[host_type] = host_type

            # 寫入 Linux Load1
            if date in load1_by_day and load1_by_day[date]:
                df_load = pd.DataFrame(load1_by_day[date], columns=["Timestamp", "Load1"])
                df_load.to_excel(writer, sheet_name="Linux_Load1", index=False)
                sheet_names["Linux_Load1"] = "Linux_Load1"

            # 產生 CPU 圖表
            for host_type in ["Linux", "Windows"]:
                if host_type in sheet_names:
                    row_count = len(host_data[host_type])
                    if row_count == 0:
                        continue
                    chart = book.add_chart({'type': 'bar'})
                    chart.add_series({
                        'name':       host_type,
                        'categories': f"='{host_type}'!$A$2:$A${row_count + 1}",
                        'values':     f"='{host_type}'!$B$2:$B${row_count + 1}",
                    })
                    chart.set_title({'name': f'CPU Peak - {host_type} - {date}'})
                    chart.set_x_axis({'name': 'CPU Usage (%)'})
                    chart.set_y_axis({'name': 'Time'})
                    chart.set_legend({'position': 'bottom'})
                    chart_sheet = book.add_worksheet(f'Chart_{host_type}')
                    chart_sheet.insert_chart('B3', chart)

            # 產生 Linux Load1 圖表
            if "Linux_Load1" in sheet_names:
                row_count = len(load1_by_day[date])
                if row_count > 0:
                    chart = book.add_chart({'type': 'bar'})
                    chart.add_series({
                        'name':       'Linux Load1',
                        'categories': f"='Linux_Load1'!$A$2:$A${row_count + 1}",  # Y軸: 時間
                        'values':     f"='Linux_Load1'!$B$2:$B${row_count + 1}",  # X軸: Load1
                    })
                    chart.set_title({'name': f'Load Average (1min) - Linux - {date}'})
                    chart.set_x_axis({'name': 'Load1'})
                    chart.set_y_axis({'name': 'Time'})
                    chart.set_legend({'position': 'bottom'})
                    chart_sheet = book.add_worksheet('Chart_Linux_Load1')
                    chart_sheet.insert_chart('B3', chart)

        print(f"✅ 匯出完成：{output_file}")

if __name__ == '__main__':
    print("🔍 查詢 Prometheus CPU 使用率資料中...")

    linux_results = query_range(LINUX_CPU_QUERY, START, END, STEP)
    windows_results = query_range(WINDOWS_CPU_QUERY, START, END, STEP)
    linux_load1_results = query_range(LINUX_LOAD1_QUERY, START, END, STEP)

    if not linux_results and not windows_results:
        print("🚫 沒查到任何 CPU 資料")
        exit()

    combined_results = (linux_results or []) + (windows_results or [])

    print("📦 分析與分類資料中...")
    daily_data = split_by_host_and_day(combined_results)
    daily_load1 = split_load_by_day(linux_load1_results)

    print("💾 開始寫入 Excel 報表...")
    write_excel_per_day(daily_data, daily_load1)
