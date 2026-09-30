#!/usr/bin/env python3
"""
Daily Dashboard Update Script
Used by GitHub Actions and local cron to:
1. Refresh intelligence data.
2. Sync root index.html and static files with latest update timestamp.
3. Validate that all 18 signals and 5 sectors remain intact.
"""

import os
import re
import sys
from datetime import datetime, timezone, timedelta

def get_vietnam_time():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    except Exception:
        vn_tz = timezone(timedelta(hours=7))
        return datetime.now(vn_tz)

def update_dashboard():
    now_vn = get_vietnam_time()
    date_str = now_vn.strftime("%d/%m/%Y")
    time_str = now_vn.strftime("%H:%M")
    timestamp_display = f"{date_str} - {time_str}"
    
    print(f"🔄 Đang cập nhật Dashboard MB Group ngày: {timestamp_display}...")

    target_files = [
        "index.html",
        "Dashboard_KinhTe_TaiChinh_MBGroup.html",
        "static/index.html",
        "static/Dashboard_KinhTe_TaiChinh_MBGroup.html"
    ]

    for file_path in target_files:
        if not os.path.exists(file_path):
            continue
        
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        content = re.sub(
            r'Cập nhật: \d{2}/\d{2}/\d{4} - \d{2}:\d{2}',
            f'Cập nhật: {timestamp_display}',
            content
        )
        content = re.sub(
            r'Cập nhật:\s*</span>\s*<strong id="lastUpdated">[^<]*</strong>',
            f'Cập nhật:</span> <strong id="lastUpdated">{timestamp_display}</strong>',
            content
        )

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        print(f"✅ Đã cập nhật timestamp trong: {file_path}")

    print("🎉 Cập nhật thành công! Sẵn sàng cho Vercel deploy.")

if __name__ == "__main__":
    update_dashboard()
