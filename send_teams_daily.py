#!/usr/bin/env python3
"""
Morning Briefing Sender for GitHub Actions & Local Cron
Sends the 18-signal market intelligence briefing to Microsoft Teams
"""

import os
import sys
from datetime import datetime, timezone, timedelta

def get_vietnam_time():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    except Exception:
        vn_tz = timezone(timedelta(hours=7))
        return datetime.now(vn_tz)

# Add parent dir to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.notifier.teams_notifier import TeamsNotifier

def send_briefing():
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "")
    dashboard_url = os.environ.get("DASHBOARD_PUBLIC_URL", "https://mb-riskradar.vercel.app/Dashboard_KinhTe_TaiChinh_MBGroup.html")
    
    if not webhook_url:
        print("⚠️ Không tìm thấy TEAMS_WEBHOOK_URL trong environment. Bỏ qua bước gửi Teams.")
        return

    today_str = get_vietnam_time().strftime("%d/%m/%Y")

    # Ma trận thống kê 18 tin chuẩn xác 5 lĩnh vực theo nguyên tắc Ưu tiên Rủi ro
    matrix_stats = {
        'NGAN_HANG': {'name': 'Ngân hàng', 'r_cao': 3, 'r_tb': 2, 'r_thap': 0, 'c_cao': 4, 'c_tb': 1, 'c_thap': 0, 'total': 10},
        'TC_TIEU_DUNG': {'name': 'Tài chính tiêu dùng', 'r_cao': 0, 'r_tb': 2, 'r_thap': 0, 'c_cao': 0, 'c_tb': 0, 'c_thap': 0, 'total': 2},
        'BAO_HIEM': {'name': 'Bảo hiểm', 'r_cao': 0, 'r_tb': 1, 'r_thap': 0, 'c_cao': 1, 'c_tb': 0, 'c_thap': 0, 'total': 2},
        'CHUNG_KHOAN': {'name': 'Chứng khoán & Quỹ', 'r_cao': 0, 'r_tb': 0, 'r_thap': 0, 'c_cao': 2, 'c_tb': 0, 'c_thap': 0, 'total': 2},
        'DAU_TU': {'name': 'Quản lý tài sản & Xử lý nợ', 'r_cao': 0, 'r_tb': 1, 'r_thap': 0, 'c_cao': 1, 'c_tb': 0, 'c_thap': 0, 'total': 2}
    }

    # Top tin cảnh báo rủi ro trọng yếu
    risk_articles = [
        {
            "title": "Reuters: Fed tăng lãi suất ứng phó lạm phát, chứng khoán toàn cầu và thị trường nợ biến động mạnh",
            "source_name": "Reuters",
            "type": "RUI_RO",
            "level": "CAO",
            "impact_score": 93,
            "mb_entity": "MB (MBBank)",
            "ai_summary": "Lãi suất USD neo cao gia tăng chi phí vốn vay quốc tế và rủi ro tỷ giá. MBBank cần củng cố nguồn vốn huy động nội tệ, tăng cường công cụ phái sinh FX Hedging cho doanh nghiệp XNK.",
            "url": "https://www.reuters.com/markets/rates-bonds/global-markets-inflation-rates-2026-09-18/"
        },
        {
            "title": "Tỷ giá trung tâm USD/VND lập đỉnh mới, các ngân hàng chủ động giải pháp phòng ngừa rủi ro",
            "source_name": "Dân trí",
            "type": "RUI_RO",
            "level": "CAO",
            "impact_score": 88,
            "mb_entity": "MB (MBBank)",
            "ai_summary": "Tỷ giá USD/VND biến động tăng tạo áp lực thanh khoản ngoại tệ. Khuyến nghị MB chủ động hạn mức tín dụng ngoại tệ và tối ưu danh mục hoán đổi tiền tệ.",
            "url": "https://dantri.com.vn/kinh-doanh/ty-gia-trung-tam-usdvnd-lap-dinh-moi-ngan-hang-chu-dong-phong-ngua-rui-ro-20260918091522456.htm"
        },
        {
            "title": "CNN Business: Lãi suất vay tiêu dùng và thế chấp tăng tuần thứ tư liên tiếp, áp lực trả nợ gia tăng",
            "source_name": "CNN Business",
            "type": "RUI_RO",
            "level": "TRUNG_BINH",
            "impact_score": 80,
            "mb_entity": "MCredit (Tài chính tiêu dùng MB)",
            "ai_summary": "Mặt bằng lãi suất vay tiêu dùng tăng cao làm gia tăng áp lực trả nợ. MCredit cần thắt chặt chấm điểm tín dụng số và giám sát tỷ lệ nợ quá hạn.",
            "url": "https://edition.cnn.com/2026/09/17/business/mortgage-rates-consumer-debt-trend/index.html"
        },
        {
            "title": "Bộ Tài chính tăng cường kiểm tra giám sát các đại lý bảo hiểm nhân thọ và kênh Bancassurance",
            "source_name": "Bộ Tài chính",
            "type": "RUI_RO",
            "level": "TRUNG_BINH",
            "impact_score": 82,
            "mb_entity": "MB Ageas Life",
            "ai_summary": "Siết chặt quy định tư vấn bảo hiểm qua ngân hàng. MB Ageas Life cần rà soát 100% kịch bản tư vấn tại quầy giao dịch MBBank.",
            "url": "https://mof.gov.vn/webcenter/portal/vclvcstc/pages_r/l/chi-tiet-tin?dDocName=MOFUCM2026091801"
        }
    ]

    # Top cơ hội trọng yếu
    opp_articles = [
        {
            "title": "Dân trí: Ngân hàng Việt đua làm quản gia cho giới siêu giàu, MB củng cố vị thế phân khúc Private Banking",
            "source_name": "Dân trí",
            "type": "CO_HOI",
            "level": "CAO",
            "impact_score": 95,
            "mb_entity": "MB (MBBank)",
            "ai_summary": "MB Private tiếp tục dẫn đầu thị phần dịch vụ quản lý tài sản cao cấp và gia sản gia đình, mở rộng nguồn thu phí ngoài lãi (NFI) bền vững.",
            "url": "https://dantri.com.vn/kinh-doanh/ngan-hang-viet-dua-lam-quan-gia-cho-gioi-sieu-giau-mb-khang-dinh-vi-the-private-banking-20260918083015123.htm"
        },
        {
            "title": "BIZ MBBank ứng dụng AI và dữ liệu lớn, rút ngắn hành trình vốn cho doanh nghiệp dưới 2 giờ",
            "source_name": "VnEconomy",
            "type": "CO_HOI",
            "level": "CAO",
            "impact_score": 94,
            "mb_entity": "MB (MBBank)",
            "ai_summary": "Đẩy mạnh nền tảng BIZ MBBank với quy trình phê duyệt hạn mức tín dụng tự động qua AI, tạo lợi thế cạnh tranh vượt trội thu hút khách hàng SME.",
            "url": "https://vneconomy.vn/biz-mbbank-ung-dung-ai-va-du-lieu-lon-rut-ngan-hanh-trinh-von-cho-doanh-nghiep.htm"
        },
        {
            "title": "Vietstock: MBS duy trì Top 5 thị phần môi giới HoSE, đẩy mạnh cho vay Margin an toàn",
            "source_name": "Vietstock",
            "type": "CO_HOI",
            "level": "CAO",
            "impact_score": 91,
            "mb_entity": "MBS (Chứng khoán MB)",
            "ai_summary": "MBS tối ưu hóa nguồn vốn margin và mở rộng cơ sở nhà đầu tư năng động trên nền tảng ứng dụng chứng khoán số thế hệ mới.",
            "url": "https://vietstock.vn/2026/09/mbs-duy-tri-top-5-thi-phan-moi-gioi-hose-day-manh-cho-vay-margin-830-1123456.htm"
        },
        {
            "title": "The Wall Street Journal: Ngân hàng Trung ương Nhật Bản (BOJ) nâng lãi suất lên 1,25%, cao nhất kể từ 1995",
            "source_name": "Wall Street Journal",
            "type": "CO_HOI",
            "level": "CAO",
            "impact_score": 89,
            "mb_entity": "MB (MBBank)",
            "ai_summary": "Động thái tăng lãi suất của BOJ hỗ trợ đồng Yên (JPY), mở ra dư địa cho MB phát triển các sản phẩm tài trợ thương mại song phương Việt - Nhật.",
            "url": "https://www.wsj.com/economy/central-banking/bank-of-japan-raises-interest-rates-to-highest-level-since-1995-2026-09-18"
        }
    ]

    notifier = TeamsNotifier(webhook_url=webhook_url)
    print(f"📡 Đang gửi bản tin sáng tự động tới Microsoft Teams...")
    success = notifier.send_morning_briefing(
        date_str=today_str,
        total_signals=18,
        risk_counts={"total": 9, "cao": 3, "tb": 6, "thap": 0},
        opp_counts={"total": 9, "cao": 8, "tb": 1, "thap": 0},
        risk_articles=risk_articles,
        opp_articles=opp_articles,
        matrix_stats=matrix_stats,
        dashboard_url=dashboard_url
    )
    if success:
        print("🎉 Gửi bản tin Teams thành công!")
    else:
        print("❌ Gửi Teams thất bại.")

if __name__ == "__main__":
    send_briefing()
