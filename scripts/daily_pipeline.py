#!/usr/bin/env python3
"""
FinIntel AI - Standalone Autonomous Daily Intelligence Pipeline
Runs daily on GitHub Actions (07:30 AM VN time / 00:30 UTC) or local cron.

Features:
- Self-contained: No external app/ directory dependencies required.
- Real-time crawler: Fetches 12 Domestic & Global financial RSS feeds.
- AI Risk-First Classifier: Strict CAO first, then TRUNG BINH, descending by impact score.
- Direct Dashboard Synchronizer: Replaces REAL_ARTICLES in index.html.
- Executive Adaptive Card: Broadcasts 18-signal matrix & top alerts to MS Teams.
"""

import os
import sys
import re
import json
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import List, Dict, Any

import html
import httpx
import requests

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))
except Exception:
    pass

try:
    from zoneinfo import ZoneInfo
    VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except Exception:
    VN_TZ = timezone(timedelta(hours=7))

def get_vietnam_time() -> datetime:
    return datetime.now(VN_TZ)

# 12 NGUỒN BÁO CHÍ TÀI CHÍNH & CÔNG NGHỆ UY TÍN (6 TRONG NƯỚC + 6 QUỐC TẾ)
RSS_FEEDS = [
    # Trong nước (Quét cả mảng Tài chính - Kinh tế và mảng Công nghệ / Số hóa của 6 tòa soạn)
    {"source": "cafef", "sourceName": "CafeF", "sourceGroup": "DOMESTIC", "url": "https://cafef.vn/tai-chinh-ngan-hang.rss"},
    {"source": "cafef", "sourceName": "CafeF", "sourceGroup": "DOMESTIC", "url": "https://cafef.vn/thi-truong-chung-khoan.rss"},
    {"source": "vnexpress", "sourceName": "VnExpress", "sourceGroup": "DOMESTIC", "url": "https://vnexpress.net/rss/kinh-doanh.rss"},
    {"source": "vnexpress", "sourceName": "VnExpress", "sourceGroup": "DOMESTIC", "url": "https://vnexpress.net/rss/so-hoa.rss"},
    {"source": "vneconomy", "sourceName": "VNEconomy", "sourceGroup": "DOMESTIC", "url": "https://vneconomy.vn/tai-chinh.rss"},
    {"source": "vneconomy", "sourceName": "VNEconomy", "sourceGroup": "DOMESTIC", "url": "https://vneconomy.vn/chung-khoan.rss"},
    {"source": "dantri", "sourceName": "Dân trí", "sourceGroup": "DOMESTIC", "url": "https://dantri.com.vn/rss/kinh-doanh.rss"},
    {"source": "dantri", "sourceName": "Dân trí", "sourceGroup": "DOMESTIC", "url": "https://dantri.com.vn/rss/cong-nghe.rss"},
    {"source": "tuoitre", "sourceName": "Tuổi Trẻ", "sourceGroup": "DOMESTIC", "url": "https://tuoitre.vn/rss/kinh-doanh.rss"},
    {"source": "tuoitre", "sourceName": "Tuổi Trẻ", "sourceGroup": "DOMESTIC", "url": "https://tuoitre.vn/rss/nhip-song-so.rss"},
    {"source": "vietstock", "sourceName": "Vietstock", "sourceGroup": "DOMESTIC", "url": "https://vietstock.vn/rss/tai-chinh.rss"},
    # Quốc tế (Quét cả Tài chính, Kinh tế và Công nghệ / An ninh mạng từ 6 hãng tin hàng đầu thế giới)
    {"source": "reuters", "sourceName": "Reuters", "sourceGroup": "INTERNATIONAL", "url": "https://news.google.com/rss/search?q=when:2d+source:Reuters+finance+markets+banking&hl=en-US&gl=US&ceid=US:en"},
    {"source": "reuters", "sourceName": "Reuters", "sourceGroup": "INTERNATIONAL", "url": "https://news.google.com/rss/search?q=when:2d+source:Reuters+technology+cybersecurity&hl=en-US&gl=US&ceid=US:en"},
    {"source": "bloomberg", "sourceName": "Bloomberg", "sourceGroup": "INTERNATIONAL", "url": "https://news.google.com/rss/search?q=when:2d+source:Bloomberg+markets+finance+economy&hl=en-US&gl=US&ceid=US:en"},
    {"source": "bloomberg", "sourceName": "Bloomberg", "sourceGroup": "INTERNATIONAL", "url": "https://news.google.com/rss/search?q=when:2d+source:Bloomberg+technology+ai&hl=en-US&gl=US&ceid=US:en"},
    {"source": "cnn", "sourceName": "CNN", "sourceGroup": "INTERNATIONAL", "url": "https://news.google.com/rss/search?q=when:2d+source:CNN+business+markets+economy&hl=en-US&gl=US&ceid=US:en"},
    {"source": "wsj", "sourceName": "The Wall Street Journal", "sourceGroup": "INTERNATIONAL", "url": "https://news.google.com/rss/search?q=when:2d+source:Wall+Street+Journal+finance+markets&hl=en-US&gl=US&ceid=US:en"},
    {"source": "guardian", "sourceName": "The Guardian", "sourceGroup": "INTERNATIONAL", "url": "https://www.theguardian.com/business/economics/rss"},
    {"source": "guardian", "sourceName": "The Guardian", "sourceGroup": "INTERNATIONAL", "url": "https://www.theguardian.com/technology/rss"},
    {"source": "economist", "sourceName": "The Economist", "sourceGroup": "INTERNATIONAL", "url": "https://news.google.com/rss/search?q=when:7d+source:The+Economist+finance+banking+economy&hl=en-US&gl=US&ceid=US:en"},
    {"source": "cnbc", "sourceName": "CNBC", "sourceGroup": "INTERNATIONAL", "url": "https://search.cnbc.com/rs/search/view.html?partnerId=2000&keywords=markets&sort=date"}
]

SECTOR_MAP = {
    "NGAN_HANG": {"name": "Ngân hàng", "default_entity": "MB", "default_entity_name": "MB (MBBank)"},
    "TC_TIEU_DUNG": {"name": "Tài chính tiêu dùng", "default_entity": "MCREDIT", "default_entity_name": "Mcredit"},
    "BAO_HIEM": {"name": "Bảo hiểm", "default_entity": "MBLIFE", "default_entity_name": "MB Ageas Life"},
    "CHUNG_KHOAN": {"name": "Chứng khoán & Quỹ", "default_entity": "MBS", "default_entity_name": "MBS"},
    "DAU_TU": {"name": "Quản lý tài sản & XL nợ", "default_entity": "MBAMC", "default_entity_name": "MB AMC"}
}

def clean_html(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def extract_summary(desc: str, title: str) -> str:
    c = clean_html(desc)
    # Loại bỏ tiền tố nguồn tin thừa ở đầu nếu có
    c = re.sub(r"^\([^\)]+\)\s*[-–—:]\s*", "", c)
    c = re.sub(r"^(VnEconomy|CafeF|VnExpress|Dân trí|Tuổi Trẻ|Vietstock|Reuters|Bloomberg|CNBC)\s*[-–—:]\s*", "", c, flags=re.I)
    # Loại bỏ tên nguồn tin thừa ở cuối nếu có
    c = re.sub(r"\s*[-–—|]\s*(Reuters|Bloomberg|CNBC|VnEconomy|CafeF|VnExpress|Dân trí|Tuổi Trẻ)\s*$", "", c, flags=re.I)
    c = re.sub(r"\s+(Reuters|Bloomberg|CNBC|VnEconomy|CafeF|VnExpress|Dân trí|Tuổi Trẻ)\s*$", "", c, flags=re.I)
    c = c.strip()
    if not c or len(c) < 15:
        c = title
    if len(c) > 280:
        c = c[:277].rsplit(" ", 1)[0] + "..."
    return c

def parse_rss_date(pub_date_str: str) -> datetime:
    if not pub_date_str:
        return datetime.min.replace(tzinfo=VN_TZ)
    clean_str = pub_date_str.strip().replace('\u202f', ' ').replace('\xa0', ' ')
    try:
        return parsedate_to_datetime(clean_str).astimezone(VN_TZ)
    except Exception:
        pass
    for fmt in [
        '%m/%d/%Y %I:%M:%S %p',
        '%m/%d/%Y %H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y %I:%M:%S %p',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%SZ'
    ]:
        try:
            d = datetime.strptime(clean_str, fmt)
            if d.tzinfo is None:
                d = d.replace(tzinfo=VN_TZ)
            else:
                d = d.astimezone(VN_TZ)
            return d
        except Exception:
            pass
    return datetime.min.replace(tzinfo=VN_TZ)

async def fetch_rss_feed(client: httpx.AsyncClient, feed_info: dict) -> List[dict]:
    items = []
    try:
        resp = await client.get(feed_info["url"], timeout=10.0, follow_redirects=True)
        if resp.status_code != 200:
            return items
        root = ET.fromstring(resp.text)
        channel = root.find("channel")
        feed_items = channel.findall("item") if channel is not None else root.findall(".//item")

        now_vn = datetime.now(VN_TZ)
        for it in feed_items[:15]:
            title = it.findtext("title", "").strip()
            link = it.findtext("link", "").strip()
            desc = it.findtext("description", "").strip()
            pub_date = it.findtext("pubDate", "").strip()

            if not title or not link:
                continue

            dt = parse_rss_date(pub_date)

            # Lọc nghiêm ngặt: Bỏ qua tin cũ hơn 36 giờ (chỉ lấy tin hôm nay và hôm qua)
            if dt < (now_vn - timedelta(hours=36)):
                continue

            summary_text = extract_summary(desc, title)

            items.append({
                "title": title,
                "url": link,
                "source": feed_info["source"],
                "sourceName": feed_info["sourceName"],
                "source_name": feed_info["sourceName"],
                "sourceGroup": feed_info["sourceGroup"],
                "source_group": feed_info["sourceGroup"],
                "content": clean_html(f"{title}. {desc}"),
                "summary": summary_text,
                "published_at": dt
            })
    except Exception:
        pass
    return items

def generate_dynamic_evaluation(title: str, content: str, sector: str, mb_entity_name: str, typ: str, lvl: str) -> str:
    text = f"{title} {content}".lower()
    
    # 0. RỦI RO CÔNG NGHỆ, AN TOÀN THÔNG TIN & AN NINH MẠNG (Áp dụng cho cả 5 nhóm lĩnh vực MB Group)
    tech_risk_keywords = [
        "đột nhập", "xâm nhập", "tấn công mạng", "an ninh mạng", "an toàn thông tin",
        "lỗ hổng", "hacker", "hack", "rò rỉ dữ liệu", "lộ lọt dữ liệu", "đánh cắp dữ liệu",
        "mã độc", "tống tiền", "ransomware", "chiếm quyền", "sập hệ thống", "sự cố it",
        "gián đoạn hệ thống", "lộ thông tin", "openai", "ai agent", "tác nhân ai", "trí tuệ nhân tạo",
        "deepfake", "lỗ hổng bảo mật", "phần mềm độc hại", "ddos", "phishing",
        "cyber attack", "cyberattack", "data breach", "malware", "exploit", "zero-day",
        "hacked", "security flaw", "threat actor", "infiltrate", "infiltrated", "unauthorized access"
    ]
    if any(k in text for k in tech_risk_keywords):
        if typ == "RUI_RO":
            impact = "Sự cố an toàn thông tin, xâm nhập hệ thống, rò rỉ dữ liệu hoặc rủi ro tác nhân AI đe dọa an toàn vận hành, bảo mật dữ liệu khách hàng và tiềm ẩn nguy cơ gián đoạn dịch vụ."
            if sector == "NGAN_HANG" or "mbbank" in mb_entity_name.lower():
                arg = f"Khuyến nghị {mb_entity_name} khẩn cấp rà soát các cổng API, kiểm soát phân quyền tác nhân AI/kết nối bên ngoài, tăng cường giám sát trung tâm SOC 24/7 và kiểm tra phương án ứng cứu sự cố an ninh mạng."
            elif sector == "TC_TIEU_DUNG" or "mcredit" in mb_entity_name.lower():
                arg = f"Khuyến nghị {mb_entity_name} thắt chặt bảo mật luồng phê duyệt và giải ngân tự động, tăng cường xác thực đa lớp (MFA/eKYC) nhằm ngăn chặn tấn công chiếm đoạt tài khoản và gian lận định danh khách hàng."
            elif sector == "CHUNG_KHOAN" or "mbs" in mb_entity_name.lower():
                arg = f"Khuyến nghị {mb_entity_name} tăng cường an ninh hệ thống giao dịch trực tuyến, phòng ngừa nguy cơ tấn công DDoS, rò rỉ tài khoản và can thiệp trái phép vào hệ thống lệnh mua bán."
            elif sector == "BAO_HIEM" or "ageas" in mb_entity_name.lower() or "mic" in mb_entity_name.lower():
                arg = f"Khuyến nghị {mb_entity_name} rà soát bảo mật dữ liệu hợp đồng và thông tin sức khỏe/tài chính cá nhân, siết chặt quản lý dữ liệu đối tác số nhằm tránh lộ lọt thông tin nhạy cảm."
            elif sector == "DAU_TU" or "amc" in mb_entity_name.lower():
                arg = f"Khuyến nghị {mb_entity_name} tăng cường bảo mật hạ tầng số quản lý danh mục nợ và dữ liệu đấu giá tài sản bảo đảm, bảo vệ an toàn hồ sơ định giá và thông tin khách hàng."
            else:
                arg = f"Khuyến nghị {mb_entity_name} rà soát lỗ hổng bảo mật, cập nhật bản vá hệ thống và tăng cường cơ chế kiểm soát an toàn thông tin đa tầng."
        else:
            impact = "Xu hướng đột phá công nghệ, tự động hóa và ứng dụng AI tạo lợi thế cạnh tranh vượt trội về tốc độ xử lý, trải nghiệm khách hàng và tối ưu chi phí vận hành (CIR)."
            arg = f"{mb_entity_name} tiếp tục tiên phong ứng dụng công nghệ số và AI an toàn trong quy trình vận hành và phục vụ khách hàng, mở rộng tệp người dùng hệ sinh thái."

    # 1. Tỷ giá / Ngoại hối / FED / BOJ / Tiền tệ quốc tế
    elif any(k in text for k in ["tỷ giá", "usd", "ngoại tệ", "fed", "đô la", "yên", "boj", "jpy", "eur", "cny", "exchange rate", "currency"]):
        if typ == "RUI_RO":
            impact = "Biến động tỷ giá và chi phí vốn ngoại tệ gia tăng áp lực lên thanh khoản USD và rủi ro trạng thái ngoại hối."
            arg = f"Khuyến nghị {mb_entity_name} chủ động kiểm soát hạn mức trạng thái ngoại tệ ròng, đẩy mạnh các công cụ phái sinh FX Forward/Swap cho doanh nghiệp XNK."
        else:
            impact = "Tín hiệu tỷ giá hạ nhiệt hoặc chính sách tiền tệ nới lỏng hỗ trợ dòng vốn đầu tư và hoạt động thương mại quốc tế."
            arg = f"{mb_entity_name} xem xét mở rộng các gói tài trợ thương mại ưu đãi và dịch vụ thanh toán quốc tế số."

    # 2. Lãi suất / Tiền gửi / Huy động vốn / CASA / NIM
    elif any(k in text for k in ["lãi suất", "tiết kiệm", "huy động", "tiền gửi", "casa", "nim", "lãi vay", "interest rate", "deposit"]):
        if typ == "RUI_RO":
            impact = "Áp lực mặt bằng lãi suất huy động gia tăng có thể làm co hẹp biên lãi thuần (NIM) và đẩy chi phí vốn đầu vào tăng cao."
            arg = f"Khuyến nghị {mb_entity_name} tối ưu hóa cơ cấu nguồn vốn, gia tăng tỷ trọng tiền gửi không kỳ hạn (CASA) linh hoạt để bảo vệ NIM."
        else:
            impact = "Mặt bằng lãi suất ổn định kết hợp dòng vốn tiền gửi dồi dào củng cố thanh khoản và mở rộng dư địa tăng trưởng tín dụng."
            arg = f"{mb_entity_name} phát huy thế mạnh nền tảng số để thu hút dòng tiền gửi nhàn rỗi và đẩy mạnh cho vay khách hàng chuẩn."

    # 3. Lừa đảo / Gian lận / Giả mạo / Thanh tra / Pháp lý
    elif any(k in text for k in ["lừa đảo", "giả mạo", "khởi tố", "chiếm đoạt", "thao túng", "thanh tra", "vi phạm", "xử phạt", "phạt", "bộ công an", "cảnh sát", "fraud", "scam", "probe"]):
        if typ == "RUI_RO":
            impact = "Rủi ro gian lận, tội phạm công nghệ cao và hành vi vi phạm đe dọa an toàn tài sản của khách hàng và uy tín tổ chức tín dụng."
            arg = f"Khuyến nghị {mb_entity_name} siết chặt quy trình xác thực sinh trắc học, đẩy mạnh cảnh báo rủi ro trên App và tăng cường kiểm soát gian lận."
        else:
            impact = "Công tác thanh tra, chuẩn hóa kỷ cương pháp lý giúp lành mạnh hóa thị trường và bảo vệ các định chế tài chính uy tín."
            arg = f"{mb_entity_name} tận dụng lợi thế quản trị chuẩn mực và thương hiệu an toàn để gia tăng niềm tin và thu hút khách hàng mới."

    # 4. Hàng hóa / Vàng / Dầu thô / Năng lượng / Lạm phát
    elif any(k in text for k in ["vàng", "giá vàng", "gold", "dầu", "dầu thô", "oil", "năng lượng", "lạm phát", "cpi", "inflation"]):
        if typ == "RUI_RO":
            impact = "Biến động mạnh của thị trường hàng hóa/kim loại quý kích hoạt tâm lý đầu cơ, tiềm ẩn áp lực dịch chuyển dòng tiền khỏi hệ thống ngân hàng."
            arg = f"Khuyến nghị {mb_entity_name} theo dõi sát diễn biến thanh khoản liên ngân hàng, chủ động các gói tiền gửi cạnh tranh để giữ chân dòng vốn."
        else:
            impact = "Giá hàng hóa và lạm phát hạ nhiệt giúp ổn định kinh tế vĩ mô và giảm bớt áp lực điều hành chính sách tiền tệ."
            arg = f"{mb_entity_name} nắm bắt cơ hội cung cấp các giải pháp quản lý thanh khoản và tài trợ vốn lưu động cho doanh nghiệp sản xuất."

    # 5. Bất động sản / Trái phiếu doanh nghiệp / Nợ xấu / Xử lý nợ (MB AMC)
    elif any(k in text for k in ["bất động sản", "nhà đất", "trái phiếu", "xử lý nợ", "nợ xấu", "đấu giá", "tài sản bảo đảm", "chung cư", "dự án", "bond", "real estate", "debt"]):
        if typ == "RUI_RO":
            impact = "Khó khăn thanh khoản thị trường tài sản bảo đảm làm chậm tiến độ xử lý nợ và gia tăng áp lực trích lập dự phòng rủi ro."
            arg = f"Khuyến nghị {mb_entity_name} rà soát chặt chẽ giá trị định giá tài sản bảo đảm, đẩy nhanh tiến độ xử lý và cơ cấu nợ theo quy định."
        else:
            impact = "Các chính sách tháo gỡ pháp lý và sự phục hồi của thị trường bất động sản tạo điều kiện thuận lợi cho công tác thanh lý tài sản."
            arg = f"{mb_entity_name} đẩy mạnh xúc tiến giao dịch tài sản thanh lý, tư vấn giải pháp tái cơ cấu nợ cho các đối tác tiềm năng."

    # 6. Chứng khoán / Cổ phiếu / VN-Index / Margin (MBS)
    elif any(k in text for k in ["chứng khoán", "cổ phiếu", "vnindex", "hose", "margin", "tự doanh", "môi giới", "ftse", "stock", "stocks"]):
        if typ == "RUI_RO":
            impact = "Biến động giảm mạnh của thị trường chứng khoán gia tăng rủi ro cho vay Margin và ảnh hưởng tới danh mục tự doanh."
            arg = f"Khuyến nghị {mb_entity_name} kiểm soát chặt tỷ lệ cho vay ký quỹ, theo dõi sát các mã có thanh khoản thấp và kích hoạt quản trị rủi ro tự động."
        else:
            impact = "Thanh khoản thị trường bứt phá và kỳ vọng nâng hạng tạo động lực tăng trưởng mạnh mẽ cho phí môi giới và dịch vụ ngân hàng đầu tư."
            arg = f"{mb_entity_name} đẩy mạnh mở rộng tệp nhà đầu tư cá nhân trên App, tối ưu giải pháp margin thông minh và phát triển sản phẩm quản lý quỹ."

    # 7. Tài chính tiêu dùng / Thẻ tín dụng / Vay tiêu dùng (Mcredit)
    elif any(k in text for k in ["vay tiêu dùng", "tiêu dùng", "trả góp", "thẻ tín dụng", "nợ quá hạn", "consumer"]):
        if typ == "RUI_RO":
            impact = "Khả năng trả nợ của phân khúc khách hàng cá nhân suy giảm có thể làm tăng tỷ lệ nợ quá hạn và chi phí thu hồi nợ."
            arg = f"Khuyến nghị {mb_entity_name} thắt chặt tiêu chuẩn thẩm định tín dụng số, tập trung cho vay đối với nhóm khách hàng trả lương qua ngân hàng."
        else:
            impact = "Nhu cầu tiêu dùng số tăng trưởng mở ra cơ hội mở rộng quy mô tín dụng bán lẻ an toàn với tỷ suất sinh lời hấp dẫn."
            arg = f"{mb_entity_name} đẩy mạnh tích hợp sản phẩm vay tiêu dùng/thẻ tín dụng tức thì trên hệ sinh thái số, nâng cao trải nghiệm khách hàng."

    # 8. Bảo hiểm / Bancassurance (MB Ageas Life / MIC)
    elif any(k in text for k in ["bảo hiểm", "bancassurance", "nhân thọ", "phi nhân thọ", "insurance"]):
        if typ == "RUI_RO":
            impact = "Quy định siết chặt hoạt động phân phối bảo hiểm đòi hỏi nâng cao tiêu chuẩn tuân thủ và gia tăng chi phí đào tạo, giám sát."
            arg = f"Khuyến nghị {mb_entity_name} chuẩn hóa 100% quy trình tư vấn độc lập, tăng cường hậu kiểm âm thanh/hình ảnh tại các điểm bán."
        else:
            impact = "Nhu cầu tích lũy tài chính và bảo vệ sức khỏe dài hạn mở ra dư địa tăng trưởng doanh thu phí bảo hiểm thuần (NFI)."
            arg = f"{mb_entity_name} đẩy mạnh số hóa hành trình mua và giải quyết quyền lợi bảo hiểm, phát triển các gói sản phẩm liên kết linh hoạt."

    # 9. Chuyển đổi số / AI / BIZ MBBank
    elif any(k in text for k in ["chuyển đổi số", "công nghệ", "ai", "số hóa", "biz", "app", "digital"]):
        impact = "Xu hướng chuyển đổi số và tự động hóa công nghệ tạo lợi thế cạnh tranh vượt trội về tốc độ xử lý và tối ưu chi phí vận hành (CIR)."
        arg = f"{mb_entity_name} tiếp tục tiên phong tích hợp AI trong chấm điểm tín dụng và hành trình tài chính số, mở rộng quy mô khách hàng BIZ."

    # 10. Mặc định theo Lĩnh vực & Cấp độ
    else:
        sec_name = SECTOR_MAP.get(sector, {}).get("name", "Tài chính")
        if typ == "RUI_RO":
            impact = f"Tín hiệu biến động vĩ mô cấp {lvl.lower()} tiềm ẩn rủi ro tác động gián tiếp lên biên lợi nhuận và thanh khoản ngành {sec_name}."
            arg = f"Khuyến nghị {mb_entity_name} chủ động theo dõi sát diễn biến thị trường và linh hoạt điều chỉnh kịch bản quản trị thanh khoản."
        else:
            impact = f"Tín hiệu thị trường tích cực cấp {lvl.lower()} mở ra tiềm năng mở rộng quy mô và củng cố vị thế cho mảng {sec_name}."
            arg = f"{mb_entity_name} xem xét tận dụng cơ hội để gia tăng thị phần và nâng cao hiệu quả sử dụng nguồn vốn."

    return f"• Đánh giá tác động: {impact}<br>• Luận điểm: {arg}"

def analyze_article_rules(title: str, content: str, article_summary: str = "") -> dict:
    full_text = f"{title} {content}".lower()

    # 1. Nhận diện đơn vị MB Group nếu có
    mb_entity = "MB"
    mb_entity_name = "MB (MBBank)"
    sector = "NGAN_HANG"
    is_mb = False

    if any(k in full_text for k in ["mcredit", "tài chính tiêu dùng mb", "mb shinsei"]):
        mb_entity = "MCREDIT"
        mb_entity_name = "Mcredit"
        sector = "TC_TIEU_DUNG"
        is_mb = True
    elif any(k in full_text for k in ["mblife", "mb ageas", "bảo hiểm mb"]):
        mb_entity = "MBLIFE"
        mb_entity_name = "MB Ageas Life"
        sector = "BAO_HIEM"
        is_mb = True
    elif any(k in full_text for k in ["mbs", "chứng khoán mb"]):
        mb_entity = "MBS"
        mb_entity_name = "MBS"
        sector = "CHUNG_KHOAN"
        is_mb = True
    elif any(k in full_text for k in ["mbamc", "quản lý tài sản mb", "xử lý nợ mb"]):
        mb_entity = "MBAMC"
        mb_entity_name = "MB AMC"
        sector = "DAU_TU"
        is_mb = True
    elif any(k in full_text for k in ["mbbank", "mbb", "ngân hàng quân đội"]):
        mb_entity = "MB"
        mb_entity_name = "MB (MBBank)"
        sector = "NGAN_HANG"
        is_mb = True

    # 2. Nhận diện ngành nếu không trực tiếp nhắc MB
    if not is_mb:
        if any(k in full_text for k in ["chứng khoán", "cổ phiếu", "vnindex", "hose", "ftse", "stock", "stocks", "etf", "nasdaq", "dow"]):
            sector = "CHUNG_KHOAN"
            mb_entity = "MBS"
            mb_entity_name = "MBS"
        elif any(k in full_text for k in ["bảo hiểm", "bancassurance", "nhân thọ", "phi nhân thọ", "insurance"]):
            sector = "BAO_HIEM"
            mb_entity = "MBLIFE"
            mb_entity_name = "MB Ageas Life"
        elif any(k in full_text for k in ["vay tiêu dùng", "trả góp", "tín dụng đen", "thẻ tín dụng", "travel booking", "consumer debt"]):
            sector = "TC_TIEU_DUNG"
            mb_entity = "MCREDIT"
            mb_entity_name = "Mcredit"
        elif any(k in full_text for k in ["bất động sản", "trái phiếu", "xử lý nợ", "fdi", "dầu thô", "lạm phát", "giá dầu", "cà phê", "debt", "creditor", "creditors", "seize"]):
            sector = "DAU_TU"
            mb_entity = "MBAMC"
            mb_entity_name = "MB AMC"
        else:
            sector = "NGAN_HANG"
            mb_entity = "MB"
            mb_entity_name = "MB (MBBank)"

    # 3. Phân loại Risk-First
    risk_high_keywords = [
        "tăng lãi suất", "nâng lãi suất", "vỡ nợ", "khởi tố", "lừa đảo", "giả mạo", "nợ xấu", "thao túng",
        "bán tháo", "mất giá", "chiếm đoạt", "đình chỉ", "đóng băng", "khủng hoảng", "tỷ giá tăng", "lập đỉnh",
        "đột nhập", "xâm nhập", "tấn công mạng", "lỗ hổng", "hacker", "rò rỉ dữ liệu", "lộ lọt dữ liệu",
        "đánh cắp dữ liệu", "mã độc", "tống tiền", "ransomware", "chiếm quyền", "sập hệ thống", "gián đoạn hệ thống",
        "rate hike", "rates high", "high yield", "yield reaches", "default", "seize", "seizes", "crackdown",
        "probe", "probes", "debt under pressure", "will not be paid", "insolvency", "bankruptcy", "crisis",
        "cyber attack", "cyberattack", "data breach", "ransomware", "exploit", "malware", "zero-day",
        "hacked", "ddos", "security breach", "data leak", "threat actor", "infiltrate", "infiltrated", "unauthorized access"
    ]
    risk_tb_keywords = [
        "áp lực", "sức ép", "thắt chặt", "suy giảm", "giảm", "lao dốc", "bốc hơi", "rủi ro", "cảnh báo",
        "nợ", "nợ vay", "chậm trả", "siết", "thanh tra", "kiểm tra", "xử phạt", "phạt", "thuế nhập khẩu",
        "khó khăn", "thách thức", "thiệt hại", "tổn thất", "chậm trễ", "giảm điểm", "lạm phát", "giá giảm",
        "cảnh báo bảo mật", "lỗi hệ thống", "lỗi phần mềm", "nguy cơ an ninh", "sự cố công nghệ", "rủi ro ai",
        "pressure", "debt", "slowdown", "threaten", "threatens", "threat", "halt", "halts", "slump",
        "fall", "falls", "drop", "drops", "risk", "risks", "warning", "warn", "warns", "creditors",
        "unpaid", "layoff", "layoffs", "loss", "losses", "deficit", "uncertainty",
        "security flaw", "vulnerability", "system outage", "tech glitch", "outage", "ai risk"
    ]
    opp_high_keywords = [
        "kỷ lục", "bứt phá", "nâng hạng", "hạ lãi suất", "giảm lãi suất", "lợi nhuận kỷ lục", "lãi lớn",
        "vượt đỉnh", "thịnh vượng", "thị trường mới nổi",
        "record", "surge", "surges", "boom", "upgrade", "rate cut", "massive profit", "soar", "soars"
    ]
    opp_tb_keywords = [
        "tăng trưởng", "nới lỏng", "lợi nhuận", "cơ hội", "số hóa", "mở rộng", "hợp tác", "đầu tư",
        "mua ròng", "hỗ trợ", "ưu đãi", "miễn phí", "tích cực", "khởi sắc", "hồi phục", "phục hồi",
        "growth", "profit", "profits", "easing", "rally", "gain", "gains", "partnership", "expansion",
        "recovery", "opportunity"
    ]

    has_risk_high = any(k in full_text for k in risk_high_keywords)
    has_risk_tb = any(k in full_text for k in risk_tb_keywords)
    has_opp_high = any(k in full_text for k in opp_high_keywords)
    has_opp_tb = any(k in full_text for k in opp_tb_keywords)

    if has_risk_high:
        typ = "RUI_RO"
        lvl = "CAO"
        score = 88 if any(k in full_text for k in [
            "tăng lãi suất", "rate hike", "lập đỉnh", "vỡ nợ", "seize",
            "đột nhập", "xâm nhập", "tấn công mạng", "cyber attack", "data breach", "ransomware"
        ]) else 85
    elif has_risk_tb:
        typ = "RUI_RO"
        lvl = "TRUNG_BINH"
        score = 78 if any(k in full_text for k in ["siết", "kiểm tra", "thanh tra", "slowdown", "halts", "cảnh báo bảo mật", "lỗ hổng bảo mật"]) else 75
    elif has_opp_high:
        typ = "CO_HOI"
        lvl = "CAO"
        score = 92
    elif has_opp_tb:
        typ = "CO_HOI"
        lvl = "TRUNG_BINH"
        score = 78
    else:
        typ = "CO_HOI"
        lvl = "TRUNG_BINH"
        score = 72

    if article_summary and len(article_summary.strip()) >= 15:
        summary = article_summary.strip()
    else:
        summary = title

    evaluation = generate_dynamic_evaluation(title, content, sector, mb_entity_name, typ, lvl)

    return {
        "sector": sector,
        "sectorName": SECTOR_MAP[sector]["name"],
        "mbGroupCategory": sector,
        "mbEntity": mb_entity,
        "mbEntityName": mb_entity_name,
        "type": typ,
        "level": lvl,
        "impactScore": score,
        "summary": summary,
        "evaluation": evaluation
    }

async def run_pipeline():
    now_vn = get_vietnam_time()
    today_str = now_vn.strftime("%d/%m/%Y")
    now_display = now_vn.strftime("%d/%m/%Y - %H:%M")
    print(f"==================================================")
    print(f"🚀 FININTEL AI DAILY PIPELINE STARTED: {now_display}")
    print(f"==================================================")

    # 1. Quét tin tức thời gian thực từ 12 nguồn
    print(f"📡 [1/5] Đang quét tin tức từ 12 nguồn báo chí tài chính...")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    raw_articles = []

    async with httpx.AsyncClient(headers=headers, timeout=12.0) as client:
        tasks = [fetch_rss_feed(client, feed) for feed in RSS_FEEDS]
        feed_results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in feed_results:
            if isinstance(res, list):
                raw_articles.extend(res)

    print(f"✅ Thu thập thành công {len(raw_articles)} tin tức mới.")

    # 2. Phân loại Risk-First
    print(f"🤖 [2/5] Đang phân tích tin tức và phân loại 5 lĩnh vực...")
    analyzed_items = []
    seen_urls = set()

    for item in raw_articles:
        url = item.get("url", "").strip()
        title = item.get("title", "").strip()
        if not url or not title or url in seen_urls:
            continue
        seen_urls.add(url)

        analysis = analyze_article_rules(title, item.get("content", ""), item.get("summary", ""))

        analyzed_items.append({
            "title": title,
            "url": url,
            "source": item.get("source", "news"),
            "sourceName": item.get("sourceName", "Báo chí"),
            "source_name": item.get("sourceName", "Báo chí"),
            "sourceGroup": item.get("sourceGroup", "DOMESTIC"),
            "source_group": item.get("sourceGroup", "DOMESTIC"),
            "time": item.get("published_at", now_vn).strftime("%H:%M %d/%m"),
            "sector": analysis["sector"],
            "sectorName": analysis["sectorName"],
            "mbGroupCategory": analysis["mbGroupCategory"],
            "mbEntity": analysis["mbEntity"],
            "mbEntityName": analysis["mbEntityName"],
            "type": analysis["type"],
            "level": analysis["level"],
            "impactScore": analysis["impactScore"],
            "frequency": 3500 + int(analysis["impactScore"]) * 15,
            "summary": analysis["summary"],
            "evaluation": analysis["evaluation"],
            "tickers": ["MBB"]
        })

    # 3. Phân tách và cân bằng độc lập 2 nhóm nguồn: Trong Nước (DOMESTIC) & Quốc Tế (INTERNATIONAL)
    print(f"📊 [3/5] Đang cân bằng song song Tin Trong Nước & Tin Quốc Tế theo nguyên tắc Risk-First...")
    sort_key = lambda x: (1 if x.get("level") == "CAO" else 0, int(x.get("impactScore", 0)))

    # A. Nhóm Tin Trong Nước (DOMESTIC): Tối đa 20 Rủi ro + 10 Cơ hội
    dom_items = [it for it in analyzed_items if it.get("sourceGroup") == "DOMESTIC"]
    dom_risks = [it for it in dom_items if it["type"] == "RUI_RO"]
    dom_opps = [it for it in dom_items if it["type"] == "CO_HOI"]
    dom_risks.sort(key=sort_key, reverse=True)
    dom_opps.sort(key=sort_key, reverse=True)
    dashboard_dom_risks = dom_risks[:20]
    dashboard_dom_opps = dom_opps[:10]
    selected_domestic = dashboard_dom_risks + dashboard_dom_opps

    # B. Nhóm Tin Quốc Tế (INTERNATIONAL): Bảo đảm luôn được chọn lọc riêng, không bị tin trong nước lấn át
    intl_items = [it for it in analyzed_items if it.get("sourceGroup") in ("INTERNATIONAL", "GLOBAL")]
    intl_risks = [it for it in intl_items if it["type"] == "RUI_RO"]
    intl_opps = [it for it in intl_items if it["type"] == "CO_HOI"]
    intl_risks.sort(key=sort_key, reverse=True)
    intl_opps.sort(key=sort_key, reverse=True)
    dashboard_intl_risks = intl_risks[:10]
    dashboard_intl_opps = intl_opps[:6]
    selected_intl = dashboard_intl_risks + dashboard_intl_opps

    # C. Hợp nhất vào Dashboard
    selected_dashboard = []
    idx = 1
    for item in selected_domestic + selected_intl:
        item["id"] = idx
        idx += 1
        selected_dashboard.append(item)

    print(f"✅ Dashboard đã chọn {len(selected_dashboard)} bài viết:")
    print(f"   🇻🇳 Trong Nước: {len(selected_domestic)} tin ({len(dashboard_dom_risks)} Rủi ro | {len(dashboard_dom_opps)} Cơ hội)")
    print(f"   🌐 Quốc Tế: {len(selected_intl)} tin ({len(dashboard_intl_risks)} Rủi ro | {len(dashboard_intl_opps)} Cơ hội)")

    # 2. Lọc dữ liệu gửi Teams: Cân bằng đại diện cả Trong Nước và Quốc Tế
    teams_risks = (dom_risks[:7] + intl_risks[:3])
    teams_risks.sort(key=sort_key, reverse=True)
    teams_opps = (dom_opps[:3] + intl_opps[:2])
    teams_opps.sort(key=sort_key, reverse=True)
    print(f"📢 Teams: Đã chọn {len(teams_risks)} Rủi ro cao nhất ({sum(1 for it in teams_risks if it.get('sourceGroup')=='DOMESTIC')} Trong nước, {sum(1 for it in teams_risks if it.get('sourceGroup')!='DOMESTIC')} Quốc tế) | {len(teams_opps)} Cơ hội tốt nhất.")

    # 4. Cập nhật Dashboard index.html
    print(f"📝 [4/5] Đang cập nhật Dashboard index.html...")
    target_html = os.path.join(BASE_DIR, "index.html")
    if os.path.exists(target_html):
        with open(target_html, "r", encoding="utf-8") as f:
            html_text = f.read()

        html_text = re.sub(r'Cập nhật: \d{2}/\d{2}/\d{4} - \d{2}:\d{2}', f'Cập nhật: {now_display}', html_text)
        html_text = re.sub(r'Cập nhật:\s*</span>\s*<strong id="lastUpdated">[^<]*</strong>', f'Cập nhật:</span> <strong id="lastUpdated">{now_display}</strong>', html_text)

        # Tích lũy dữ liệu 30 ngày (Rolling 30-Day Window)
        existing_articles = []
        marker_start = "const REAL_ARTICLES = ["
        marker_end = "];"
        idx_start = html_text.find(marker_start)
        if idx_start != -1:
            idx_end = html_text.find(marker_end, idx_start)
            if idx_end != -1:
                raw_json_str = html_text[idx_start + len(marker_start) - 1:idx_end + 1]
                try:
                    existing_articles = json.loads(raw_json_str)
                except Exception:
                    existing_articles = []

        seen_keys = {it.get("url", "").strip() for it in selected_dashboard if it.get("url")}
        seen_keys.update({it.get("title", "").strip().lower() for it in selected_dashboard if it.get("title")})

        merged_articles = list(selected_dashboard)
        for old_it in existing_articles:
            u = old_it.get("url", "").strip()
            t = old_it.get("title", "").strip().lower()
            if u and t and u not in seen_keys and t not in seen_keys:
                seen_keys.add(u)
                seen_keys.add(t)
                merged_articles.append(old_it)

        # Giữ tối đa 500 bài viết trong phạm vi 30 ngày gần nhất (đáp ứng tích lũy liên tục cả tháng)
        cutoff_date = now_vn - timedelta(days=30)
        filtered_by_date = []
        for it in merged_articles:
            t_str = it.get("time", "")
            try:
                parts = t_str.strip().split()
                if len(parts) >= 2:
                    d_parts = parts[1].split("/")
                    day = int(d_parts[0])
                    month = int(d_parts[1])
                    year = now_vn.year if month <= now_vn.month else now_vn.year - 1
                    art_dt = datetime(year, month, day, tzinfo=VN_TZ)
                    if art_dt < cutoff_date:
                        continue
            except Exception:
                pass
            filtered_by_date.append(it)

        merged_articles = filtered_by_date[:500]
        for i, it in enumerate(merged_articles, 1):
            it["id"] = i

        articles_json = json.dumps(merged_articles, ensure_ascii=False, indent=2)
        if idx_start != -1 and idx_end != -1:
            html_text = (
                html_text[:idx_start + len("const REAL_ARTICLES = ")]
                + articles_json
                + html_text[idx_end + len(marker_end) - 1:]
            )
            with open(target_html, "w", encoding="utf-8") as f:
                f.write(html_text)
            print(f"✅ Đã cập nhật thành công REAL_ARTICLES tích lũy trong index.html với tổng cộng {len(merged_articles)} tin!")

        static_html = os.path.join(BASE_DIR, "static", "index.html")
        if os.path.exists(static_html):
            with open(static_html, "w", encoding="utf-8") as f:
                f.write(html_text)

    # 5. Gửi thẻ Microsoft Teams
    print(f"📢 [5/5] Đang tạo và gửi thẻ Morning Briefing vào Microsoft Teams...")
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "")
    if not webhook_url:
        print("⚠️ Không tìm thấy TEAMS_WEBHOOK_URL trong environment, bỏ qua gửi Teams.")
        return

    today_str = now_vn.strftime("%d/%m")
    today_articles = [it for it in merged_articles if today_str in it.get("time", "")]
    articles_for_matrix = today_articles if today_articles else selected_dashboard

    matrix_counts = {k: {"r_cao": 0, "r_tb": 0, "c_cao": 0, "c_tb": 0, "total": 0} for k in SECTOR_MAP}
    for it in articles_for_matrix:
        sec = it.get("sector", "NGAN_HANG")
        if sec not in matrix_counts:
            sec = "NGAN_HANG"
        matrix_counts[sec]["total"] += 1
        is_risk = it.get("type") == "RUI_RO"
        is_cao = it.get("level") == "CAO"
        if is_risk:
            if is_cao: matrix_counts[sec]["r_cao"] += 1
            else: matrix_counts[sec]["r_tb"] += 1
        else:
            if is_cao: matrix_counts[sec]["c_cao"] += 1
            else: matrix_counts[sec]["c_tb"] += 1

    def make_matrix_row(c1, c2, c3, c4, is_header=False, is_total=False, c2_color="Attention", c3_color="Good"):
        weight = "Bolder" if (is_header or is_total) else "Default"
        row = {
            "type": "ColumnSet",
            "spacing": "Small",
            "separator": True,
            "columns": [
                {"type": "Column", "width": 38, "items": [{"type": "TextBlock", "text": c1, "weight": weight, "wrap": True}]},
                {"type": "Column", "width": 28, "items": [{"type": "TextBlock", "text": c2, "weight": weight, "color": c2_color if (c2 != "0 tin" and not is_header) else ("Attention" if is_header else "Default")}]},
                {"type": "Column", "width": 24, "items": [{"type": "TextBlock", "text": c3, "weight": weight, "color": c3_color if (c3 != "0 tin" and not is_header) else ("Good" if is_header else "Default")}]},
                {"type": "Column", "width": 10, "items": [{"type": "TextBlock", "text": str(c4), "weight": weight, "horizontalAlignment": "Right"}]}
            ]
        }
        if is_header or is_total:
            row["style"] = "emphasis"
        return row

    def format_cell(cao, tb, label="tin"):
        parts = []
        if cao > 0: parts.append(f"{cao} Cao")
        if tb > 0: parts.append(f"{tb} TB")
        tot = cao + tb
        if tot == 0: return "0 tin"
        return f"{tot} {label} ({', '.join(parts)})"

    matrix_rows = [
        {"type": "TextBlock", "text": "📊 MA TRẬN TÍN HIỆU RỦI RO", "weight": "Bolder", "color": "Accent", "size": "Medium"},
        make_matrix_row("Lĩnh vực", "🔴 Rủi ro", "🟢 Cơ hội", "Tổng", is_header=True)
    ]

    for k, v in SECTOR_MAP.items():
        stats = matrix_counts[k]
        r_text = format_cell(stats["r_cao"], stats["r_tb"])
        c_text = format_cell(stats["c_cao"], stats["c_tb"])
        matrix_rows.append(make_matrix_row(v["name"], r_text, c_text, stats["total"]))

    tot_r = sum(stats["r_cao"] + stats["r_tb"] for stats in matrix_counts.values())
    tot_c = sum(stats["c_cao"] + stats["c_tb"] for stats in matrix_counts.values())
    tot_all = len(articles_for_matrix)
    tot_dom = sum(1 for it in articles_for_matrix if it.get("sourceGroup") == "DOMESTIC")
    tot_intl = sum(1 for it in articles_for_matrix if it.get("sourceGroup") in ("INTERNATIONAL", "GLOBAL"))
    matrix_rows.append(make_matrix_row(
        "TỔNG CỘNG HỆ THỐNG",
        f"{tot_r} TIN ({round(tot_r/tot_all*100) if tot_all else 0}%)",
        f"{tot_c} TIN ({round(tot_c/tot_all*100) if tot_all else 0}%)",
        f"{tot_all} TIN",
        is_total=True
    ))
    matrix_rows.append({
        "type": "TextBlock",
        "text": f"*(Cơ cấu nguồn: 🇻🇳 **{tot_dom}** Trong Nước • 🌐 **{tot_intl}** Quốc Tế)*",
        "isSubtle": True,
        "size": "Small",
        "spacing": "None"
    })

    risk_blocks = [
        {"type": "TextBlock", "text": f"🚨 TOP {len(teams_risks)} CẢNH BÁO RỦI RO CAO NHẤT", "weight": "Bolder", "color": "Attention", "size": "Medium"}
    ]
    for i, r in enumerate(teams_risks, 1):
        icon = "🔴" if r.get("level") == "CAO" else "🟠"
        lvl_text = "RỦI RO CAO" if r.get("level") == "CAO" else "RỦI RO TRUNG BÌNH"
        src_name = r.get("sourceName") or r.get("source_name") or "Báo chí"
        mb_ent = r.get("mbEntityName") or r.get("mb_entity_name") or "MB (MBBank)"
        summary = r.get("summary") or r.get("ai_summary") or ""
        r_title = (r.get("title") or "").replace("\n", " ").strip()
        r_url = (r.get("url") or "").strip()
        r_score = r.get("impactScore", 75)
        risk_blocks.append({
            "type": "TextBlock",
            "text": f"{i}. {icon} **[{lvl_text}]** [{r_title}]({r_url})  \n   *• Đơn vị:* **{mb_ent}** | *Tác động:* **{r_score}/100** | *Nguồn:* {src_name}  \n   *• Nội dung tóm tắt:* {summary}",
            "wrap": True,
            "spacing": "Small"
        })

    opp_blocks = [
        {"type": "TextBlock", "text": f"🎯 TOP {len(teams_opps)} CƠ HỘI TỐT NHẤT", "weight": "Bolder", "color": "Good", "size": "Medium"}
    ]
    for r in teams_opps:
        mb_ent = r.get("mbEntityName") or r.get("mb_entity_name") or "MB (MBBank)"
        summary = r.get("summary") or r.get("ai_summary") or ""
        o_title = (r.get("title") or "").replace("\n", " ").strip()
        o_url = (r.get("url") or "").strip()
        o_score = r.get("impactScore", 85)
        opp_blocks.append({
            "type": "TextBlock",
            "text": f"• 🟢 **[{o_score}/100]** [{o_title}]({o_url}) — *Đơn vị:* **{mb_ent}**  \n  *• Nội dung tóm tắt:* {summary}",
            "wrap": True,
            "spacing": "Small"
        })

    dashboard_url = "https://mb-riskradar.vercel.app"

    card_payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "msteams": {"width": "Full"},
                "body": [
                    {
                        "type": "Container",
                        "style": "emphasis",
                        "bleed": True,
                        "items": [
                            {"type": "TextBlock", "text": "NGÂN HÀNG TMCP QUÂN ĐỘI (MB) • FININTEL AI", "weight": "Bolder", "color": "Accent"},
                            {"type": "TextBlock", "text": "BẢN TIN ĐIỀU HÀNH TÍN HIỆU KINH TẾ & TÀI CHÍNH", "size": "Large", "weight": "Bolder", "spacing": "Small"},
                            {"type": "TextBlock", "text": f"Hệ sinh thái MB Group • Cập nhật tự động: {now_vn.strftime('%H:%M')} ngày {today_str}", "isSubtle": True, "spacing": "None"}
                        ]
                    },
                    {"type": "Container", "spacing": "Medium", "items": matrix_rows},
                    {"type": "Container", "spacing": "Medium", "items": risk_blocks},
                    {"type": "Container", "spacing": "Medium", "items": opp_blocks}
                ],
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "🌐 Mở Dashboard bản tin hằng ngày",
                        "style": "positive",
                        "url": dashboard_url
                    }
                ]
            }
        }]
    }

    resp = requests.post(webhook_url, json=card_payload, headers={"Content-Type": "application/json"}, timeout=15)
    print(f"📢 Kết quả gửi Teams: Status {resp.status_code}")
    if resp.status_code in [200, 202]:
        print("🎉 Gửi bản tin Morning Briefing thành công!")
    else:
        print(f"⚠️ Chi tiết lỗi: {resp.text}")

    print("🏁 PIPELINE COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":
    asyncio.run(run_pipeline())
