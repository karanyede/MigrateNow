import sys
from datetime import datetime

class SimplePDFWriter:
    """
    A pure Python PDF generator designed specifically to build premium-looking
    migration reports without any external dependencies (like fpdf2, reportlab, etc.).
    This ensures it works reliably in any environment.
    """
    def __init__(self):
        self.objects = []
        self.offsets = []

    def _add_object(self, obj_str):
        self.objects.append(obj_str.encode('utf-8') if isinstance(obj_str, str) else obj_str)
        return len(self.objects)

    def generate(self, r: dict) -> bytes:
        # Define objects
        # 1: Catalog
        # 2: Page List (Pages)
        # 3: Helvetica Font
        # 4: Page 1 Description
        # 5: Page Content Stream
        
        # We need to construct the stream first to know its length.
        stream_content = self._build_stream_content(r)
        
        catalog_id = 1
        pages_id = 2
        font_id = 3
        page_id = 4
        stream_id = 5
        
        # Object 1: Catalog
        self._add_object(f"{catalog_id} 0 obj\n<< /Type /Catalog /Pages {pages_id} 0 R >>\nendobj")
        
        # Object 2: Pages
        self._add_object(f"{pages_id} 0 obj\n<< /Type /Pages /Kids [ {page_id} 0 R ] /Count 1 >>\nendobj")
        
        # Object 3: Font
        self._add_object(f"{font_id} 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /MacRomanEncoding >>\nendobj")
        
        # Object 4: Page (refers to stream and resources)
        self._add_object(
            f"{page_id} 0 obj\n"
            f"<< /Type /Page\n"
            f"   /Parent {pages_id} 0 R\n"
            f"   /Resources << /Font << /F1 {font_id} 0 R >> >>\n"
            f"   /MediaBox [ 0 0 595 842 ]\n" # A4 dimensions
            f"   /Contents {stream_id} 0 R\n"
            f">>\n"
            f"endobj"
        )

        # Object 5: Content Stream
        stream_len = len(stream_content)
        stream_obj = (
            f"{stream_id} 0 obj\n"
            f"<< /Length {stream_len} >>\n"
            f"stream\n"
        ).encode('utf-8') + stream_content + b"\nendstream\nendobj"
        self._add_object(stream_obj)

        # Assemble PDF file structure with offset calculations for cross-reference table
        pdf = bytearray()
        pdf.extend(b"%PDF-1.4\n")
        
        offsets = []
        for obj in self.objects:
            offsets.append(len(pdf))
            pdf.extend(obj)
            pdf.extend(b"\n")
            
        xref_offset = len(pdf)
        pdf.extend(f"xref\n0 {len(self.objects) + 1}\n".encode('utf-8'))
        pdf.extend(b"0000000000 65535 f \n")
        for offset in offsets:
            pdf.extend(f"{offset:010d} 00000 n \n".encode('utf-8'))
            
        pdf.extend(
            f"trailer\n"
            f"<< /Size {len(self.objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n"
            f"{xref_offset}\n"
            f"%%EOF\n".encode('utf-8')
        )
        
        return bytes(pdf)

    def _build_stream_content(self, r: dict) -> bytes:
        cmds = []
        
        # Helper: draw a rectangle (filled or stroked)
        def draw_rect(x, y, w, h, fill_color=None, stroke_color=None):
            s = []
            if fill_color:
                s.append(f"{fill_color[0]/255:.3f} {fill_color[1]/255:.3f} {fill_color[2]/255:.3f} rg")
            if stroke_color:
                s.append(f"{stroke_color[0]/255:.3f} {stroke_color[1]/255:.3f} {stroke_color[2]/255:.3f} RG")
            s.append(f"{x} {y} {w} {h} re")
            if fill_color and stroke_color:
                s.append("B")
            elif fill_color:
                s.append("f")
            else:
                s.append("S")
            return " ".join(s)

        # Helper: draw text
        def draw_text(text, x, y, size=10, color=(31,41,55), is_bold=False):
            # (Bold is simulated by increasing text size or stroke in pure Type1, 
            # here we just render clean standard text at defined size)
            escaped = str(text).replace("(", "\\(").replace(")", "\\)")
            s = [
                "BT",
                f"/F1 {size} Tf",
                f"{color[0]/255:.3f} {color[1]/255:.3f} {color[2]/255:.3f} rg",
                f"{x} {y} Td",
                f"({escaped}) Tj",
                "ET"
            ]
            return " ".join(s)

        # Helper: draw line
        def draw_line(x1, y1, x2, y2, color=(229,231,235), width=1):
            return (
                f"q {width} w {color[0]/255:.3f} {color[1]/255:.3f} {color[2]/255:.3f} RG "
                f"{x1} {y1} m {x2} {y2} l S Q"
            )

        # ── 1. Page Background & Border ──
        # Off-white background
        cmds.append(draw_rect(0, 0, 595, 842, fill_color=(249, 250, 251)))
        
        # ── 2. Top Banner Header (Premium Dark Navy Theme) ──
        cmds.append(draw_rect(0, 742, 595, 100, fill_color=(15, 23, 42)))
        # Accent Cyan line
        cmds.append(draw_rect(0, 738, 595, 4, fill_color=(6, 182, 212)))
        
        cmds.append(draw_text("MigrateNow Migration Report", 40, 792, size=22, color=(255, 255, 255)))
        date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cmds.append(draw_text(f"Generated At: {date_str}", 40, 762, size=9, color=(148, 163, 184)))
        
        # Migration Type Badge
        mt_text = str(r.get("migration_type", "sn_sn")).replace("_", " -> ").upper()
        cmds.append(draw_rect(440, 775, 115, 24, fill_color=(6, 182, 212)))
        cmds.append(draw_text(mt_text, 448, 783, size=9, color=(255, 255, 255)))

        # ── 3. Configuration Details Section ──
        cmds.append(draw_text("Configuration Details", 40, 705, size=12, color=(15, 23, 42)))
        cmds.append(draw_line(40, 698, 555, 698, color=(203, 213, 225), width=1))
        
        config_rows = [
            ("Source Instance:", str(r.get("source_instance", "N/A")), "Source Table:", str(r.get("source_table", "N/A"))),
            ("Target Instance:", str(r.get("target_instance", "N/A")), "Target Table:", str(r.get("target_table", "N/A"))),
            ("Fetch Mode Used:", str(r.get("fetch_mode_used", "AUTO")).upper(), "Total Recs Processed:", f"{r.get('total_source_records', 0):,}")
        ]
        
        y_pos = 675
        for col1_lbl, col1_val, col2_lbl, col2_val in config_rows:
            # Col 1
            cmds.append(draw_text(col1_lbl, 45, y_pos, size=9, color=(100, 116, 139)))
            cmds.append(draw_text(col1_val, 150, y_pos, size=9, color=(30, 41, 59)))
            # Col 2
            cmds.append(draw_text(col2_lbl, 320, y_pos, size=9, color=(100, 116, 139)))
            cmds.append(draw_text(col2_val, 440, y_pos, size=9, color=(30, 41, 59)))
            y_pos -= 20

        # ── 4. Migration Summary Stat Cards (Vibrant Cards Grid) ──
        cmds.append(draw_text("Migration Summary", 40, 600, size=12, color=(15, 23, 42)))
        cmds.append(draw_line(40, 593, 555, 593, color=(203, 213, 225), width=1))
        
        # Grid variables
        card_w = 95
        card_h = 55
        card_x = 40
        card_y = 525
        card_gap = 10
        
        stats = [
            ("FETCHED", f"{r.get('total_source_records', 0):,}", (241, 245, 249), (100, 116, 139)),
            ("INSERTED", f"{r.get('inserts', 0):,}", (240, 253, 244), (22, 163, 74)),
            ("UPDATED", f"{r.get('updates', 0):,}", (239, 246, 255), (37, 99, 235)),
            ("SKIPPED", f"{r.get('skipped', 0):,}", (254, 253, 237), (202, 138, 4)),
            ("FAILED", f"{r.get('failed', 0):,}", (255, 241, 242), (220, 38, 38))
        ]
        
        for lbl, val, bg, fg in stats:
            # Card background
            cmds.append(draw_rect(card_x, card_y, card_w, card_h, fill_color=bg, stroke_color=(226, 232, 240)))
            # Label
            cmds.append(draw_text(lbl, card_x + 8, card_y + 38, size=8, color=(100, 116, 139)))
            # Value
            cmds.append(draw_text(val, card_x + 8, card_y + 14, size=14, color=fg))
            card_x += card_w + card_gap

        # ── 5. Timing Breakdown Table ──
        cmds.append(draw_text("Timing Breakdown", 40, 455, size=12, color=(15, 23, 42)))
        cmds.append(draw_line(40, 448, 275, 448, color=(203, 213, 225), width=1))
        
        # timing may be a full dict (from live report) or absent (history entry only has flat "duration")
        t = r.get("timing") or {}
        flat_dur = r.get("duration")  # fallback: flat number stored in history entries

        def fmt_dur(sec):
            if sec is None or sec == "": return "--"
            try:
                s = float(sec)
            except (TypeError, ValueError):
                return "--"
            if s >= 60:
                return f"{int(s // 60)}m {s % 60:.1f}s"
            return f"{s:.1f}s"

        # If timing dict is missing/empty, fall back to showing total from flat duration
        if t:
            timing_rows = [
                ("Fetch Source",        fmt_dur(t.get("fetch_source"))),
                ("Fetch Target",        fmt_dur(t.get("fetch_target"))),
                ("Diff Compute",        fmt_dur(t.get("diff"))),
                ("Load (Insert+Update)",fmt_dur(t.get("load"))),
                ("Total Duration",      fmt_dur(t.get("total"))),
            ]
        else:
            timing_rows = [
                ("Fetch Source",        "--"),
                ("Fetch Target",        "--"),
                ("Diff Compute",        "--"),
                ("Load (Insert+Update)","--"),
                ("Total Duration",      fmt_dur(flat_dur)),
            ]
        
        # Draw table
        y_pos = 425
        cmds.append(draw_rect(40, y_pos, 235, 20, fill_color=(15, 23, 42)))
        cmds.append(draw_text("Phase", 48, y_pos + 6, size=8, color=(255, 255, 255)))
        cmds.append(draw_text("Duration", 200, y_pos + 6, size=8, color=(255, 255, 255)))
        
        for name, dur in timing_rows:
            y_pos -= 20
            # alternating bg
            bg_color = (248, 250, 252) if y_pos % 40 == 5 else (255, 255, 255)
            cmds.append(draw_rect(40, y_pos, 235, 20, fill_color=bg_color, stroke_color=(241, 245, 249)))
            # Bold for Total Duration row
            c_text = (15, 23, 42) if name == "Total Duration" else (71, 85, 105)
            cmds.append(draw_text(name, 48, y_pos + 6, size=8.5, color=c_text))
            cmds.append(draw_text(dur, 200, y_pos + 6, size=8.5, color=c_text))

        # ── 6. API Call Statistics Table ──
        cmds.append(draw_text("API Call Statistics", 310, 455, size=12, color=(15, 23, 42)))
        cmds.append(draw_line(310, 448, 555, 448, color=(203, 213, 225), width=1))
        
        apis = r.get("api_calls", {})
        src_api = apis.get("source", {})
        tgt_api = apis.get("target", {})
        
        api_rows = [
            ("Source Calls", f"{src_api.get('calls_made', 0):,}", f"Limit: {src_api.get('rate_limit_per_hour', 'N/A')}/hr"),
            ("Target Calls", f"{tgt_api.get('calls_made', 0):,}", f"Limit: {tgt_api.get('rate_limit_per_hour', 'N/A')}/hr"),
            ("Total API Calls", f"{apis.get('total_calls', 0):,}", "")
        ]
        
        y_pos = 425
        cmds.append(draw_rect(310, y_pos, 245, 20, fill_color=(15, 23, 42)))
        cmds.append(draw_text("Category", 318, y_pos + 6, size=8, color=(255, 255, 255)))
        cmds.append(draw_text("Calls", 415, y_pos + 6, size=8, color=(255, 255, 255)))
        cmds.append(draw_text("Details", 465, y_pos + 6, size=8, color=(255, 255, 255)))
        
        for name, calls, details in api_rows:
            y_pos -= 20
            bg_color = (248, 250, 252) if y_pos % 40 == 5 else (255, 255, 255)
            cmds.append(draw_rect(310, y_pos, 245, 20, fill_color=bg_color, stroke_color=(241, 245, 249)))
            c_text = (15, 23, 42) if name == "Total API Calls" else (71, 85, 105)
            cmds.append(draw_text(name, 318, y_pos + 6, size=8.5, color=c_text))
            cmds.append(draw_text(calls, 415, y_pos + 6, size=8.5, color=c_text))
            cmds.append(draw_text(details, 465, y_pos + 6, size=8, color=(100, 116, 139)))

        # ── 7. Footer Accent ──
        cmds.append(draw_line(40, 60, 555, 60, color=(226, 232, 240), width=1))
        cmds.append(draw_text("MigrateNow v2.0 - Enterprise Data Migration Platform", 40, 42, size=8, color=(148, 163, 184)))
        cmds.append(draw_text("CONFIDENTIAL - FOR INTERNAL USE ONLY", 415, 42, size=8, color=(148, 163, 184)))

        # Return byte string representation of all drawing operations
        return "\n".join(cmds).encode('utf-8')
