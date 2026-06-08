"""
Cafe Javas Receipt Generator
Usage: python cafe_javas_receipt.py
"""

from reportlab.pdfgen import canvas
import os

# ─────────────────────────────────────────────
#  CONFIGURATION — edit everything below
# ─────────────────────────────────────────────

OUTPUT_DIR = "receipts"

RECEIPTS = [
    {
        "filename":   "receipt_26Mar2026.pdf",
        "date":       "26-Mar-26",
        "invoice_no": "CJMBO260320260011",
        "server":     "DIANA",
        "pay_method": "Mobile Cash",   # Mobile Money | Cash | Credit Card
        "watermark":  "PAID",         # <-- change this to whatever you want
        "items": [
            # (Description,               Qty,  Unit Price UGX)
            ("Caribbean Jerk Chicken",      1,   44000),
            ("Lemon & Herb Chicken",        1,   44000),
            ("Chicken Wing Meal",           1,   44000),
        ],
    },
    {
        "filename":   "receipt_27Mar2026.pdf",
        "date":       "27-Mar-26",
        "invoice_no": "CJBMB270324460016",
        "server":     "GRACE",
        "pay_method": "Mobile Money",
        "watermark":  "PAID",
        "items": [
            ("Turmeric Chicken",                1, 44000),
            ("Mushroom Grilled Chkn Breast",    1, 45000),
            ("Juicy Grilled Chicken Breast",    1, 44000),
        ],
    },
    {
        "filename":   "receipt_31Mar2026.pdf",
        "date":       "31-Mar-26",
        "invoice_no": "CJKRA310320260013",
        "server":     "SARAH",
        "pay_method": "Mobile Money",
        "watermark":  "PAID",
        "items": [
            ("BBQ Chicken",              1, 44000),
            ("Chicken Tenders Platter",  1, 44000),
        ],
    },
]

# ─────────────────────────────────────────────
#  ENGINE — no need to edit below this line
# ─────────────────────────────────────────────

W = 226        # ~80mm thermal roll
MARGIN = 14


def build_receipt(cfg):
    items      = cfg["items"]
    date_str   = cfg["date"]
    invoice_no = cfg["invoice_no"]
    server     = cfg["server"].upper()
    pay_method = cfg["pay_method"]
    watermark  = cfg["watermark"]

    total    = sum(qty * price for _, qty, price in items)
    vat_excl = round(total / 1.18, 2)
    vat_amt  = round(total - vat_excl, 2)
    H        = 500 + len(items) * 24

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, cfg["filename"])
    c = canvas.Canvas(path, pagesize=(W, H))

    # ── Watermark (always drawn, text is configurable) ──
    c.saveState()
    c.setFont("Helvetica-Bold", 22)
    c.setFillColorRGB(0.85, 0.85, 0.85)
    c.translate(W / 2, H / 2)
    c.rotate(35)
    c.drawCentredString(0, 0, watermark)
    c.restoreState()

    y = H - 20

    def ln(text, size=7.5, center=True, bold=False, gap=13):
        nonlocal y
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.setFillColorRGB(0, 0, 0)
        if center:
            c.drawCentredString(W / 2, y, text)
        else:
            c.drawString(MARGIN, y, text)
        y -= gap

    def rln(left, right, size=7.5, bold=False, gap=12):
        nonlocal y
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.setFillColorRGB(0, 0, 0)
        c.drawString(MARGIN, y, left)
        c.drawRightString(W - MARGIN, y, right)
        y -= gap

    def dash(gap=6):
        nonlocal y
        y -= 2
        c.setLineWidth(0.5)
        c.setDash(2, 2)
        c.line(MARGIN, y, W - MARGIN, y)
        c.setDash()
        y -= gap

    # Header
    ln("SAVERS LIMITED",                     size=10, bold=True, gap=14)
    ln("Cafe Javas - Bombo Road, Kampala",    size=7.2)
    ln("TIN No: 1009927885",                 size=7)
    dash()
    ln("Tax Invoice",                        size=9, bold=True, gap=12)
    ln(f"Invoice No: {invoice_no}",          size=6.8)
    ln(f"Date: {date_str}",                  size=7.2)
    ln("Customer Name/TIN No:",              size=7, gap=11)
    dash()

    # Items table header
    c.setFont("Helvetica-Bold", 7)
    c.drawString(MARGIN,     y, "Description")
    c.drawString(118,        y, "Qty")
    c.drawString(145,        y, "Price")
    c.drawRightString(W - MARGIN, y, "Amount")
    y -= 5
    c.setLineWidth(0.4)
    c.line(MARGIN, y, W - MARGIN, y)
    y -= 11

    for name, qty, price in items:
        c.setFont("Helvetica", 7.2)
        c.drawString(MARGIN,          y, name)
        c.drawString(118,             y, str(qty))
        c.drawString(145,             y, f"{price:,}")
        c.drawRightString(W - MARGIN, y, f"{qty * price:,}")
        y -= 14

    dash()
    rln("TotalAmount",          f"{total:,}",      bold=True)
    rln("CashEntered",          "0")
    rln(f"{pay_method} Amount", f"{total:,.2f}")
    rln("Credit Card Amount",   "0.00")
    rln("Change",               "0")
    y -= 2
    ln(f"You were served by   {server}", size=7, gap=11)
    dash()

    # VAT footer
    c.setFont("Helvetica-Bold", 7)
    c.drawString(MARGIN, y, "VAT")
    c.drawString(52,     y, "Amt W/o VAT")
    c.drawString(122,    y, "VAT Amt")
    c.drawRightString(W - MARGIN, y, "TOTAL Amt")
    y -= 12
    c.setFont("Helvetica", 7)
    c.drawString(MARGIN, y, "Standard")
    c.drawString(52,     y, f"{vat_excl:,.2f}")
    c.drawString(122,    y, f"{vat_amt:,.2f}")
    c.drawRightString(W - MARGIN, y, f"{total:,}")

    c.save()
    print(f"  Created: {path}  |  Total: UGX {total:,}")


if __name__ == "__main__":
    print("\nCafe Javas Receipt Generator\n" + "-" * 35)
    for r in RECEIPTS:
        build_receipt(r)
    print(f"\nDone! Files saved to → ./{OUTPUT_DIR}/\n")