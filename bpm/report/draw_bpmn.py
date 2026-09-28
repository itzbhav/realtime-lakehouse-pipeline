import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Circle, FancyArrowPatch, Polygon

fig, ax = plt.subplots(figsize=(14, 9.5))
ax.set_xlim(0, 11.5)
ax.set_ylim(0, 9.5)
ax.axis("off")

LANE_COLORS = {
    "Customer App": "#eef3ff",
    "Dark Store Ops": "#eefbf0",
    "Rider / Courier": "#fff7e6",
    "Exception Handling": "#fdeaea",
}
LANES = [
    ("Customer App", 7.9, 9.5),
    ("Dark Store Ops", 4.9, 7.9),
    ("Rider / Courier", 1.9, 4.9),
    ("Exception Handling", 0.0, 1.9),
]

for name, y0, y1 in LANES:
    ax.add_patch(plt.Rectangle((0, y0), 11.5, y1 - y0, facecolor=LANE_COLORS[name], edgecolor="#888888", linewidth=1.0, zorder=0))
    ax.text(0.12, (y0 + y1) / 2, name, rotation=90, va="center", ha="center", fontsize=11, fontweight="bold", color="#444444")

TASK_STYLE = dict(boxstyle="round,pad=0.28,rounding_size=0.15", facecolor="#e3f2ff", edgecolor="#2a5d8f", linewidth=1.4)
EXC_STYLE = dict(boxstyle="round,pad=0.28,rounding_size=0.15", facecolor="#fff0d6", edgecolor="#b3760f", linewidth=1.4)
CANCEL_STYLE = dict(boxstyle="round,pad=0.28,rounding_size=0.4", facecolor="#ffd0d0", edgecolor="#a03030", linewidth=1.4)
EVENT_COLOR = "#2a5d8f"


def task(x, y, label, style=TASK_STYLE, w=1.7, h=0.72, fontsize=8.8):
    box = FancyBboxPatch((x - w / 2, y - h / 2), w, h, **style, zorder=3)
    ax.add_patch(box)
    ax.text(x, y, label, ha="center", va="center", fontsize=fontsize, fontweight="bold", zorder=4)
    return (x, y, w, h)


def event(x, y, label, end=False):
    r = 0.32
    circ = Circle((x, y), r, facecolor="white", edgecolor=EVENT_COLOR, linewidth=2.8 if end else 1.8, zorder=3)
    ax.add_patch(circ)
    ax.text(x, y - 0.58, label, ha="center", va="center", fontsize=8, fontweight="bold", zorder=4)
    return (x, y, r)


def gateway(x, y, label):
    s = 0.42
    pts = [(x, y + s), (x + s, y), (x, y - s), (x - s, y)]
    poly = Polygon(pts, closed=True, facecolor="#fff3c4", edgecolor="#a67c00", linewidth=1.6, zorder=3)
    ax.add_patch(poly)
    ax.text(x, y, "X", ha="center", va="center", fontsize=11, fontweight="bold", zorder=4)
    ax.text(x, y - 0.72, label, ha="center", va="center", fontsize=7.8, fontweight="bold", zorder=4)
    return (x, y, s)


def arrow(p1, p2, color="#333333", lw=1.7, rad=0.0, ls="-"):
    a = FancyArrowPatch(p1, p2, arrowstyle="-|>", mutation_scale=14, color=color, linewidth=lw,
                         connectionstyle=f"arc3,rad={rad}", linestyle=ls, zorder=2)
    ax.add_patch(a)


# ---- Customer App lane ----
sx, sy, sr = event(1.0, 8.55, "Order\nPlaced")
ox, oy, ow, oh = task(2.9, 8.55, "ORDER_\nRECEIVED")
arrow((sx + sr, sy), (ox - ow / 2, oy))

# ---- Dark Store Ops lane ----
pax, pay, paw, pah = task(2.9, 6.9, "PICKER_\nASSIGNED")
gwx, gwy, gws = gateway(5.3, 6.9, "Item in\nstock?")
subx, suby, subw, subh = task(5.3, 5.4, "ITEM_\nSUBSTITUTED", style=EXC_STYLE)
ibx, iby, ibw, ibh = task(7.7, 6.9, "ITEMS_\nBAGGED")

arrow((ox, oy - oh / 2), (pax, pay + pah / 2), rad=-0.15)
arrow((pax + paw / 2, pay), (gwx - gws, gwy))
arrow((gwx, gwy - gws), (subx, suby + subh / 2))
ax.text(gwx + 0.32, gwy - gws - 0.55, "no", fontsize=8, color="#a67c00", fontweight="bold")
arrow((subx + subw / 2, suby + 0.12), (ibx - ibw / 2, iby - 0.25), rad=-0.22)
arrow((gwx + gws, gwy), (ibx - ibw / 2, iby), rad=0.18)
ax.text((gwx + ibx) / 2, gwy + 0.42, "yes", fontsize=8, color="#a67c00", fontweight="bold")

# ---- Rider / Courier lane ----
rax, ray, raw, rah = task(2.9, 3.4, "RIDER_\nASSIGNED")
dfx, dfy, dfw, dfh = task(5.85, 3.4, "DISPATCHED_\nFROM_DARKSTORE", w=2.05)
ddx, ddy, ddw, ddh = task(8.85, 3.4, "DOORSTEP_\nDELIVERED")
ex, ey, er = event(10.75, 3.4, "Order\nComplete", end=True)

arrow((ibx, iby - ibh / 2), (rax, ray + rah / 2), rad=-0.35)
arrow((rax + raw / 2, ray), (dfx - dfw / 2, dfy))
arrow((dfx + dfw / 2, dfy), (ddx - ddw / 2, ddy))
arrow((ddx + ddw / 2, ddy), (ex - er, ey))

# ---- Exception Handling lane ----
cx, cy, cw, ch = task(4.5, 0.95, "ORDER_\nCANCELLED", style=CANCEL_STYLE, h=0.66)
arrow((pax - 0.15, pay - pah / 2), (cx - 0.5, cy + ch / 2 + 0.05), color="#a03030", ls="dashed", rad=-0.25)
arrow((rax + 0.15, ray - rah / 2), (cx - 0.15, cy + ch / 2), color="#a03030", ls="dashed", rad=-0.2)
ax.text(1.05, 2.35, "timeout / no\npicker available", fontsize=7, color="#a03030", ha="center")
ax.text(3.55, 2.05, "rider\nunavailable", fontsize=7, color="#a03030", ha="center")

ax.set_title("BPMN Process Map \u2014 Quick-Commerce Order Fulfillment & Dark-Store Dispatch",
             fontsize=13.5, fontweight="bold", pad=16)

plt.tight_layout()
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "bpmn_diagram.png")
plt.savefig(OUT_PATH, dpi=220, bbox_inches="tight")
print("saved")
