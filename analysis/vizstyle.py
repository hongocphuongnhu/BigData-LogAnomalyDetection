"""Style matplotlib dung chung cho moi hinh bao cao (EDA + ket qua mo hinh).

Theo skill dataviz (palette tham chieu, ban light vi hinh dung cho Word/slide):
  - mau categorical gan theo THU TU co dinh, khong xoay vong; scatter toi da 3 mau
  - sequential = mot hue (xanh) nhat -> dam; khong dung bang mau cau vong
  - net mong (bar <= ~24px, line 2px), grid hairline lien (khong net dut), truc mo
  - chu dung mau muc (ink), khong dung mau series; >= 2 series luon co legend
Bang mau da chay scripts/validate_palette.js (4 slot dau: PASS; aqua/vang < 3:1
voi nen -> luon kem legend/nhan truc tiep).
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
NEUTRAL = "#c3c2b7"  # "phan con lai" / binh thuong khi lam noi bat mot nhom

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
SEQ_BLUE = LinearSegmentedColormap.from_list("seq_blue", BLUE_RAMP)

# Thu tu co dinh cho cac thuc the lap lai giua nhieu hinh (mau di theo thuc the).
ATTACK_COLORS = dict(zip(["brute_force", "sqli", "scan", "ddos"], SERIES[:4]))


def apply() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"],
            "font.size": 10,
            "text.color": INK,
            "axes.labelcolor": INK_2,
            "axes.titlecolor": INK,
            "axes.titlesize": 12,
            "axes.titleweight": "semibold",
            "axes.titlelocation": "left",
            "axes.edgecolor": AXIS,
            "axes.linewidth": 0.8,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "grid.linestyle": "-",
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "xtick.labelcolor": INK_2,
            "ytick.labelcolor": INK_2,
            "lines.linewidth": 2,
            "lines.solid_capstyle": "round",
            "lines.solid_joinstyle": "round",
            "legend.frameon": False,
            "legend.labelcolor": INK_2,
            "axes.prop_cycle": plt.cycler(color=SERIES),
        }
    )


def subtitle(ax, text: str) -> None:
    """Dong phu de mau muc phu ngay duoi tieu de."""
    ax.text(0, 1.01, text, transform=ax.transAxes, color=INK_2, fontsize=9, va="bottom")


def bar_kwargs(color: str) -> dict:
    """Bar mong + khe 2px mau nen giua cac bar canh nhau (thay cho vien)."""
    return {"color": color, "edgecolor": SURFACE, "linewidth": 2}


def save(fig, out_dir, name: str) -> None:
    fig.tight_layout()
    path = out_dir / name
    fig.savefig(path, dpi=160)
    plt.close(fig)
    print(f"  {path}")
