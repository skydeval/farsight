#!/usr/bin/env python3
"""Generates crates/farsight-web/static/og-default.png: the one preview
image the public UI offers (1200x630, the same for every page, no data).

Pure standard library: a 5x7 block alphabet drawn as rectangles, written
out as an 8-bit RGB PNG. Run from the repository root."""

import struct
import zlib

W, H = 1200, 630
BG = (21, 21, 23)
FG = (236, 236, 238)
ACCENT = (122, 167, 236)
MUTED = (160, 160, 168)
LINE = (51, 51, 56)

GLYPHS = {
    "A": ["01110", "10001", "10001", "11111", "10001", "10001", "10001"],
    "B": ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
    "C": ["01111", "10000", "10000", "10000", "10000", "10000", "01111"],
    "D": ["11110", "10001", "10001", "10001", "10001", "10001", "11110"],
    "E": ["11111", "10000", "10000", "11110", "10000", "10000", "11111"],
    "F": ["11111", "10000", "10000", "11110", "10000", "10000", "10000"],
    "G": ["01111", "10000", "10000", "10011", "10001", "10001", "01111"],
    "H": ["10001", "10001", "10001", "11111", "10001", "10001", "10001"],
    "I": ["11111", "00100", "00100", "00100", "00100", "00100", "11111"],
    "K": ["10001", "10010", "10100", "11000", "10100", "10010", "10001"],
    "L": ["10000", "10000", "10000", "10000", "10000", "10000", "11111"],
    "N": ["10001", "11001", "10101", "10011", "10001", "10001", "10001"],
    "O": ["01110", "10001", "10001", "10001", "10001", "10001", "01110"],
    "P": ["11110", "10001", "10001", "11110", "10000", "10000", "10000"],
    "R": ["11110", "10001", "10001", "11110", "10100", "10010", "10001"],
    "S": ["01111", "10000", "10000", "01110", "00001", "00001", "11110"],
    "T": ["11111", "00100", "00100", "00100", "00100", "00100", "00100"],
    "U": ["10001", "10001", "10001", "10001", "10001", "10001", "01110"],
    "X": ["10001", "10001", "01010", "00100", "01010", "10001", "10001"],
    " ": ["00000"] * 7,
}

px = [[BG] * W for _ in range(H)]


def rect(x0, y0, x1, y1, color):
    for y in range(max(0, y0), min(H, y1)):
        row = px[y]
        for x in range(max(0, x0), min(W, x1)):
            row[x] = color


def text(s, x, y, cell, color):
    for ch in s:
        for r, bits in enumerate(GLYPHS[ch]):
            for c, bit in enumerate(bits):
                if bit == "1":
                    rect(x + c * cell, y + r * cell, x + (c + 1) * cell, y + (r + 1) * cell, color)
        x += 6 * cell
    return x


def ring(cx, cy, r0, r1, color):
    for y in range(max(0, cy - r1), min(H, cy + r1 + 1)):
        for x in range(max(0, cx - r1), min(W, cx + r1 + 1)):
            d = (x - cx) ** 2 + (y - cy) ** 2
            if r0 * r0 <= d <= r1 * r1:
                px[y][x] = color


# A horizon of rings rising from the lower right corner.
for i, r in enumerate(range(140, 700, 80)):
    ring(1110, 640, r, r + 3, ACCENT if i == 0 else LINE)
rect(90, 150, 102, 480, ACCENT)
text("FARSIGHT", 140, 196, 18, FG)
text("PUBLIC BLOCK RECORDS", 142, 374, 6, MUTED)
text("AN INDEPENDENT INDEX", 142, 430, 6, MUTED)

raw = b"".join(b"\x00" + bytes(v for p in row for v in p) for row in px)


def chunk(kind, data):
    body = kind + data
    return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)


png = (
    b"\x89PNG\r\n\x1a\n"
    + chunk(b"IHDR", struct.pack(">IIBBBBB", W, H, 8, 2, 0, 0, 0))
    + chunk(b"IDAT", zlib.compress(raw, 9))
    + chunk(b"IEND", b"")
)
out = "crates/farsight-web/static/og-default.png"
with open(out, "wb") as f:
    f.write(png)
print(out, len(png), "bytes")
