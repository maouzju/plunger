"""Generate a plunger (马桶塞) icon as plunger.ico using Pillow."""

import math
import os
from PIL import Image, ImageDraw


def draw_plunger(size: int) -> Image.Image:
    """Draw a plunger icon at the given size and return a PIL Image."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    s = size / 64.0

    # Colors
    grip_color = (176, 120, 64)
    grip_outline = (140, 94, 48)
    handle_color = (200, 149, 108)
    handle_outline = (166, 117, 72)
    dome_color = (255, 123, 84)
    dome_outline = (208, 96, 64)
    highlight_color = (255, 170, 130)

    # T-bar grip
    gx1, gy1, gx2, gy2 = int(18*s), int(2*s), int(46*s), int(10*s)
    draw.rectangle([gx1, gy1, gx2, gy2], fill=grip_color, outline=grip_outline, width=max(1, int(s)))

    # Wooden handle
    hx1, hy1, hx2, hy2 = int(27*s), int(9*s), int(37*s), int(36*s)
    draw.rectangle([hx1, hy1, hx2, hy2], fill=handle_color, outline=handle_outline, width=max(1, int(s)))

    # Rubber dome - semicircle (upper arc) + bottom rect
    # Draw the dome as a filled chord/arc
    dome_left = int(8*s)
    dome_right = int(56*s)
    dome_top = int(24*s)
    dome_bottom = int(56*s)

    # Full ellipse bounding box for the arc
    draw.pieslice(
        [dome_left, dome_top, dome_right, dome_bottom],
        start=180, end=360,
        fill=dome_color, outline=dome_outline,
        width=max(1, int(1.5*s))
    )

    # Bottom rim rectangle
    rim_top = int((dome_top + dome_bottom) // 2)
    rim_bottom = int(rim_top + 6*s)
    draw.rectangle(
        [dome_left, rim_top, dome_right, rim_bottom],
        fill=dome_color, outline=dome_outline,
        width=max(1, int(1.5*s))
    )

    # Highlight on dome
    hl_left = int(14*s)
    hl_right = int(30*s)
    hl_top = int(28*s)
    hl_bottom = int(40*s)
    draw.pieslice(
        [hl_left, hl_top, hl_right, hl_bottom],
        start=200, end=320,
        fill=highlight_color
    )

    return img


def create_ico(output_path: str):
    """Create a multi-size .ico file."""
    sizes = [16, 24, 32, 48, 64, 128, 256]
    images = []
    for sz in sizes:
        images.append(draw_plunger(sz))

    # Save as ICO - use the largest as base
    images[0].save(
        output_path,
        format="ICO",
        sizes=[(sz, sz) for sz in sizes],
        append_images=images[1:]
    )


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plunger.ico")
    create_ico(out)
    print(f"Icon saved to {out}")
