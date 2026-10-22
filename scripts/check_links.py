"""Check the links between the Markdown files, as GitHub renders them.

Every relative link must point at a file that exists, every #anchor at a
heading of its page, and every page in docs/ must be linked from the
README. Links to websites are not fetched. Needs no stack.

    python scripts/check_links.py [FILE.md ...]

Without arguments it checks README.md, CONTRIBUTING.md, CHANGELOG.md and
docs/*.md.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"\]\(([^)\s]+)\)")


def prose(path: Path) -> str:
    """The text of a page outside code blocks and HTML comments."""
    lines, code = [], False
    for line in path.read_text().splitlines():
        if line.lstrip().startswith("```"):
            code = not code
        elif not code:
            lines.append(line)
    return re.sub(r"<!--.*?-->", "", "\n".join(lines), flags=re.S)


def slug(heading: str) -> str:
    """GitHub's anchor for a heading."""
    heading = heading.strip().lower().replace("`", "")
    return re.sub(r"[^\w\- ]", "", heading).replace(" ", "-")


def anchors(path: Path) -> set[str]:
    return {slug(m.group(1)) for m in re.finditer(r"^#{1,6} (.*)$", prose(path), flags=re.M)}


def broken(page: Path) -> list[str]:
    found = []
    for target in LINK.findall(prose(page)):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        path, _, fragment = target.partition("#")
        linked = (page.parent / path).resolve() if path else page
        if not linked.exists():
            found.append(f"{page.relative_to(ROOT)}: missing {target}")
        elif fragment and linked.suffix == ".md" and fragment not in anchors(linked):
            found.append(f"{page.relative_to(ROOT)}: no heading for {target}")
    return found


def unlinked() -> list[str]:
    linked = {(ROOT / target.partition("#")[0]).resolve() for target in LINK.findall(prose(ROOT / "README.md"))}
    return [f"README.md: does not link {page.relative_to(ROOT)}"
            for page in sorted((ROOT / "docs").glob("*.md")) if page.resolve() not in linked]


def main() -> None:
    if sys.argv[1:]:
        pages = [Path(arg).resolve() for arg in sys.argv[1:]]
    else:
        pages = [ROOT / name for name in ("README.md", "CONTRIBUTING.md", "CHANGELOG.md") if (ROOT / name).exists()]
        pages += sorted((ROOT / "docs").glob("*.md"))
    problems = [problem for page in pages for problem in broken(page)] + unlinked()
    for problem in problems:
        print(problem)
    print(f"{len(pages)} pages checked, {len(problems)} problems" if problems
          else f"{len(pages)} pages checked, every link resolves")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
