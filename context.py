#!/usr/bin/env python3

import sys
import argparse
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print Rust/TOML project files as prompt context."
    )
    parser.add_argument(
        "--strip-tests",
        action="store_true",
        default=False,
        help="Strip test-related code (Rust #[cfg(test)], #[test], and common test sections).",
    )
    parser.add_argument(
        "--strip-bodies",
        action="store_true",
        default=False,
        help="Strip Rust function/module/impl bodies, keeping only signatures and structure.",
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Root directory to scan (default: current directory).",
    )
    return parser.parse_args()


# --- Rust transformations ----------------------------------------------------


def strip_rust_tests(content: str) -> str:
    """
    Heuristically remove test modules and test functions from Rust source.

    - Removes #[cfg(test)] mod tests { ... }
    - Removes functions annotated with #[test]
    """
    lines = content.splitlines()
    out: list[str] = []
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]

        # Handle #[cfg(test)] mod tests { ... }
        if line.strip().startswith("#[cfg(test)]"):
            j = i + 1
            # Look for "mod X {" starting soon after
            while j < n and lines[j].strip() == "":
                j += 1
            if j < n and lines[j].lstrip().startswith("mod ") and "{" in lines[j]:
                # Skip the entire mod block by brace counting
                brace_count = lines[j].count("{") - lines[j].count("}")
                j += 1
                while j < n and brace_count > 0:
                    brace_count += lines[j].count("{")
                    brace_count -= lines[j].count("}")
                    j += 1
                # We consumed the cfg+mod tests block
                i = j
                continue
            # If no mod block found, fall through and just keep the line

        # Handle #[test] functions
        if line.strip().startswith("#[test]"):
            j = i + 1
            # Skip blank/attribute lines to find the fn
            while j < n and lines[j].strip() == "":
                j += 1
            if j < n and "fn " in lines[j]:
                sig_line = lines[j]

                # One-line or inline-body fn: skip until balanced
                if "{" in sig_line:
                    brace_count = sig_line.count("{") - sig_line.count("}")
                    j += 1
                    while j < n and brace_count > 0:
                        brace_count += lines[j].count("{")
                        brace_count -= lines[j].count("}")
                        j += 1
                    i = j
                    continue
                else:
                    # Multi-line signature until '{', then skip body
                    j += 1
                    while j < n and "{" not in lines[j]:
                        j += 1
                    if j < n and "{" in lines[j]:
                        brace_count = lines[j].count("{") - lines[j].count("}")
                        j += 1
                        while j < n and brace_count > 0:
                            brace_count += lines[j].count("{")
                            brace_count -= lines[j].count("}")
                            j += 1
                        i = j
                        continue

            # If pattern not matched, drop #[test] line anyway
            i = j
            continue

        out.append(line)
        i += 1

    return "\n".join(out)


def strip_rust_bodies(content: str) -> str:
    """
    Heuristically strip bodies from Rust fns / impls / mods while preserving structure.

    For:
      - fn ... { ... }
      - impl ... { ... }
      - mod ... { ... }
    emits headers and a single-line body placeholder:
      { /* ... */ }
    """
    lines = content.splitlines()
    out: list[str] = []
    i = 0
    n = len(lines)

    def starts_block_header(s: str) -> bool:
        st = s.strip()
        return (
            st.startswith("fn ")
            or st.startswith("pub fn ")
            or st.startswith("async fn ")
            or st.startswith("pub async fn ")
            or st.startswith("impl ")
            or st.startswith("pub impl ")
            or st.startswith("mod ")
            or st.startswith("pub mod ")
        )

    while i < n:
        line = lines[i]

        # Collect attributes directly above potential headers
        attrs: list[str] = []
        j = i
        while j < n and lines[j].strip().startswith("#["):
            attrs.append(lines[j])
            j += 1

        if attrs:
            if j < n and starts_block_header(lines[j].lstrip()):
                # Defer handling; we'll treat from j as a header
                i = j
                line = lines[i]
            else:
                # Not followed by a known header; just emit attributes
                out.extend(attrs)
                i = j
                continue

        # Check for header we want to collapse
        if starts_block_header(line.lstrip()):
            header_lines = []
            indent = line[: len(line) - len(line.lstrip())]

            # Collect full header (may be multi-line) until we see '{'
            k = i
            open_brace_found = False
            while k < n:
                header_lines.append(lines[k])
                if "{" in lines[k]:
                    open_brace_found = True
                    break
                k += 1

            if not open_brace_found:
                # Incomplete/unusual; emit as-is
                out.extend(header_lines)
                i = k
                continue

            # Initial brace count from the header line that has '{'
            brace_count = header_lines[-1].count("{") - header_lines[-1].count("}")
            k += 1

            # Empty body like '{}' on header line: keep as-is
            if brace_count == 0:
                out.extend(header_lines)
                i = k
                continue

            # Consume until braces balance
            body_end = k
            while body_end < n and brace_count > 0:
                brace_count += lines[body_end].count("{")
                brace_count -= lines[body_end].count("}")
                body_end += 1

            if brace_count != 0:
                # Mismatched braces: keep original block to avoid mangling
                out.extend(header_lines)
                out.extend(lines[k:body_end])
                i = body_end
                continue

            # We have a full block [i, body_end).
            # Replace its body with a concise placeholder.
            last = header_lines[-1]
            brace_idx = last.find("{")
            if brace_idx != -1:
                before = last[:brace_idx].rstrip()
                # Emit all but last header line unchanged
                if len(header_lines) > 1:
                    out.extend(header_lines[:-1])
                # Emit collapsed header + placeholder
                out.append(f"{before} {{ /* ... */ }}")
            else:
                # Fallback; shouldn't hit often
                out.extend(header_lines)
                out.append(f"{indent}{{ /* ... */ }}")

            i = body_end
            continue

        # Default: passthrough
        out.append(line)
        i += 1

    return "\n".join(out)


# --- TOML transformations ----------------------------------------------------


def strip_toml_tests(content: str) -> str:
    """
    Strip common test-only sections from Cargo.toml-like files:
    - [dev-dependencies]
    - [target.'cfg(test)'.dependencies]
    """
    lines = content.splitlines()
    out: list[str] = []
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]
        stripped = line.strip()

        if stripped.startswith("[") and stripped.endswith("]"):
            section_name = stripped[1:-1].strip().strip('"').strip("'")
            if (
                section_name == "dev-dependencies"
                or section_name.startswith("target.'cfg(test)'.")
            ):
                # Skip lines until next section header or EOF
                i += 1
                while i < n:
                    next_line = lines[i].strip()
                    if next_line.startswith("[") and next_line.endswith("]"):
                        break
                    i += 1
                continue

        out.append(line)
        i += 1

    return "\n".join(out)


# --- Dispatcher --------------------------------------------------------------


def process_content(path: Path, content: str, strip_tests: bool, strip_bodies: bool) -> str:
    suffix = path.suffix.lower()

    if suffix == ".rs":
        if "sample_data" in str(path):
            content = ":: excluded given large sample data size ::"
            return content
        if strip_tests:
            content = strip_rust_tests(content)
        if strip_bodies:
            content = strip_rust_bodies(content)
        return content

    if suffix == ".toml":
        if strip_tests:
            content = strip_toml_tests(content)
        return content

    return content


def iter_files(root: Path, patterns: Iterable[str], target_dir_name: str = "target") -> Iterable[Path]:
    target_dir = root / target_dir_name

    for pattern in patterns:
        for file_path in root.rglob(pattern):
            if not file_path.is_file():
                continue
            if target_dir in file_path.parents or file_path.parent == target_dir:
                continue
            yield file_path


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    patterns = ["*.rs", "*.toml"]
    file_found_previously = False

    for file_path in iter_files(root, patterns):
        try:
            content = file_path.read_text(encoding="utf-8")
            content = process_content(
                file_path,
                content,
                strip_tests=args.strip_tests,
                strip_bodies=args.strip_bodies,
            )

            if file_found_previously:
                print()

            # Header
            try:
                relative_name = file_path.relative_to(root)
                print("```")
                print(f"# {relative_name}")
            except ValueError:
                print("```")
                print(f"# {file_path.name}")

            # Content (no extra newline inside block)
            print(content, end="")
            print("```")

            file_found_previously = True

        except Exception as e:
            print(f"# Error reading {file_path.name}: {e}", file=sys.stderr)

    if file_found_previously:
        print()


if __name__ == "__main__":
    main()
