#!/usr/bin/env python3

import sys
from pathlib import Path

def main():
    cwd = Path.cwd()
    target_dir = cwd / "target"
    patterns = ["*.rs", "*.toml"]
    file_found_previously = False

    for pattern in patterns:
        for file_path in cwd.rglob(pattern):
            if not file_path.is_file():
                continue

            # Check if the file is within the 'target' directory
            if target_dir in file_path.parents or file_path.parent == target_dir:
                continue

            # Add a separator newline if this isn't the first file
            if file_found_previously:
                print()  
            
            try:
                content = file_path.read_text(encoding="utf-8")
                
                # Print the header (relative path)
                try:
                    relative_name = file_path.relative_to(cwd)
                    print("```")
                    print(f"# {relative_name}")
                except ValueError:
                    print(f"# {file_path.name}") # Fallback to just name
                
                # Print the exact content without an extra trailing newline
                print(content, end="")
                print("```")
                
                file_found_previously = True
            
            except Exception as e:
                # Report errors to standard error
                print(f"# Error reading {file_path.name}: {e}", file=sys.stderr)

    # Ensure the entire output ends with a newline if we printed anything
    if file_found_previously:
        print()

if __name__ == "__main__":
    main()
