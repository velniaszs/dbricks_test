"""Generate API reference pages using mkdocs-gen-files."""

import sys
from pathlib import Path

import mkdocs_gen_files

# Find the source directory
src_path = Path("src")
if not src_path.exists():
    print("Warning: src/ directory not found")
    sys.exit()

# Find package directories
package_dirs = [d for d in src_path.iterdir() if d.is_dir() and not d.name.startswith(".")]

if not package_dirs:
    print("Warning: No package directory found in src/")
    sys.exit()

# Use the first package (or you could iterate through all)
package_name = package_dirs[0].name
package_path = src_path / package_name

# Create navigation structure
nav = mkdocs_gen_files.Nav()

# Generate reference pages for each Python module
for py_file in sorted(package_path.rglob("*.py")):
    # Skip private modules and __pycache__
    if py_file.name.startswith("_") and py_file.name != "__init__.py":
        continue

    # Calculate module path relative to package
    module_path = py_file.relative_to(src_path).with_suffix("")
    doc_path = py_file.relative_to(src_path).with_suffix(".md")
    full_doc_path = Path("reference") / doc_path

    # Convert path to module name
    parts = list(module_path.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
        module_name = ".".join(parts)
        if not module_name:
            module_name = package_name
    else:
        module_name = ".".join(parts)

    # Add to navigation
    nav[parts] = doc_path.as_posix()

    # Generate the markdown content
    with mkdocs_gen_files.open(full_doc_path, "w") as f:
        print(f"# {module_name}", file=f)
        print("", file=f)
        print(f"::: {module_name}", file=f)

    # Set edit path for the generated file
    mkdocs_gen_files.set_edit_path(full_doc_path, py_file)

# Generate the main reference index
with mkdocs_gen_files.open("reference/index.md", "w") as f:
    print("# API Reference", file=f)
    print("", file=f)
    print("This section contains the API reference for all modules.", file=f)
    print("", file=f)
    print("## Modules", file=f)
    print("", file=f)

    # Write navigation as a simple list
    for py_file in sorted(package_path.rglob("*.py")):
        if py_file.name.startswith("_") and py_file.name != "__init__.py":
            continue

        module_path = py_file.relative_to(src_path).with_suffix("")
        doc_path = py_file.relative_to(src_path).with_suffix(".md")

        parts = list(module_path.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
            if not parts:
                continue

        module_name = ".".join(parts)
        link_path = doc_path.as_posix()
        print(f"- [{module_name}]({link_path})", file=f)

# Write the navigation
with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())

print(f"✅ Generated API reference for package: {package_name}")
