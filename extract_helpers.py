#!/usr/bin/env python3
import re, os, sys

script_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/nexus/NEXUS/infra/terraform/aws/scripts/startup.sh"
repl_single = lambda m: m.group(0).replace('$$', '$')
with open(script_path) as f:
    content = f.read()

# Find the write_nexus_helpers() function body
m = re.search(r'write_nexus_helpers\(\)\s*\{', content)
if not m:
    print("ERROR: write_nexus_helpers() not found")
    sys.exit(1)

start = m.end()
# Count braces to find the matching closing brace
depth = 1
pos = start
while pos < len(content) and depth > 0:
    if content[pos] == '{':
        depth += 1
    elif content[pos] == '}':
        depth -= 1
    pos += 1
func_body = content[start:pos-1]

# Extract each heredoc:  cat >/usr/local/bin/SCRIPT <<'DELIM' ... DELIM (on its own line)
targets = {}
pattern = re.compile(r"cat\s+>(/usr/local/bin/\S+)\s*<<'(.*?)'\s*\n(.*?)\n\2", re.DOTALL)
for m in pattern.finditer(func_body):
    path = m.group(1)
    body = m.group(3)
    body = body.replace('$$', '$')
    targets[path] = body

# Also handle the node-services.sh copy
cp_match = re.search(r'cp\s+"\$\$\{NEXUS_APP_DIR\}/infra/docker/node-services.sh"\s+(/usr/local/bin/\S+)', func_body)
if cp_match:
    node_svc_dest = cp_match.group(1)
    node_svc_src = script_path.replace('infra/terraform/aws/scripts/startup.sh', 'infra/docker/node-services.sh')
    print(f"Will copy infra/docker/node-services.sh -> {node_svc_dest}")

for path, body in sorted(targets.items()):
    print(f"Extracting: {path} ({len(body)} bytes)")
    with open(path, 'w') as f:
        f.write(body)
    os.chmod(path, 0o755)

# Copy node-services.sh
if cp_match:
    src = os.path.join(os.path.dirname(script_path), '../../../../infra/docker/node-services.sh')
    src = os.path.normpath(src)
    dest = cp_match.group(1)
    if os.path.exists(src):
        import shutil
        shutil.copy2(src, dest)
        os.chmod(dest, 0o644)
        print(f"Copied: {src} -> {dest}")
    else:
        print(f"WARNING: {src} not found")

print(f"\nDone. {len(targets)} helper scripts installed.")
