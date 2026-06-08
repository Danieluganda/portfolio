import re

def patch():
    with open('index.html', 'r', encoding='utf-8') as f:
        content = f.read()

    builders = [
        'buildDigitalCreditHTML',
        'buildFoundationHTML',
        'buildEoiHTML',
        'buildYiwHTML',
        'buildBuzNeedsHTML',
        'buildPlatformsHTML',
        'buildDevicesHTML',
        'buildSegmentationHTML',
        'buildGrowthPlansHTML'
    ]

    for name in builders:
        # Find the function start
        start_idx = content.find(f"function {name}")
        if start_idx == -1:
            print(f"Skipping {name} (not found)")
            continue
        
        # Find the end of the function (approximate by looking for next closing brace at column 6/8)
        # Or better, just find the last return string before the next function or end of script
        next_func = content.find("function ", start_idx + 10)
        search_end = next_func if next_func != -1 else len(content)
        
        # Find the last "`;" in this block
        last_backtick_semicolon = content.rfind("`;", start_idx, search_end)
        if last_backtick_semicolon != -1:
            arg = "creditData" if name == "buildDigitalCreditHTML" else "p"
            patch_str = "` + buildParticipantListHTML(" + arg + ");"
            content = content[:last_backtick_semicolon] + patch_str + content[last_backtick_semicolon+2:]
            print(f"Patched {name}")
        else:
            print(f"Could not find return tail for {name}")

    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    patch()
