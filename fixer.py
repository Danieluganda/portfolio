import re

with open('index.html', 'r', encoding='utf-8') as f:
    text = f.read()

# I will find the flawed duplicate block and replace it back to the original `const eso = e.target.dataset.eso;` etc.
bad_block = """            const eso =       /* ── So What Metrics Builder ── */
      function buildSoWhatHTML(portfolios) {"""

if bad_block in text:
    # We strip from the bad block all the way down to `    `;\n      }+ eso + '</h2></div>';`
    start_idx = text.find(bad_block)
    end_marker = "    `;\n      }+ eso + '</h2></div>';"
    end_idx = text.find(end_marker, start_idx) + len(end_marker)
    
    # What was originally here?
    orig = """            const eso = e.target.dataset.eso;
            const pid = `panel-eso-report-${eso.replace(/[^a-zA-Z0-9]/g, '_')}`;
            showTypeGroup('eso_report');
            showPanel(pid);
          }
        });

        /* Show All Portfolios overview by default */
        showTypeGroup('overview');
      }

      /* ═══════════════════════════════════════════════════════════════
         REFRESH HINT
      ═══════════════════════════════════════════════════════════════ */
      window.showRefreshHint = function () {
        alert(
          'To refresh data with new Excel files:\\n\\n' +
          '1. Drop your .xlsx files into the same folder\\n' +
          '2. Run:  python extract_data.py\\n' +
          '3. Reload this page (F5)\\n\\n' +
          'Or double-click run_dashboard.bat for a one-step update.'
        );
      };

      // ═══════════════════════════════════════════════════════════════
      // ESO REPORT BUILDERS (NEW)
      // ═══════════════════════════════════════════════════════════════
      // Summary grid for all ESOs across all aspects
      function buildEsoReportSummaryGrid(esoPortfolios) {
        if (!esoPortfolios.length) return '<div class="no-data-note">No ESO data available</div>';
        // Collect all unique ESOs
        const esoSet = new Set(esoPortfolios.map(p => p.eso));
        const aspects = [
          { key: 'foundation', label: 'Foundation' },
          { key: 'yiw', label: 'Youth in Work' },
          { key: 'buz_needs', label: 'Business Needs' },
          { key: 'devices', label: 'Devices' },
          { key: 'platforms', label: 'Platforms' },
        ];
        // Build a lookup: aspect → eso → portfolio
        const aspectEsoMap = {};
        aspects.forEach(a => {
          aspectEsoMap[a.key] = {};
          esoPortfolios.forEach(p => {
            if (p.type === a.key && p.eso) aspectEsoMap[a.key][p.eso] = p;
          });
        });
        // Table header
        let html = '<div class="card" style="margin-bottom:18px"><h3>ESO Comparison Grid</h3>';
        html += '<div style="overflow-x:auto"><table class="eso-summary-table" style="border-collapse:collapse;width:100%">';
        html += '<thead><tr style="background:#F3F4F6">'
          + '<th style="padding:7px 10px;text-align:left;font-size:.9em">ESO</th>';
        aspects.forEach(a => {
          html += '<th style="padding:7px 10px;text-align:center;font-size:.9em">' + a.label + '</th>';
        });
        html += '</tr></thead><tbody>';
        // Table rows: one per ESO
        Array.from(esoSet).sort().forEach(eso => {
          html += '<tr>';
          html += '<td style="padding:7px 10px;font-weight:600">' + eso + '</td>';
          aspects.forEach(a => {
            const p = aspectEsoMap[a.key][eso];
            if (p) {
              html += '<td style="padding:7px 10px;text-align:center">'
                + '<button class="eso-report-btn" data-eso="' + eso + '" style="font-size:.9em;padding:4px 10px;border-radius:6px;border:none;background:#6366F1;color:#fff;cursor:pointer">View</button>'
                + '<div style="font-size:.8em;color:#64748B;margin-top:2px">' + (p.stats?.total?.toLocaleString() || '-') + '</div>'
                + '</td>';
            } else {
              html += '<td style="padding:7px 10px;text-align:center;color:#CBD5E1">—</td>';
            }
          });
          html += '</tr>';
        });
        html += '</tbody></table></div></div>';
        return html;
      }

      // ESO Report List: summary grid + quick links
      function buildEsoReportListHTML(esoPortfolios) {
        if (!esoPortfolios.length) return '<div class="no-data-note">No ESO data available</div>';
        // Summary grid
        let html = buildEsoReportSummaryGrid(esoPortfolios);
        // Quick links to each ESO
        const esoSet = new Set(esoPortfolios.map(p => p.eso));
        html += '<div class="card" style="margin-top:18px"><h3>Jump to ESO Report</h3>';
        html += '<div style="display:flex;flex-wrap:wrap;gap:10px">';
        Array.from(esoSet).sort().forEach(eso => {
          const pid = `panel-eso-report-${eso.replace(/[^a-zA-Z0-9]/g, '_')}`;
          html += '<button class="eso-report-btn" data-eso="' + eso + '" style="font-size:.95em;padding:7px 18px;border-radius:7px;border:none;background:#3B82F6;color:#fff;cursor:pointer">' + eso + '</button>';
        });
        html += '</div></div>';
        return html;
      }

      // Per-ESO report: show all aspects for a single ESO
      function buildEsoReportHTML(eso, esoPortfolios) {
        // Find all portfolios for this ESO, grouped by aspect
        const aspects = [
          { key: 'foundation', label: 'Foundation', builder: buildFoundationHTML },
          { key: 'yiw', label: 'Youth in Work', builder: buildYiwHTML },
          { key: 'buz_needs', label: 'Business Needs', builder: buildBuzNeedsHTML },
          { key: 'devices', label: 'Devices', builder: buildDevicesHTML },
          { key: 'platforms', label: 'Platforms', builder: buildPlatformsHTML },
        ];
        let html = '<div class="section-hdr"><h2>ESO Report: ' + eso + '</h2></div>';"""

    text = text[:start_idx] + orig + text[end_idx:]

# Also replace the escaped backslashes in the true buildSoWhatHTML block
text = text.replace("\\${fmt(totalAll)}", "${fmt(totalAll)}")
text = text.replace("\\${fmt(totalUrsb)}", "${fmt(totalUrsb)}")
text = text.replace("\\${creditStr}", "${creditStr}")
text = text.replace("\\${fmt(pwdCount)}", "${fmt(pwdCount)}")

# Remove the escaped backtick that breaks everything
# The problematic line expects to just be '    `;' without a backslash
text = text.replace("    \\`;", "    `;")

# Now inject Hot Reload!
run_block = """      /* ─── Run ─── */
  if (document.readyState === 'loading') {"""

hot_reload = """      /* ─── Auto-Updater (Hot Reload) ─── */
      // Pings data.js headers every 3 seconds to check if data is newly prepared
      let lastModifiedStr = null;
      setInterval(() => {
        fetch('data.js', { method: 'HEAD' }).then(res => {
          const currentModified = res.headers.get('Last-Modified') || res.headers.get('Date');
          if (!lastModifiedStr) {
            lastModifiedStr = currentModified;
          } else if (lastModifiedStr !== currentModified) {
            console.log('🔄 Dashboard Data Updated. Auto-reloading!');
            location.reload();
          }
        }).catch(() => {});
      }, 3000);

      /* ─── Run ─── */
  if (document.readyState === 'loading') {"""

if hot_reload not in text:
    text = text.replace(run_block, hot_reload)

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(text)

print("Fix completed cleanly!")
