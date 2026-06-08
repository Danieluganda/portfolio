import subprocess

# Checkout index.html to pristine state
subprocess.check_call(['git', 'checkout', '--', 'index.html'])

# Now apply exactly what I want cleanly
with open('index.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Add 'so_what' to groupedTypes overview
old_line1 = "        const groupedTypes = {\n          'overview': [['overview', 'Executive Summary', 'badge-all']],"
new_line1 = "        const groupedTypes = {\n          'overview': [['so_what', 'Insights Hub', 'badge-all'], ['overview', 'Executive Summary', 'badge-all']],"
text = text.replace(old_line1, new_line1)

# 2. Add handler in init() for so_what
old_line2 = "            } else if (g === 'overview') {\n              panel.innerHTML = buildOverviewHTML(allPortfolios);"
new_line2 = "            } else if (g === 'so_what') {\n              panel.innerHTML = buildSoWhatHTML(allPortfolios);\n            } else if (g === 'overview') {\n              panel.innerHTML = buildOverviewHTML(allPortfolios);"
text = text.replace(old_line2, new_line2)

# 3. Add the builder function
sowhat_func = """
      /* ── So What Metrics Builder ── */
      function buildSoWhatHTML(portfolios) {
        const totalAll = portfolios.reduce((s, p) => s + (p.stats?.total || 0), 0);
        const totalUrsb = portfolios.reduce((s, p) => s + (p.stats?.ursb || p.ursb || 0), 0);
        const pwdCount = portfolios.reduce((s, p) => s + (p.stats?.pwd || p.pwd || 0), 0);

        const creditApps = portfolios.filter(p => p.type === 'digital_credit');
        let totalCredit = 0;
        creditApps.forEach(c => {
          totalCredit += (c.eso_credit || []).reduce((s, e) => s + (e.amount_ugx || 0), 0);
        });
        const creditStr = totalCredit >= 1e9 ? (totalCredit / 1e9).toFixed(2) + 'B' : totalCredit >= 1e6 ? (totalCredit / 1e6).toFixed(1) + 'M' : totalCredit.toLocaleString();

        return `
      <div class="section-hdr" style="margin-bottom: 24px;">
        <h2 style="font-size: 1.6rem;">So What? — Portfolio Impact & Outcomes</h2>
        <span class="type-badge badge-all">Key Metrics Hub</span>
      </div>

      <div class="kpi-row" style="grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 20px;">
        <div class="card" style="border-left: 5px solid #10B981; background: #ECFDF5; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);">
          <h3 style="color: #065F46; font-size: .8rem;">BENEFICIARIES REACHED</h3>
          <div style="font-size: 2.8rem; font-weight: 800; color: #047857; margin: 10px 0; line-height: 1;">${fmt(totalAll)}</div>
          <div style="font-size: .85rem; color: #065F46; line-height: 1.4;">Direct participants & MSMEs touched across all programmes</div>
        </div>
        <div class="card" style="border-left: 5px solid #3B82F6; background: #EFF6FF; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);">
          <h3 style="color: #1D4ED8; font-size: .8rem;">FORMALIZATION</h3>
          <div style="font-size: 2.8rem; font-weight: 800; color: #1E3A8A; margin: 10px 0; line-height: 1;">${fmt(totalUrsb)}</div>
          <div style="font-size: .85rem; color: #1D4ED8; line-height: 1.4;">Businesses successfully registered with URSB</div>
        </div>
        <div class="card" style="border-left: 5px solid #F59E0B; background: #FFFBEB; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);">
          <h3 style="color: #B45309; font-size: .8rem;">CAPITAL MOBILIZED</h3>
          <div style="font-size: 2.4rem; font-weight: 800; color: #78350F; margin: 10px 0 15px; line-height: 1;">UGX ${creditStr}</div>
          <div style="font-size: .85rem; color: #B45309; line-height: 1.4;">Total digital credit disbursed to MSMEs in the ecosystem</div>
        </div>
        <div class="card" style="border-left: 5px solid #8B5CF6; background: #F5F3FF; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);">
          <h3 style="color: #6D28D9; font-size: .8rem;">INCLUSIVE EMPOWERMENT</h3>
          <div style="font-size: 2.8rem; font-weight: 800; color: #4C1D95; margin: 10px 0; line-height: 1;">${fmt(pwdCount)}</div>
          <div style="font-size: .85rem; color: #6D28D9; line-height: 1.4;">Persons with Disabilities actively empowered</div>
        </div>
      </div>

      <div class="three-col" style="margin-top:24px; gap: 20px;">
        <div class="col-section" style="grid-column: span 2;">
          <div class="col-hdr green-hdr" style="font-size: .8rem; padding: 12px 16px;">WHY THIS MATTERS (THE "SO WHAT")</div>
          <div class="col-body" style="font-size: .95rem; color: #374151; line-height: 1.7; padding: 20px;">
            <p style="margin-bottom: 16px"><strong>1. Broad Market Digitization:</strong> By engaging over <strong>${fmt(totalAll)}</strong> MSMEs and individuals, the programme serves as a vital bridge to the formal digital economy, transforming invisible businesses into traceable, bankable entities.</p>
            <p style="margin-bottom: 16px"><strong>2. Driving Economic Formalization:</strong> Moving <strong>${fmt(totalUrsb)}</strong> entities toward URSB registration significantly expands the national formal baseline and reduces the systemic stagnation risks of a purely informal market.</p>
            <p style="margin-bottom: 16px"><strong>3. Unlocking Working Capital:</strong> The ecosystem strategy has proven capable of de-risking capital. By unlocking <strong>UGX ${creditStr}</strong> in credit, the programme directly fulfills working capital gaps that traditional banks typically ignore.</p>
            <p><strong>4. Leave No One Behind:</strong> Proactive integration of rural, female, youth, and PWD demographics guarantees that economic digitization reduces rather than widens the wealth and opportunity divide.</p>
          </div>
        </div>
        <div class="col-section">
          <div class="col-hdr indigo-hdr" style="font-size: .8rem; padding: 12px 16px;">STRATEGIC OUTCOMES</div>
          <div class="col-body" style="padding: 20px;">
            <ul style="font-size: .9rem; color: #475569; padding-left: 20px; line-height: 2;">
               <li style="margin-bottom: 10px"><strong>Data-Driven MSME Policy:</strong> Translating grassroots metrics to national policy action.</li>
               <li style="margin-bottom: 10px"><strong>Sustained Job Creation:</strong> Transitioning subsistence work into structured livelihoods.</li>
               <li style="margin-bottom: 10px"><strong>Resilient Value Chains:</strong> Enhanced tech integration provides stability against shocks.</li>
               <li style="margin-bottom: 10px"><strong>Scaled Financial Literacy:</strong> Preparing users for sophisticated economic participation.</li>
            </ul>
          </div>
        </div>
      </div>
    `;
      }
"""

old_line3 = "      /* ─── Run ─── */"
new_line3 = sowhat_func + "\n" + old_line3
text = text.replace(old_line3, new_line3)

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Success')
