import re
import sys

def patch_index():
    with open('index.html', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Update showPanel to handle 'eso_report'
    old_showpanel = """        } else if (meta.type === 'eso_report_list') {
          const dynPortfolios = meta.portfolios.map(p => window.recalculatePortfolio(p, window.TIME_FILTER));
          panel.innerHTML = buildEsoReportListHTML(dynPortfolios);
        } else if (meta.type === 'so_what') {"""
        
    new_showpanel = """        } else if (meta.type === 'eso_report_list') {
          const dynPortfolios = meta.portfolios.map(p => window.recalculatePortfolio(p, window.TIME_FILTER));
          panel.innerHTML = buildEsoReportListHTML(dynPortfolios);
        } else if (meta.type === 'eso_report') {
          const dynPortfolios = meta.portfolios.map(p => window.recalculatePortfolio(p, window.TIME_FILTER));
          panel.innerHTML = buildEsoReportHTML(meta.eso, dynPortfolios);
        } else if (meta.type === 'so_what') {"""
        
    if old_showpanel in content:
        content = content.replace(old_showpanel, new_showpanel)
    else:
        print("Could not find showPanel hook to patch")
        # Proceed anyway

    # 2. Add digital_credit to aspects array in buildEsoReportSummaryGrid
    old_aspects1 = """        const aspects = [
          { key: 'foundation', label: 'Foundation' },
          { key: 'yiw', label: 'Youth in Work' },
          { key: 'buz_needs', label: 'Business Needs' },
          { key: 'devices', label: 'Devices' },
          { key: 'platforms', label: 'Platforms' },
        ];"""
    
    new_aspects1 = """        const aspects = [
          { key: 'foundation', label: 'Foundation' },
          { key: 'yiw', label: 'Youth in Work' },
          { key: 'buz_needs', label: 'Business Needs' },
          { key: 'devices', label: 'Devices' },
          { key: 'platforms', label: 'Platforms' },
          { key: 'digital_credit', label: 'Digital Credit' },
        ];"""
    
    content = content.replace(old_aspects1, new_aspects1)

    # 3. Add digital_credit to aspects array in buildEsoReportHTML
    old_aspects2 = """        const aspects = [
          { key: 'foundation', label: 'Foundation', builder: buildFoundationHTML },
          { key: 'yiw', label: 'Youth in Work', builder: buildYiwHTML },
          { key: 'buz_needs', label: 'Business Needs', builder: buildBuzNeedsHTML },
          { key: 'devices', label: 'Devices', builder: buildDevicesHTML },
          { key: 'platforms', label: 'Platforms', builder: buildPlatformsHTML },
        ];"""
        
    new_aspects2 = """        const aspects = [
          { key: 'foundation', label: 'Foundation', builder: buildFoundationHTML },
          { key: 'yiw', label: 'Youth in Work', builder: buildYiwHTML },
          { key: 'buz_needs', label: 'Business Needs', builder: buildBuzNeedsHTML },
          { key: 'devices', label: 'Devices', builder: buildDevicesHTML },
          { key: 'platforms', label: 'Platforms', builder: buildPlatformsHTML },
          { key: 'digital_credit', label: 'Digital Credit', builder: buildDigitalCreditHTML },
        ];"""
        
    content = content.replace(old_aspects2, new_aspects2)

    # 4. Radically redesign buildSoWhatHTML to be the 360 trace view
    # we use regex to extract and replace the whole function
    sowhat_func_pattern = re.compile(r"/\*\s+──\s+So What Metrics Builder\s+──\s+\*/\s+function buildSoWhatHTML\(portfolios\) \{.*?\n\s+\}(?=\n\s+/\*)", re.DOTALL)
    
    new_sowhat = """/* ── So What Metrics Builder ── */
      function buildSoWhatHTML(portfolios) {
        const totalAll = portfolios.reduce((s, p) => s + (p.stats?.total || 0), 0);
        const totalUrsb = portfolios.reduce((s, p) => s + (p.stats?.ursb || p.ursb || 0), 0);
        const pwdCount = portfolios.reduce((s, p) => s + (p.stats?.pwd || p.pwd || 0), 0);
        
        // Count unique ESOs across all portfolios
        const esoSet = new Set();
        portfolios.forEach(p => { if (p.eso) esoSet.add(p.eso); });
        const totalEso = esoSet.size || 1;

        // Trace funnels
        const totalFoundation = portfolios.filter(p => p.type === 'foundation').reduce((s, p) => s + (p.stats?.total || 0), 0);
        const totalDevices = portfolios.filter(p => p.type === 'devices').reduce((s, p) => s + (p.stats?.total || 0), 0);
        const totalPlatforms = portfolios.filter(p => p.type === 'platforms').reduce((s, p) => s + (p.stats?.total || 0), 0);

        const creditApps = portfolios.filter(p => p.type === 'digital_credit');
        let totalCredit = 0;
        let totalCreditParticipants = creditApps.reduce((s, p) => s + (p.stats?.total || 0), 0);
        creditApps.forEach(c => {
          totalCredit += (c.eso_credit || []).reduce((s, e) => s + (e.amount_ugx || 0), 0);
        });
        const creditStr = totalCredit >= 1e9 ? (totalCredit / 1e9).toFixed(2) + 'B' : totalCredit >= 1e6 ? (totalCredit / 1e6).toFixed(1) + 'M' : totalCredit.toLocaleString();

        const funnelSteps = [
          { label: 'ESOs Mobilized', val: fmt(totalEso), color: '#3B82F6', bg: '#EFF6FF', border: '#BFDBFE', desc: 'Enterprise Support Orgs' },
          { label: 'Total Base REACH', val: fmt(totalAll), color: '#0F172A', bg: '#F8FAFC', border: '#E2E8F0', desc: 'Baseline MSMEs Engaged' },
          { label: 'Foundation Level', val: fmt(totalFoundation), color: '#8B5CF6', bg: '#F5F3FF', border: '#DDD6FE', desc: 'Capacity Graduations' },
          { label: 'Device Financing', val: fmt(totalDevices), color: '#F59E0B', bg: '#FFFBEB', border: '#FDE68A', desc: 'Smart Devices Placed' },
          { label: 'Platform Onboarding', val: fmt(totalPlatforms), color: '#10B981', bg: '#ECFDF5', border: '#A7F3D0', desc: 'E-commerce/SaaS users' },
          { label: 'Digital Credit', val: 'UGX ' + creditStr, color: '#EC4899', bg: '#FDF2F8', border: '#FBCFE8', desc: 'Growth Capital Accessed' }
        ];

        let funnelCards = '';
        funnelSteps.forEach((step, idx) => {
          const isLast = idx === funnelSteps.length - 1;
          const arrow = isLast ? '' : '<div style="position:absolute; right:-14px; top:50%; transform:translateY(-50%); z-index:10; font-size:1.4rem; color:#94A3B8;">▶</div>';
          funnelCards += `
            <div style="flex:1; min-width:170px; background:${step.bg}; border:2px solid ${step.border}; border-radius:10px; padding:20px 12px; text-align:center; position:relative; display:flex; flex-direction:column; justify-content:center;">
               <div style="font-size:.65rem; font-weight:700; color:#64748B; text-transform:uppercase; margin-bottom:6px;">${step.label}</div>
               <div style="font-size:1.8rem; font-weight:800; color:${step.color}; line-height:1; margin-bottom:4px;">${step.val}</div>
               <div style="font-size:.65rem; color:#64748B;">${step.desc}</div>
               ${arrow}
            </div>
          `;
        });

        return `
      <div class="section-hdr" style="margin-bottom: 24px;">
        <h2 style="font-size: 1.6rem;">So What? — The 360° Participant Journey</h2>
        <span class="type-badge badge-all">Ecosystem Impact</span>
      </div>

      <div class="card" style="margin-bottom:24px; padding: 24px;">
        <h3 style="color:#0F172A; font-size:.9rem; margin-bottom:20px;">VALUE CHAIN FUNNEL: FROM ESO TO CAPITAL</h3>
        <div style="display:flex; justify-content:space-between; align-items:stretch; gap:16px; overflow-x:auto; padding-bottom:10px;">
           ${funnelCards}
        </div>
      </div>

      <div class="kpi-row" style="grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 20px;">
        <div class="card" style="border-left: 5px solid #1D4ED8; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
          <h3 style="color: #64748B; font-size: .75rem; border-bottom: 1px solid #E2E8F0; padding-bottom:10px; margin-bottom:10px;">MARKET FORMALIZATION</h3>
          <div style="font-size: 2.8rem; font-weight: 800; color: #1E3A8A; line-height: 1;">${fmt(totalUrsb)}</div>
          <div style="font-size: .8rem; color: #64748B; line-height: 1.4; margin-top:8px;">Businesses successfully registered with URSB</div>
        </div>
        <div class="card" style="border-left: 5px solid #6D28D9; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
          <h3 style="color: #64748B; font-size: .75rem; border-bottom: 1px solid #E2E8F0; padding-bottom:10px; margin-bottom:10px;">INCLUSIVE EMPOWERMENT</h3>
          <div style="font-size: 2.8rem; font-weight: 800; color: #4C1D95; line-height: 1;">${fmt(pwdCount)}</div>
          <div style="font-size: .8rem; color: #64748B; line-height: 1.4; margin-top:8px;">Persons with Disabilities actively empowered</div>
        </div>
        <div class="card" style="border-left: 5px solid #047857; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
          <h3 style="color: #64748B; font-size: .75rem; border-bottom: 1px solid #E2E8F0; padding-bottom:10px; margin-bottom:10px;">CREDIT PARTICIPANTS</h3>
          <div style="font-size: 2.8rem; font-weight: 800; color: #065F46; line-height: 1;">${fmt(totalCreditParticipants)}</div>
          <div style="font-size: .8rem; color: #64748B; line-height: 1.4; margin-top:8px;">MSMEs actively engaging in Digital Credit pipeline</div>
        </div>
      </div>
    `;
      }
"""
    content = sowhat_func_pattern.sub(new_sowhat, content)

    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(content)

patch_index()
